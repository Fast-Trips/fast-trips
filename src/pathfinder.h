/**
 * \file pathfinder.h
 *
 * Defines the class that does the transit pathfinding for fast-trips.
 */

#include <ctime>
#include <map>
#include <vector>
#include <queue>
#include <iostream>
#include <fstream>
#include <string>
#include "pathspec.h"
#include "access_egress.h"
#include "LabelStopQueue.h"
#include "hyperlink.h"
#include "path.h"

#if __APPLE__
#include <tr1/unordered_set>
#elif __linux__
#include <tr1/unordered_set>
#else
#include <unordered_set>
#endif

namespace fasttrips {

    /// Weight lookup
    typedef struct {
        std::string     user_class_;
        std::string     purpose_;
        DemandModeType  demand_mode_type_;
        std::string     demand_mode_;
    } UserClassPurposeMode;

    /// Comparator to enable the fasttrips::WeightLookup to use UserClassMode as a lookup
    struct UCPMCompare {
        // less than
        bool operator()(const UserClassPurposeMode &ucpm1, const UserClassPurposeMode &ucpm2) const {
            if (ucpm1.user_class_       < ucpm2.user_class_      ) { return true;  }
            if (ucpm1.user_class_       > ucpm2.user_class_      ) { return false; }
            if (ucpm1.purpose_          < ucpm2.purpose_         ) { return true;  }
            if (ucpm1.purpose_          > ucpm2.purpose_         ) { return false; }
            if (ucpm1.demand_mode_type_ < ucpm2.demand_mode_type_) { return true;  }
            if (ucpm1.demand_mode_type_ > ucpm2.demand_mode_type_) { return false; }
            if (ucpm1.demand_mode_      < ucpm2.demand_mode_     ) { return true;  }
            if (ucpm1.demand_mode_      > ucpm2.demand_mode_     ) { return false; }
            return false;
        }
    };

    enum WeightType {
        WEIGHT_LINEAR      = 0,  // default
        WEIGHT_EXPONENTIAL = 1,
        WEIGHT_LOGARITHMIC = 2,
        WEIGHT_LOGISTIC    = 3
    };

    typedef struct  {
      WeightType type_;          // the type of weight
      double     weight_;        // this is the primary part -- the weight itself
      double     log_base_;      // only for WEIGHT_LOGARITHMIC
      double     logistic_max_;  // oly for WEIGHT_LOGISTIC
      double     logistic_mid_;  // oly for WEIGHT_LOGISTIC
    } Weight;

    // This is a lot of naming but it does make iterator construction easier
    typedef std::map<std::string, Weight> NamedWeights;
    typedef std::map<int, NamedWeights> SupplyModeToNamedWeights;
    typedef std::map< UserClassPurposeMode, SupplyModeToNamedWeights, struct fasttrips::UCPMCompare > WeightLookup;



    // Transfer information: stop id -> stop id -> attribute map
    typedef std::map<int, Attributes> StopToAttr;
    typedef std::map<int, StopToAttr> StopStopToAttr;


    /// Supply data: access/egress time and cost between TAZ and stops
    typedef struct {
        double  time_;          ///< in minutes
        double  access_cost_;   ///< general cost units
        double  egress_cost_;   ///< general cost units
    } TazStopCost;

    /// Supply data: Transit trip data, indexed by trip ID
    typedef struct {
        int        supply_mode_num_;
        int        route_id_;
        Attributes trip_attr_;
    } TripInfo;

    /// Supply data: Transit vehicle schedules
    typedef struct {
        int     trip_id_;         /// trip ID
        int     seq_;             /// stop sequence, starts at 1
        int     stop_id_;         /// stop ID
        double  arrive_time_;     /// minutes after midnight
        double  depart_time_;     /// minutes after midnight
        double  shape_dist_trav_; /// shape distance traveled
        double  overcap_;         /// number of passengers overcap
    } TripStopTime;

    /// For capacity lookups: TripStop definition
    typedef struct {
        int     trip_id_;
        int     seq_;
        int     stop_id_;
    } TripStop;

    /// Comparator for the PathFinder::bump_wait_ std::map
    struct TripStopCompare {
        bool operator()(const TripStop &ts1, const TripStop &ts2) const {
            return ((ts1.trip_id_ < ts2.trip_id_) ||
                    ((ts1.trip_id_ == ts2.trip_id_) && (ts1.seq_ < ts2.seq_)));
        }
    };

    /// For fare lookups FarePeriod index
    typedef struct {
        int         route_id_;          ///< Route id number, if applicable
        int         origin_zone_;       ///< Origin stop zone number, if applicable
        int         destination_zone_;  ///< Destination stop zone number, if applicable
    } RouteStopZone;

    struct RouteStopZoneCompare {
        bool operator()(const RouteStopZone& rsz1, const RouteStopZone& rsz2) const {
            if (rsz1.route_id_         < rsz2.route_id_        ) { return true;  }
            if (rsz1.route_id_         > rsz2.route_id_        ) { return false; }
            if (rsz1.origin_zone_      < rsz2.origin_zone_     ) { return true;  }
            if (rsz1.origin_zone_      > rsz2.origin_zone_     ) { return false; }
            if (rsz1.destination_zone_ < rsz2.destination_zone_) { return true;  }
            if (rsz1.destination_zone_ > rsz2.destination_zone_) { return false; }
            return false;
        }
    };

    /// For fare lookups: FarePeriod definition
    struct FarePeriod {
        std::string fare_id_;           ///< Fare ID
        std::string fare_period_;       ///< Name of the fare period
        double      start_time_;        ///< Start time of the fare period
        double      end_time_;          ///< End time of the fare period
        double      price_;             ///< Currency unspecified but matches value_of_time_
        int         transfers_;         ///< Number of free transfers allowed on this fare.
        double      transfer_duration_; ///< Transfer duration, in seconds. -1 if no requirement.
    };

    /// Maps route id + origin zone + dest zone (any of these may be NA, or -1) => FarePeriod
    typedef std::multimap<RouteStopZone, struct FarePeriod, struct RouteStopZoneCompare> FarePeriodMmap;

    /// Fare transfer types
    enum FareTransferType {
      TRANSFER_FREE     = 1,            ///< free transfer
      TRANSFER_DISCOUNT = 2,            ///< discount transfer
      TRANSFER_COST     = 3             ///< set price transfer
    };

    /// For fare transfer rules
    typedef struct {
      FareTransferType  type_;            ///< what type of transfer is this
      double            amount_;          ///< fare transfer type
    } FareTransfer;

    /// Fare Transfer Rules
    typedef std::map< std::pair<std::string,std::string>, FareTransfer> FareTransferMap;

    /** Performance information to return. */
    typedef struct {
        int     label_iterations_;              ///< Number of label iterations performed
        int     num_labeled_stops_;             ///< Number of stops labeled
        int     max_process_count_;             ///< Maximum number of times a stop was processed
        long    milliseconds_labeling_;         ///< Number of seconds spent in labeling
        long    milliseconds_enumerating_;      ///< Number of seconds spent in enumerating
        long    workingset_bytes_;              ///< Working set size, in bytes
        long    privateusage_bytes_;            ///< Private memory usage, in bytes
        long    mem_timestamp_;                 ///< Time of memory query, in seconds since epoch
    } PerformanceInfo;

    /**
    * This is the class that does all the work.  Setup the network supply first.
    */
    class PathFinder
    {
    protected:
        /** @name Path finding parameters */
        ///@{

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.BUMP_BUFFER">fasttrips.Assignment.BUMP_BUFFER</a>
        double BUMP_BUFFER_;

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.PathSet.DEPART_EARLY_MIN">fasttrips.PathSet.DEPART_EARLY_MIN</a>
        double DEPART_EARLY_ALLOWED_MIN_;

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.PathSet.ARRIVE_LATE_MIN">fasttrips.PathSet.ARRIVE_LATE_MIN</a>
        double ARRIVE_LATE_ALLOWED_MIN_;

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.STOCH_PATHSET_SIZE">fasttrips.Assignment.STOCH_PATHSET_SIZE</a>
        int STOCH_PATHSET_SIZE_; // er....

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.STOCH_MAX_STOP_PROCESS_COUNT">fasttrips.Assignment.STOCH_MAX_STOP_PROCESS_COUNT</a>
        int STOCH_MAX_STOP_PROCESS_COUNT_;

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.MAX_NUM_PATHS">fasttrips.Assignment.MAX_NUM_PATHS</a>
        int MAX_NUM_PATHS_;

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.MIN_PATH_PROBABILITY">fasttrips.Assignment.MIN_PATH_PROBABILITY</a>
        double MIN_PATH_PROBABILITY_;
        ///@}

        /// Access this through getTransferAttributes()
        static Attributes* ZERO_WALK_TRANSFER_ATTRIBUTES_;

        /// directory in which to write trace files
        std::string output_dir_;

        /// for multi-processing
        int process_num_;

        /// (User class, demand_mode_type, demand_mode) -> supply_mode -> weight_map
        WeightLookup weight_lookup_;

        // ================ Network supply ================
        /// Access/Egress information: taz id -> supply_mode -> stop id -> (start time, end time) -> attribute map
        AccessEgressLinks access_egress_links_;

        /// Transfer information: stop id -> stop id -> attributes
        StopStopToAttr transfer_links_o_d_;
        StopStopToAttr transfer_links_d_o_;
        /// Trip information: trip id -> Trip Info
        std::map<int, TripInfo> trip_info_;
        /// Trip information: trip id -> vector of [trip id, sequence, stop id, arrival time, departure time, overcap]
        std::map<int, std::vector<TripStopTime> > trip_stop_times_;
        /// Stop information: stop id -> vector of [trip id, sequence, stop id, arrival time, departure time, overcap]
        std::map<int, std::vector<TripStopTime> > stop_trip_times_;
        // Fare information: route id -> fare id
        std::map<int, int> route_fares_;
        // Fare information: route/origin zone/dest zone -> fare period
        FarePeriodMmap fare_periods_;
        // Fare transfer rules: (from_fare_period,to_fare_period) -> FareTransfer
        FareTransferMap fare_transfer_rules_;

        // ================ ID numbers to ID strings ===============
        std::map<int, std::string> trip_num_to_str_;
        std::map<int, Stop>        stop_num_to_stop_;
        std::map<int, std::string> route_num_to_str_;
        std::map<int, std::string> mode_num_to_str_; // supply modes
        int transfer_supply_mode_;

        /**
         * From simulation: When there are capacity limitations on a vehicle and passengers cannot
         * board a vehicle, this is the time the bumped passengers arrive at a stop and wait for a
         * vehicle they cannot board.
         *
         * This structure maps the fasttrips::TripStop to the arrival time of the first waiting
         * would-be passenger.
         */
        std::map<TripStop, double, struct TripStopCompare> bump_wait_;

        /**
         * Read the intermediate files mapping integer IDs to strings
         * for modes, stops, trips, and routes.
         **/
        void readIntermediateFiles();
        void readTripIds();
        void readStopIds();
        void readRouteIds();
        void readFarePeriods();
        void readModeIds();
        void readAccessLinks();
        void readTransferLinks();
        void readTripInfo();
        void readWeights();

        void addStopState(const PathSpecification& path_spec,
                          std::ofstream& trace_file,
                          const int stop_id,
                          const StopState& ss,
                          const Hyperlink* prev_link,
                          StopStates& stop_states,
                          LabelStopQueue& label_stop_queue) const;

        /**
         * Initialize the stop states from the access (for inbound) or egress (for outbound) links
         * from the start TAZ.
         *
         * @return success.  This method will only fail if there are no access/egress links for the starting TAZ.
         */
        bool initializeStopStates(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& cost_stop_queue) const;

        /**
         * Iterate through all the stops that transfer to(outbound)/from(inbound) the
         * *current_label_stop* and update the *stop_states* with information about how
         * accessible those stops are as a transfer to/from the *current_label_stop*.
         */
        void updateStopStatesForTransfers(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& label_stop_queue,
                                  int label_iteration,
                                  const LabelStop& current_label_stop) const;

        /**
         * Part of the labeling loop. Assuming the *current_label_stop* was just pulled off the
         * *label_stop_queue*, this method will iterate through access links to (for outbound) or
         * egress links from (for inbound) the current stop and update the next stop given the current stop state.
         */
        void updateStopStatesForFinalLinks(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  const std::map<int, int>& reachable_final_stops,
                                  StopStates& stop_states,
                                  LabelStopQueue& label_stop_queue,
                                  int label_iteration,
                                  const LabelStop& current_label_stop,
                                  double& est_max_path_cost) const;


        /**
         * Iterate through all the stops that are accessible by transit vehicle trip
         * to(outbound)/from(inbound) the *current_label_stop* and update the *stop_states*
         * with information about how accessible those stops are as a transit trip to/from
         * the *current_label_stop*.
         */
        void updateStopStatesForTrips(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& label_stop_queue,
                                  int label_iteration,
                                  const LabelStop& current_label_stop,
                                  std::tr1::unordered_set<int>& trips_done) const;

        /**
         * Label stops by:
         * * while the label_stop_queue has stops AND we don't think we're done*
         *     * pulling the lowest-labeled stop
         *     * adding the stops accessible by transfer (PathFinder::updateStopStatesForTransfers)
         *     * adding the stops accessible by transit trip (PathFinder::updateStopStatesForTrips)
         *
         * Assume we're done if we've reached the final TAZ already and the current cost is some percent bigger than
         * threshhold based on the lowest cost and the minimum probability.
         */
        int labelStops(const PathSpecification& path_spec,
                       std::ofstream& trace_file,
                       const std::map<int,int>& reachable_final_stops,
                       StopStates& stop_states,
                       LabelStopQueue& label_stop_queue,
                       int& max_process_count) const;

        /**
         * This fills the reachable_final_stops map with stop_id -> number of supply links between
         * the final stop and the final TAZ.
         *
         * @return True if some final stops are reachable, False if there are none
         */
        bool setReachableFinalStops(const PathSpecification& path_spec,
                                    std::ofstream& trace_file,
                                    std::map<int, int>& reachable_final_stops) const;

        /**
         * This is like the reverse of PathFinder::initializeStopStates.
         * Once all the stops are labeled, try to get from the labeled stop to the end TAZ
         * (origin for outbound, destination for inbound).
         *
         * @return sucess.
         */
        bool finalizeTazState(const PathSpecification& path_spec,
                              std::ofstream& trace_file,
                              StopStates& stop_states,
                              LabelStopQueue& label_stop_queue,
                              int label_iteration) const;

        /**
         * Given all the labeled stops and taz, traces back and generates a
         * specific path.  We do this by setting up probabilities for each
         * option and then choosing via PathFinder::chooseState.
         *
         * @return success
         */
        bool hyperpathGeneratePath(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  Path& path) const;

        /**
         * Given a set of paths, randomly selects one based on the cumulative
         * probability (fasttrips::PathInfo.prob_i_)
         *
         * Returns a reference to that path, which is stored in paths.
         */
        Path choosePath(const PathSpecification& path_spec,
                        std::ofstream& trace_file,
                        PathSet& paths,
                        int max_prob_i) const;

        int getPathSet(const PathSpecification&      path_spec,
                       std::ofstream&                trace_file,
                       StopStates&                   stop_states,
                       PathSet&                      pathset) const;

        /**
         * If outbound, then we're searching backwards, so this returns trips that arrive at the given stop in time to depart at timepoint.
         * If inbound,  then we're searching forwards,  so this returns trips that depart at the given stop time after timepoint
         */
        void getTripsWithinTime(int stop_id, bool outbound, double timepoint, std::vector<TripStopTime>& return_trips) const;

    public:
        const static int MAX_DATETIME   = 48*60; // 48 hours in minutes

        /** Return statuses for PathFinder::findPathSet() **/
        const static int RET_SUCCESS               = 0;    ///< Success. Paths found
        const static int RET_FAIL_INIT_STOP_STATES = 1;    ///< PathFinder::initializeStopStates() failed
        const static int RET_FAIL_SET_REACHABLE    = 2;    ///< PathFinder::setReachableFinalStops() failed
        const static int RET_FAIL_END_NOT_FOUND    = 3;    ///< end taz not reached
        const static int RET_FAIL_NO_PATHS_GEN     = 4;    ///< no paths successfully walked
        const static int RET_FAIL_NO_PATH_PROB     = 5;    ///< no paths with probability found (?)

        /// PathFinder constructor.
        PathFinder();

        int processNumber() const { return process_num_; }
        /// This is the transfer supply mode number
        int transferSupplyMode() const { return transfer_supply_mode_; }
        /// Accessor for access link attributes
        const Attributes* getAccessAttributes(int taz_id, int supply_mode_num, int stop_id, double tp_time) const;
        /// Accessor for transfer link attributes
        const Attributes* getTransferAttributes(int origin_stop_id, int destination_stop_id) const;
        /// Accessor for trip info
        const TripInfo* getTripInfo(int trip_id_num) const;
        /// Accessor for route id
        int getRouteIdForTripId(int trip_id_num) const;
        /// Accessor for TripStopTime for given trip id, stop sequence
        const TripStopTime& getTripStopTime(int trip_id, int stop_seq) const;
        /**
         * Tally the link cost, which is the sum of the weighted attributes.
         * @return the cost.
         */
        double tallyLinkCost(const int supply_mode_num,
                             const PathSpecification& path_spec,
                             std::ostream& trace_file,
                             const NamedWeights& weights,
                             const Attributes& attributes,
                             bool  hush = false) const;

        /**
         * Access the named weights given user/link information.
         * Returns NULL if not found.
         **/
        const NamedWeights* getNamedWeights(const std::string& user_class,
                                            const std::string& purpose,
                                            DemandModeType     demand_mode_type,
                                            const std::string& demand_mode,
                                            int                suppy_mode_num) const;
        /**
         * Setup the path finding parameters.
         */
        void initializeParameters(double     time_window,
                                  double     bump_buffer,
                                  double     utils_conversion,
                                  double     depart_early_allowed_min,
                                  double     arrive_late_allowed_min,
                                  int        stoch_pathset_size,
                                  double     stoch_dispersion,
                                  int        stoch_max_stop_process_count,
                                  bool       transfer_fare_ignore_pf,
                                  bool       transfer_fare_ignore_pe,
                                  int        max_num_paths,
                                  double     min_path_probability);

        /**
         * Setup the network supply.  This should happen once, before any pathfinding.
         *
         * @param output_dir        The directory in which to output trace files (if any)
         * @param process_num       The process number for this instance
         * @param stoptime_index    For populating PathFinder::trip_stop_times_, this array contains
         *                          trip IDs, sequence numbers, stop IDs
         * @param stoptime_times    For populating PathFinder::trip_stop_times_, this array contains
         *                          transit vehicle arrival times, departure times, and overcap pax at a stop.
         * @param num_stoptimes     The number of stop times described in the previous two arrays.
         */
        void initializeSupply(const char*   output_dir,
                              int           process_num,
                              int*          stoptime_index,
                              double*       stoptime_times,
                              int           num_stoptimes);

        /**
         * Setup the information for bumped passengers.
         *
         * @param bw_index          For populating PathFinder::bump_wait_, this array contains the
         *                          fasttrips::TripStop fields.
         * @param bw_data           For populating the PathFinder::bum_wait_, this contains the
         *                          arrival time of the first would-be waiting passenger
         * @param num_bw            The number of trip stops with bump waits described in the
         *                          previous two arrays.
         */
        void setBumpWait(int*       bw_index,
                         double*    bw_data,
                         int        num_bw);

        /// Reset - clear state
        void reset();

        /// Destructor
        ~PathFinder();

        /**
         * Find the path set!  This method is the *WHOLE POINT* of our existence!
         *
         * See PathFinder::initializeStopStates, PathFinder::labelStops,
         * PathFinder::finalTazState, and PathFinder::getFoundPath
         *
         * @param path_spec     The specifications of that path to find
         * @param path          This is really a return fasttrips::Path
         * @param path_info     Also for returng information (e.g. about the Path cost)
         * @returns             Return code signifying return status
         */
        int findPathSet(
            PathSpecification path_spec,
            PathSet           &pathset,
            PerformanceInfo   &performance_info) const;

        double getScheduledDeparture(int trip_id, int stop_id, int sequence) const;

        const FarePeriod* getFarePeriod(int route_id, int board_stop_id, int alight_stop_id, double trip_depart_time) const;

        const FareTransfer* getFareTransfer(const std::string from_fare_period, const std::string to_fare_period) const;

        void printTimeDuration(std::ostream& ostr, const double& timedur) const;

        void printTime(std::ostream& ostr, const double& timemin) const;

        void printMode(std::ostream& ostr, const int& mode, const int& trip_id) const;

        /// Accessor for stop strings.  Assumes valid stop id.
        const std::string& stopStringForId(int stop_id) const { return stop_num_to_stop_.find(stop_id)->second.stop_str_; }
        /// Accessor for trip strings.  Assumes valid trip id.
        const std::string& tripStringForId(int trip_id) const { return trip_num_to_str_.find(trip_id)->second; }
        /// Accessor for mode strings.  Assumes valid mode number.
        const std::string& modeStringForNum(int mode_num) const { return mode_num_to_str_.find(mode_num)->second; }
    };
}
