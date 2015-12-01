/**
 * \file pathfinder.h
 *
 * Defines the C++ the finds a transit path for fast-trips.
 */

#include <map>
#include <vector>
#include <queue>
#include <iostream>
#include <fstream>
#include <string>

#if __APPLE__
#include <tr1/unordered_set>
#elif _WIN32
#include <unordered_set>
// suppress warning C4503: decorated name length exceeded, name was truncated
#pragma warning(disable:4503)
#endif

namespace fasttrips {

    enum DemandModeType {
        MODE_ACCESS   = -100,
        MODE_EGRESS   = -101,
        MODE_TRANSFER = -102,
        MODE_TRANSIT  = -103,
    };

    /// Weight lookup
    typedef struct {
        std::string     user_class_;
        DemandModeType  demand_mode_type_;
        std::string     demand_mode_;
    } UserClassMode;

    /// Comparator to enable the fasttrips::WeightLookup to use UserClassMode as a lookup
    struct UserClassModeCompare {
        // less than
        bool operator()(const UserClassMode &ucm1, const UserClassMode &ucm2) const {
            if (ucm1.user_class_       < ucm2.user_class_      ) { return true;  }
            if (ucm1.user_class_       > ucm2.user_class_      ) { return false; }
            if (ucm1.demand_mode_type_ < ucm2.demand_mode_type_) { return true;  }
            if (ucm1.demand_mode_type_ > ucm2.demand_mode_type_) { return false; }
            if (ucm1.demand_mode_      < ucm2.demand_mode_     ) { return true;  }
            if (ucm1.demand_mode_      > ucm2.demand_mode_     ) { return false; }
            return false;
        }
    };

    // This is a lot of naming but it does make iterator construction easier
    typedef std::map<std::string, double> NamedWeights;
    typedef std::map<int, NamedWeights> SupplyModeToNamedWeights;
    typedef std::map< UserClassMode, SupplyModeToNamedWeights, struct fasttrips::UserClassModeCompare > WeightLookup;

    /// Access/Egress information: taz id -> supply_mode -> stop id -> attribute map
    typedef std::map<std::string, double> Attributes;
    typedef std::map<int, Attributes> StopToAttr;
    typedef std::map<int, StopToAttr> SupplyStopToAttr;
    typedef std::map<int, SupplyStopToAttr> TAZSupplyStopToAttr;

    // Transfer information: stop id -> stop id -> attribute map
    typedef std::map<int, StopToAttr> StopStopToAttr;


    /// Supply data: access/egress time and cost between TAZ and stops
    typedef struct {
        double  time_;          ///< in minutes
        double  access_cost_;   ///< general cost units
        double  egress_cost_;   ///< general cost units
    } TazStopCost;

    /// Supply data: transfer time and cost between stops
    typedef struct {
        double  time_;          ///< in minutes
        double  cost_;          ///< general cost units
    } TransferCost;

    /// Supply data: Transit trip data, indexed by trip ID
    typedef struct {
        int        supply_mode_num_;
        int        route_id_;
        Attributes trip_attr_;
    } TripInfo;

    /// Supply data: Transit vehicle schedules
    typedef struct {
        int     trip_id_;
        int     seq_;           // start at 1
        int     stop_id_;
        double  arrive_time_;   // minutes after midnight
        double  depart_time_;   // minutes after midnight
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

    /**
     * The definition of the path we're trying to find.
     */
    typedef struct {
        int     iteration_;             ///< Iteration
        int     passenger_id_;          ///< The passenger ID
        int     path_id_;               ///< The path ID - uniquely identifies a passenger+path
        bool    hyperpath_;             ///< If true, find path using stochastic algorithm
        int     origin_taz_id_;         ///< Origin of path
        int     destination_taz_id_;    ///< Destination of path
        bool    outbound_;              ///< If true, the preferred time is for arrival, otherwise it's departure
        double  preferred_time_;        ///< Preferred time of arrival or departure, minutes after midnight
        bool    trace_;                 ///< If true, log copious details of the pathfinding into a trace log
        std::string user_class_;        ///< User class string
        std::string access_mode_;       ///< Access demand mode
        std::string transit_mode_;      ///< Transit demand mode
        std::string egress_mode_;       ///< Egress demand mode
    } PathSpecification;

    /**
     * The pathfinding algorithm is a labeling algorithm which associates each stop with a state, encapsulated
     * here.  See StopStates for more.  If the sought path is outbound, then the preferred time is an arrival time
     * at the destination, so the labeling algorithm starts at the destination and works backwards.  If the sought
     * path is inbound, then the preferred time is a departure time from the origin, so the labeling algorithm starts
     * at the origin and works forwards.  Thus, the attributes here have different meanings if the path sought is
     * outbound versus inbound, and the convention is outbound/inbound fo the variable names.
     *
     * The StopState is basically the state at this stop with details of the link after (for outbound) or before
     * (inbound) the stop in the found path.
     *
     * NOTE: for trip states, deparr_time_ and arrdep_time_ are both for the *vehicle* because the passenger
     * times can be inferred from the surrounding states.
     *
     * In particular, for outbound trips, the deparr_time_ for trip states is not necessarily the person departure time.
     * For inbound trips, the arrdep_time_ for trip states is not necessarily the person departure time.
     */
    typedef struct {
        double  label_;                 ///< The label for this stop
        double  deparr_time_;           ///< Departure time for outbound, arrival time for inbound
        int     deparr_mode_;           ///< Departure mode for outbound, arrival mode for inbound.
                                        ///< One of fasttrips::MODE_ACCESS, fasttrips::MODE_EGRESS,
                                        ///< fasttrips::MODE_TRANSFER, or fasttrips::MODE_TRANSIT
        int     trip_id_;               ///< Trip ID if deparr_mode_ is fasttrips::MODE_TRANSIT,
                                        ///< or the supply_mode_num for access, egress
        int     stop_succpred_;         ///< Successor stop for outbound, predecessor stop for inbound
        int     seq_;                   ///< The sequence number of this stop on this trip. (-1 if not trip)
        int     seq_succpred_;          ///< The sequence number of the successor/predecessor stop
        double  link_time_;             ///< Link time.  For trips, includes wait time. Just walk time for others.
        double  cost_;                  ///< Cost
        double  arrdep_time_;           ///< Arrival time for outbound, departure time for inbound
    } StopState;

    /**
     * Struct containing just a label and a stop id, this is stored in the fasttrips::LabelStopQueue
     * (a priority queue) to find the lowest label stops.
     */
    typedef struct {
        double  label_;                 ///< The label during path finding
        int     stop_id_;               ///< Stop ID corresponding to this label
    } LabelStop;

    /// Structure used in PathFinder::hyperpathChoosePath
    typedef struct {
        double  probability_;           ///< Probability of this stop
        int     prob_i_;                ///< Cumulative probability * 1000
        int     stop_id_;               ///< Stop ID
        size_t  index_;                 ///< Index into StopState vector (or taz state vector)
    } ProbabilityStop;

    /// Comparator to enable the fasttrips::LabelStopQueue to return the lowest labeled stop.
    struct LabelStopCompare {
        bool operator()(const LabelStop &cs1, const LabelStop &cs2) const {
            return (cs1.label_ > cs2.label_);
        }
    };

    /**
     * The path finding algorithm stores StopState data in this structure.
     * For the stochastic algorithm, a stop ID maps to a vector of StopState instances.
     * For the deterministic algorithm, the vector only has a single instance of StopState.
     */
    typedef std::map<int, std::vector<StopState> > StopStates;
    /**
     * The pathfinding algorithm uses this to find the lowest label stops.
     */
    typedef std::priority_queue<LabelStop, std::vector<LabelStop>, struct LabelStopCompare> LabelStopQueue;

    /** A single path consists of a map of stop id to StopState, plus the order of the stops are given
     *  a list of stop ids.
     */
    typedef struct {
        std::map<int, StopState>    states_;    ///< Stop states will be in here, indexed by stop ID.
        std::vector<int>            stops_;     ///< stop IDs. They are in origin to destination order
                                                ///< for outbound trips, and destination to origin
                                                ///< order for inbound trips.
    } Path;

    /** In stochastic path finding, this is the information we'll collect about the path. */
    typedef struct {
        int     count_;                         ///< Number of times this path was generated (for stochastic)
        double  cost_;                          ///< Cost of this path
        bool    capacity_problem_;              ///< Does this path have a capacity problem?
        double  probability_;                   ///< Probability of this stop          (for stochastic)
        int     prob_i_;                        ///< Cumulative probability * RAND_MAX (for stochastic)
    } PathInfo;

    /// Comparator to for Path instances so we can put them in a map as keys.
    /// TODO: doc more?
    struct PathCompare {
        // less than
        bool operator()(const Path &path1, const Path &path2) const {
            if (path1.stops_.size() < path2.stops_.size()) { return true; }
            if (path1.stops_.size() > path2.stops_.size()) { return false; }
            // if number of stops matches, check the stop ids and deparr_mode_
            for (int ind=0; ind<path1.stops_.size(); ++ind) {
                if (path1.stops_[ind] < path2.stops_[ind]) { return true; }
                if (path1.stops_[ind] > path2.stops_[ind]) { return false; }
                int stop_id = path1.stops_[ind];
                std::map<int, StopState>::const_iterator ssi1 = path1.states_.find(stop_id);
                std::map<int, StopState>::const_iterator ssi2 = path2.states_.find(stop_id);
                if (ssi1->second.deparr_mode_ < ssi2->second.deparr_mode_) { return true; }
                if (ssi1->second.deparr_mode_ > ssi2->second.deparr_mode_) { return false; }
                if (ssi1->second.trip_id_     < ssi2->second.trip_id_    ) { return true; }
                if (ssi1->second.trip_id_     > ssi2->second.trip_id_    ) { return false; }
            }
            return false;
        }
    };

    /** A set of paths consists of paths mapping to information about them (for choosing one)
     */
    typedef std::map<Path, PathInfo, struct fasttrips::PathCompare> PathSet;

    /**
    * This is the class that does all the work.  Setup the network supply first.
    */
    class PathFinder
    {
    protected:
        /** @name Path finding parameters */
        ///@{
        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.TIME_WINDOW">fasttrips.Assignment.TIME_WINDOW</a>
        double TIME_WINDOW_;

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.BUMP_BUFFER">fasttrips.Assignment.BUMP_BUFFER</a>
        double BUMP_BUFFER_;

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.STOCH_PATHSET_SIZE">fasttrips.Assignment.STOCH_PATHSET_SIZE</a>
        int STOCH_PATHSET_SIZE_; // er....

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.STOCH_DISPERSION">fasttrips.Assignment.STOCH_DISPERSION</a>
        double STOCH_DISPERSION_;
        ///@}

        /// directory in which to write trace files
        std::string output_dir_;

        /// for multi-processing
        int process_num_;

        /// (User class, demand_mode_type, demand_mode) -> supply_mode -> weight_map
        WeightLookup weight_lookup_;

        // ================ Network supply ================
        /// Access/Egress information: taz id -> supply_mode -> stop id -> attribute map
        TAZSupplyStopToAttr taz_access_links_;

        /// Transfer information: stop id -> stop id -> attributes
        StopStopToAttr transfer_links_o_d_;
        StopStopToAttr transfer_links_d_o_;
        /// Trip information: trip id -> Trip Info
        std::map<int, TripInfo> trip_info_;
        /// Trip information: trip id -> vector of [trip id, sequence, stop id, arrival time, departure time]
        std::map<int, std::vector<TripStopTime> > trip_stop_times_;
        /// Stop information: stop id -> vector of [trip id, sequence, stop id, arrival time, departure time]
        std::map<int, std::vector<TripStopTime> > stop_trip_times_;

        // ================ ID numbers to ID strings ===============
        std::map<int, std::string> trip_num_to_str_;
        std::map<int, std::string> stop_num_to_str_;
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
        void readModeIds();
        void readAccessLinks();
        void readTransferLinks();
        void readTripInfo();
        void readWeights();

        /**
         * Tally the link cost, which is the sum of the weighted attributes.
         * @return the cost.
         */
        double PathFinder::tallyLinkCost(const int supply_mode_num,
                                         const PathSpecification& path_spec,
                                         std::ofstream& trace_file,
                                         const NamedWeights& weights,
                                         const Attributes& attributes) const;

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
                                  const LabelStop& current_label_stop,
                                  double latest_dep_earliest_arr) const;

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
                                  const LabelStop& current_label_stop,
                                  double latest_dep_earliest_arr,
                                  std::tr1::unordered_set<int>& trips_done) const;

        /**
         * Label stops by:
         * * while the label_stop_queue has stops
         *     * pulling the lowest-labeled stop
         *     * adding the stops accessible by transfer (PathFinder::updateStopStatesForTransfers)
         *     * adding the stops accessible by transit trip (PathFinder::updateStopStatesForTrips)
         */
        void labelStops(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& label_stop_queue) const;

        /**
         * This is like the reverse of PathFinder::initializeStopStates.
         * Once all the stops are labeled, try to get from the labeled stop to the end TAZ
         * (origin for outbound, destination for inbound).
         *
         * @return sucess.
         */
        bool finalizeTazState(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states) const;

        /**
         * Given all the labeled stops and taz, traces back and generates a
         * specific path.  We do this by setting up probabilities for each
         * option and then choosing via PathFinder::chooseState.
         *
         * @return success
         */
        bool hyperpathGeneratePath(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  const StopStates& stop_states,
                                  const std::vector<StopState>& taz_state,
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
        /**
         * Given a vector of fasttrips::ProbabilityStop instances,
         * randomly selects one based on the cumulative probability
         * (fasttrips::ProbabilityStop.prob_i_)
         *
         * @return the index_ from chosen ProbabilityStop.
         */
        size_t chooseState(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  const std::vector<ProbabilityStop>& prob_stops) const;

        /** Calculates the cost for the entire given path, and checks for capacity issues.
         *  Sets the results into the given fasttrips::PathInfo instance.
         */
        void calculatePathCost(const PathSpecification& path_spec,
                               std::ofstream& trace_file,
                               Path& path,
                               PathInfo& path_info) const;

        bool getFoundPath(const PathSpecification&      path_spec,
                          std::ofstream&                trace_file,
                          const StopStates&             stop_states,
                          Path&                         path,
                          PathInfo&                     path_info) const;

        double getScheduledDeparture(int trip_id, int stop_id, int sequence) const;
        /**
         * If outbound, then we're searching backwards, so this returns trips that arrive at the given stop in time to depart at timepoint.
         * If inbound,  then we're searching forwards,  so this returns trips that depart at the given stop time after timepoint
         */
        void getTripsWithinTime(int stop_id, bool outbound, double timepoint, std::vector<TripStopTime>& return_trips) const;

        double calculateNonwalkLabel(const std::vector<StopState>& current_stop_state) const;

        void printPath(std::ostream& ostr, const PathSpecification& path_spec, const Path& path) const;
        void printPathCompat(std::ostream& ostr, const PathSpecification& path_spec, const Path& path) const;

        void printStopStateHeader(std::ostream& ostr, const PathSpecification& path_spec) const;
        void printStopState(std::ostream& ostr, int stop_id, const StopState& ss, const PathSpecification& path_spec) const;

        void printTimeDuration(std::ostream& ostr, const double& timedur) const;

        void printTime(std::ostream& ostr, const double& timemin) const;

        void printMode(std::ostream& ostr, const int& mode, const int& trip_id) const;

        bool isTrip(const int& mode) const;

    public:
        const static int MAX_DATETIME   = 48*60; // 48 hours in minutes
        const static double MAX_COST;
        const static double MAX_TIME;

        /// PathFinder constructor.
        PathFinder();

        /**
         * Setup the path finding parameters.
         */
        void initializeParameters(double     time_window,
                                  double     bump_buffer,
                                  int        stoch_pathset_size,
                                  double     stoch_dispersion);

        /**
         * Setup the network supply.  This should happen once, before any pathfinding.
         *
         * @param output_dir        The directory in which to output trace files (if any)
         * @param process_num       The process number for this instance
         * @param stoptime_index    For populating PathFinder::trip_stop_times_, this array contains
         *                          trip IDs, sequence numbers and stop IDs
         * @param stoptime_times    For populating PathFinder::trip_stop_times_, this array contains
         *                          transit vehicle arrival times and departure times at a stop.
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

        /// Destructor
        ~PathFinder();

        /**
         * Find the path!  This method is the *WHOLE POINT* of our existence!
         *
         * See PathFinder::initializeStopStates, PathFinder::labelStops,
         * PathFinder::finalTazState, and PathFinder::getFoundPath
         *
         * @param path_spec     The specifications of that path to find
         * @param path          This is really a return fasttrips::Path
         * @param path_info     Also for returng information (e.g. about the Path cost)
         */
        void findPath(PathSpecification path_spec,
                      Path              &path,
                      PathInfo          &path_info) const;
    };
}
