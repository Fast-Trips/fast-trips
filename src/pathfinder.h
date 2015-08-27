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

#if __APPLE__
#include <tr1/unordered_set>
#elif _WIN32
#include <unordered_set>
#endif

namespace fasttrips {

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

    /// Supply data: Transit vehicle schedules
    typedef struct {
        int     trip_id_;
        int     seq_;           // start at 1
        int     stop_id_;
        double  arrive_time_;   // minutes after midnight
        double  depart_time_;   // minutes after midnight
    } StopTripTime;

    /**
     * The definition of the path we're trying to find.
     */
    typedef struct {
        int     path_id_;               ///< The path ID - uniquely identifies a passenger+path
        bool    hyperpath_;             ///< If true, find path using stochastic algorithm
        int     origin_taz_id_;         ///< Origin of path
        int     destination_taz_id_;    ///< Destination of path
        bool    outbound_;              ///< If true, the preferred time is for arrival, otherwise it's departure
        double  preferred_time_;        ///< Preferred time of arrival or departure, minutes after midnight
        bool    trace_;                 ///< If true, log copious details of the pathfinding into a trace log
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
     */
    typedef struct {
        double  label_;                 ///< The label for this stop
        double  deparr_time_;           ///< Departure time for outbound, arrival time for inbound
        int     deparr_mode_;           ///< Departure mode for outbound, arrival mode for inbound
        int     succpred_;              ///< Successor stop for outbound, predecessor stop for inbound
        double  link_time_;             ///< Link time
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

    /**
    * This is the class that does all the work.  Setup the network supply first.
    */
    class PathFinder
    {
    protected:
        /// directory in which to write trace files
        std::string output_dir_;

        /// for multi-processing
        int process_num_;

        // ================ Network supply ================
        /// TAZ information: taz id -> stop id -> costs
        std::map<int, std::map<int, TazStopCost> > taz_access_links_;
        /// Transfer information: stop id -> stop id -> costs
        std::map<int, std::map<int, TransferCost> > transfer_links_o_d_;
        std::map<int, std::map<int, TransferCost> > transfer_links_d_o_;
        /// Trip information: trip id -> vector of [trip id, sequence, stop id, arrival time, departure time]
        std::map<int, std::vector<StopTripTime> > trip_stop_times_;
        /// Stop information: stop id -> vector of [trip id, sequence, stop id, arrival time, departure time]
        std::map<int, std::vector<StopTripTime> > stop_trip_times_;

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
                                  const StopStates& stop_states,
                                  std::vector<StopState>& taz_state) const;

        /**
         * Given all the labeled stops and taz, traces back and chooses a
         * specific path.  We do this by setting up probabilities for each
         * option and then choosing via PathFinder::chooseState.
         *
         * @return success
         */
        bool hyperpathChoosePath(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  const StopStates& stop_states,
                                  const std::vector<StopState>& taz_state,
                                  std::map<int, StopState>& path_states,
                                  std::vector<int>& path_stops) const;
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

        bool getFoundPath(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  const StopStates& stop_states,
                                  const std::vector<StopState>& taz_state,
                                  std::map<int, StopState>& path_states,
                                  std::vector<int>& path_stops) const;

        double getScheduledDeparture(int trip_id, int stop_id, int sequence=-1) const;
        /**
         * If outbound, then we're searching backwards, so this returns trips that arrive at the given stop in time to depart at timepoint.
         * If inbound,  then we're searching forwards,  so this returns trips that depart at the given stop time after timepoint
         */
        void getTripsWithinTime(int stop_id, bool outbound, double timepoint, std::vector<StopTripTime>& return_trips,  double time_window=30.0) const;

        double calculateNonwalkLabel(const std::vector<StopState>& current_stop_state) const;

        void printStopStateHeader(std::ostream& ostr, const PathSpecification& path_spec) const;
        void printStopState(std::ostream& ostr, int stop_id, const StopState& ss, const PathSpecification& path_spec) const;

        void printTimeDuration(std::ostream& ostr, const double& timedur) const;

        void printTime(std::ostream& ostr, const double& timemin) const;

        void printMode(std::ostream& ostr, const int& mode) const;

    public:
        const static int MODE_ACCESS    = -100;
        const static int MODE_EGRESS    = -101;
        const static int MODE_TRANSFER  = -102;
        const static int MAX_DATETIME   = 48*60; // 48 hours in minutes
        const static int MAX_HYPERPATH_ASSIGN_ATTEMPTS = 1001; // er....
        const static double DISPERSION_PARAMETER;
        const static double MAX_COST;
        const static double MAX_TIME;

        /// PathFinder constructor.
        PathFinder();

        /**
         * Setup the network supply.  This should happen once, before any pathfinding.
         *
         * @param output_dir        The directory in which to output trace files (if any)
         * @param process_num       The process number for this instance
         * @param taz_access_index  For populating PathFinder::taz_access_links_, this array contains
         *                          TAZ IDs and stop IDs
         * @param taz_access_cost   For populating PathFinder::taz_access_links_, this array contains
         *                          times, access costs, and egress costs.
         * @param num_tazlinks      The number of TAZ-stop links described by the previous two arrays
         * @param stoptime_index    For populating PathFinder::trip_stop_times_, this array contains
         *                          trip IDs, sequence numbers and stop IDs
         * @param stoptime_times    For populating PathFinder::trip_stop_times_, this array contains
         *                          transit vehicle arrival times and departure times at a stop.
         * @param num_stoptimes     The number of stop times described in the previous two arrays.
         * @param xfer_index        For populating PathFinder::transfer_links_o_d_ and
         *                          PathFinder::transfer_links_d_o_, this array contains the origin
         *                          stop ID and the destination stop ID for transfers.
         * @param xfer_data         For populating PathFinder::transfer_links_o_d_ and
         *                          PathFinder::transfer_links_d_o_, this array contains time
         *                          and cost for transfers.
         * @param num_xfers         The number of transfers described in the previous two arrays.
         */
        void initializeSupply(const char*   output_dir,
                              int           process_num,
                              int*          taz_access_index,
                              double*       taz_access_cost,
                              int           num_tazlinks,
                              int*          stoptime_index,
                              double*       stoptime_times,
                              int           num_stoptimes,
                              int*          xfer_index,
                              double*       xfer_data,
                              int           num_xfers);

        /// Destructor
        ~PathFinder();

        /**
         * Find the path!  This method is the *WHOLE POINT* of our existence!
         *
         * See PathFinder::initializeStopStates, PathFinder::labelStops,
         * PathFinder::finalTazState, and PathFinder::getFoundPath
         *
         * @param path_spec     The specifications of that path to find
         * @param path_states   This is really a return structure.  If a path is found,
         *                      the stop states will be in here, indexed by stop ID.
         * @param path_stops    This is also a return structure.  If a path is found,
         *                      the stop IDs will be in here.  They are in origin to
         *                      destination order for outbound trips, and destination to
         *                      origin order for inbound trips.
         */
        void findPath(PathSpecification         path_spec,
                      std::map<int, StopState>& path_states,
                      std::vector<int>&         path_stops) const;
    };
}
