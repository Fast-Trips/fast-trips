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

    //! Supply data: access/egress time and cost between TAZ and stops
    typedef struct TazStopCost {
        double  time_;          // in minutes
        double  access_cost_;   // general cost units
        double  egress_cost_;   // general cost units
    } TazStopCost;

    //! Supply data: transfer time and cost between stops
    typedef struct TransferCost {
        double  time_;          // in minutes
        double  cost_;          // general cost units
    } TransferCost;

    //! Supply data: Transit vehicle schedules
    typedef struct StopTripTime {
        int     trip_id_;
        int     seq_;           // start at 1
        int     stop_id_;
        double  arrive_time_;   // minutes after midnight
        double  depart_time_;   // minutes after midnight
    } StopTripTime;

    //! Package this in a struct because it'll get passed around a lot
    typedef struct PathSpecification {
        int     path_id_;
        bool    hyperpath_;
        int     origin_taz_id_;
        int     destination_taz_id_;
        bool    outbound_;
        double  preferred_time_;    // minutes after midnight
        bool    trace_;
    } PathSpecification;

    typedef struct StopState {
        double  label_;
        double  deparr_time_;
        int     deparr_mode_;
        int     succpred_;
        double  link_time_;
        double  cost_;
        double  arrdep_time_;
    } StopState;

    typedef struct {
        double  label_;
        int     stop_id_;
    } LabelStop;

    // Used in PathFinder::hyperpathChoosePath
    typedef struct ProbabilityStop {
        double  probability_;       // probability
        int     prob_i_;            // cumulative probability * 1000
        int     stop_id_;           // stop id
        size_t  index_;             // index into stop states (or taz states)
    } ProbabilityStop;

    struct LabelStopCompare {
        bool operator()(const LabelStop &cs1, const LabelStop &cs2) const {
            return (cs1.label_ > cs2.label_);
        }
    };

    // stop id -> StopState
    typedef std::map<int, std::vector<StopState> > StopStates;
    // label, stop id priority queue -- lowest label pops
    typedef std::priority_queue<LabelStop, std::vector<LabelStop>, struct LabelStopCompare> LabelStopQueue;

    /*!
    * This is the class that does all the work.  Setup the network supply first.
    */
    class PathFinder
    {
    protected:
        // directory in which to write trace files
        std::string output_dir_;

        // for multi-processing
        int process_num_;

        // ================ Network supply ================
        // TAZ information: taz id -> stop id -> costs
        std::map<int, std::map<int, TazStopCost> > taz_access_links_;
        // Transfer information: stop id -> stop id -> costs
        std::map<int, std::map<int, TransferCost> > transfer_links_o_d_;
        std::map<int, std::map<int, TransferCost> > transfer_links_d_o_;
        // Trip information: trip id -> vector of [trip id, sequence, stop id, arrival time, departure time]
        std::map<int, std::vector<StopTripTime> > trip_stop_times_;
        // Stop information: stop id -> vector of [trip id, sequence, stop id, arrival time, departure time]
        std::map<int, std::vector<StopTripTime> > stop_trip_times_;


        bool initializeStopStates(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& cost_stop_queue) const;

        void updateStopStatesForTransfers(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& label_stop_queue,
                                  const LabelStop& current_label_stop,
                                  double latest_dep_earliest_arr) const;

        void updateStopStatesForTrips(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& label_stop_queue,
                                  const LabelStop& current_label_stop,
                                  double latest_dep_earliest_arr,
                                  std::tr1::unordered_set<int>& trips_done) const;

        void labelStops(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& label_stop_queue) const;

        bool finalizeTazState(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  const StopStates& stop_states,
                                  std::vector<StopState>& taz_state) const;

        bool hyperpathChoosePath(const PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  const StopStates& stop_states,
                                  const std::vector<StopState>& taz_state,
                                  std::map<int, StopState>& path_states,
                                  std::vector<int>& path_stops) const;

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

        // Constructor
        PathFinder();

        /*!
         * Setup the network.
         */
        void initializeSupply(const char*   output_dir,
                              int           process_num,
                              int*          taz_access_index,
                              double*       taz_access_cost,
                              int           num_links,
                              int*          stoptime_index,
                              double*       stoptime_times,
                              int           num_stoptimes,
                              int*          xfer_index,
                              double*       xfer_data,
                              int           num_xfers);

        // Destructor
        ~PathFinder();

        // Find the path from the origin TAZ to the destination TAZ
        void findPath(PathSpecification         path_spec,
                      std::map<int, StopState>& path_states,
                      std::vector<int>&         path_stops) const;
    };
}
