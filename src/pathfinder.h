#include <map>
#include <vector>
#include <queue>
#include <iostream>
#include <fstream>

namespace fasttrips {

    // Supply data: access/egress time and cost between TAZ and stops
    typedef struct TazStopCost {
        float   time_;          // in minutes
        float   access_cost_;   // general cost units
        float   egress_cost_;   // general cost units
        TazStopCost(float t=0.0, float a=0.0, float e=0.0) : time_(t), access_cost_(a), egress_cost_(e) {}
    } TazStopCost;

    // Supply data: Transit vehicle schedules
    typedef struct StopTripTime {
        int     trip_id_;
        int     seq_;           // start at 1
        int     stop_id_;
        float   arrive_time_;   // minutes after midnight
        float   depart_time_;   // minutes after midnight
    } StopTripTime;

    // package this in a struct because it'll get passed around a lot
    struct PathSpecification {
        int     path_id_;
        bool    hyperpath_;
        int     origin_taz_id_;
        int     destination_taz_id_;
        bool    outbound_;
        float   preferred_time_;    // minutes after midnight
        bool    trace_;
    };

    typedef struct StopState {
        float   label_;
        float   deparr_time_;
        int     deparr_mode_;
        int     succpred_;
        float   link_time_;
        float   cost_;
        float   arrdep_time_;
    } StopState;

    typedef struct {
        float   label_;
        int     stop_id_;
    } LabelStop;

    struct LabelStopCompare {
        bool operator()(const LabelStop &cs1, const LabelStop &cs2) const {
            return (cs1.label_ > cs2.label_);
        }
    };

    // stop id -> StopState
    typedef std::map<int, std::vector<StopState> > StopStates;
    // label, stop id priority queue -- lowest label pops
    typedef std::priority_queue<LabelStop, std::vector<LabelStop>, struct LabelStopCompare> LabelStopQueue;

    class PathFinder
    {
    protected:
        // directory in which to write trace files
        std::string output_dir_;

        // for multi-processing
        int process_num_;

        // TAZ information: taz id -> stop id -> costs
        std::map<int, std::map<int, TazStopCost>> taz_access_links_;
        // Trip information: trip id -> vector of [trip id, sequence, stop id, arrival time, departure time]
        std::map<int, std::vector<StopTripTime> > trip_stop_times_;
        // Stop information: stop id -> vector of [trip id, sequence, stop id, arrival time, departure time]
        std::map<int, std::vector<StopTripTime> > stop_trip_times_;

        bool initializeStopStates(const struct PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& cost_stop_queue) const;

        void labelStops(const struct PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& cost_stop_queue) const;

        /**
         * If outbound, then we're searching backwards, so this returns trips that arrive at the given stop in time to depart at timepoint.
         * If inbound,  then we're searching forwards,  so this returns trips that depart at the given stop time after timepoint
         */
        void getTripsWithinTime(int stop_id, bool outbound, float timepoint, std::vector<const StopTripTime>& return_trips,  float time_window=30.0) const;

        void printStopState(std::ostream& ostr, int stop_id, const StopState& ss, const struct PathSpecification& path_spec) const;

        void printTimeDuration(std::ostream& ostr, const float& timedur) const;

        void printTime(std::ostream& ostr, const float& timemin) const;

        void printMode(std::ostream& ostr, const int& mode) const;

    public:
        const static int MODE_ACCESS    = -100;
        const static int MODE_EGRESS    = -101;
        const static int MODE_TRANSFER  = -102;
        const static int MAX_TIME       = 48*60; // 48 hours in minutes
        const static float DISPERSION_PARAMETER;
        // Constructor
        PathFinder();

        void initializeSupply(const char*   output_dir,
                              int           process_num,
                              int*          taz_access_index,
                              float*        taz_access_cost,
                              int           num_links,
                              int*          stoptime_index,
                              float*        stoptime_times,
                              int           num_stoptimes);

        // Destructor
        ~PathFinder();

        // Find the path from the origin TAZ to the destination TAZ
        void findPath(struct PathSpecification path_spec) const;
    };
}