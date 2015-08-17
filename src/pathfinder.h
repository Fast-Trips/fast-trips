#include <map>
#include <vector>
#include <queue>
#include <iostream>
#include <fstream>

namespace fasttrips {

    typedef struct TazStopCost {
        float   time_;          // in minutes
        float   access_cost_;   // general cost units
        float   egress_cost_;   // general cost units
        TazStopCost(float t=0.0, float a=0.0, float e=0.0) : time_(t), access_cost_(a), egress_cost_(e) {}
    } TazStopCost;

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

    typedef std::map<int, std::vector<StopState> > StopStates;
    typedef std::priority_queue<LabelStop, std::vector<LabelStop>, struct LabelStopCompare> LabelStopQueue;

    class PathFinder
    {
    protected:
        // directory in which to write trace files
        std::string output_dir_;

        // for multi-processing
        int process_num_;

        // taz id -> stop id -> costs
        std::map<int, std::map<int, TazStopCost>> taz_access_links_;

        bool initializeStopStates(const struct PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& cost_stop_queue) const;

        void labelStops(const struct PathSpecification& path_spec,
                                  std::ofstream& trace_file,
                                  StopStates& stop_states,
                                  LabelStopQueue& cost_stop_queue) const;

        void printStopState(std::ostream& ostr, int stop_id, const StopState& ss, const struct PathSpecification& path_spec) const;

        void printTimeDuration(std::ostream& ostr, const float& timedur) const;

        void printTime(std::ostream& ostr, const float& timemin) const;

        void printMode(std::ostream& ostr, const int& mode) const;

    public:
        const static int MODE_ACCESS    = -100;
        const static int MODE_EGRESS    = -101;
        const static int MODE_TRANSFER  = -102;
        const static int MAX_TIME       = 48*60; // 48 hours in minutes
        // Constructor
        PathFinder();

        void initializeSupply(const char*   output_dir,
                              int           process_num,
                              int*          taz_access_index,
                              float*        taz_access_cost,
                              int           num_links);

        // Destructor
        ~PathFinder();

        // Find the path from the origin TAZ to the destination TAZ
        void findPath(struct PathSpecification path_spec) const;
    };
}