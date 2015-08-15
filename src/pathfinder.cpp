#include "pathfinder.h"

#include <sstream>
#include <iomanip>
#include <math.h>

namespace fasttrips {

    PathFinder::PathFinder() : process_num_(-1)
    {
        std::cout << "PathFinder constructor" << std::endl;
    }

    void PathFinder::initializeSupply(
        int    process_num,
        int*   taz_access_index,
        float* taz_access_cost,
        int    num_links)
    {
        process_num_ = process_num;

        // std::ostringstream ss;
        // ss << "fasttrips_ext_" << std::setw (3) << std::setfill ('0') << process_num_ << ".log";
        // logfile_.open(ss.str().c_str());
        // logfile_ << "PathFinder InitializeSupply" << std::endl;

        for(int i=0; i<num_links; ++i) {
            taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]] = TazStopCost(taz_access_cost[3*i], taz_access_cost[3*i+1], taz_access_cost[3*i+2]);
            if ((process_num_<= 1) && ((i<5) || (i>num_links-5))) {
                printf("access_links[%d][%d]=%f, %f, %f\n", taz_access_index[2*i], taz_access_index[2*i+1],
                       taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]].time_,
                       taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]].access_cost_,
                       taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]].egress_cost_);
            }
        }
    }

    PathFinder::~PathFinder()
    {
        std::cout << "PathFinder destructor" << std::endl;
    }

    // Find the path from the origin TAZ to the destination TAZ
    void PathFinder::findPath(struct PathSpecification path_spec) const
    {
        // for now we'll just trace
        if (!path_spec.trace_) { return; }

        std::ofstream trace_file;
        if (path_spec.trace_) {
            std::ostringstream ss;
            ss << "fasttrips_trace_" << path_spec.path_id_ << ".log";
            trace_file.open(ss.str().c_str());
            trace_file << "Tracing assignment of passenger " << path_spec.path_id_ << std::endl;
        }

        StopStates      stop_states;
        CostStopQueue   cost_stop_queue;
        initializeStopStates(path_spec, trace_file, stop_states, cost_stop_queue);

        trace_file << "Pulling stop " << cost_stop_queue.top().stop_id_ << " with cost ";
        printTimeDuration(trace_file, cost_stop_queue.top().cost_);
        trace_file << std::endl;
        trace_file.close();
    }

    bool PathFinder::initializeStopStates(const struct PathSpecification& path_spec,
                                          std::ofstream& trace_file,
                                          StopStates& stop_states,
                                          CostStopQueue& cost_stop_queue) const
    {
        trace_file << "Initialize stop states" << std::endl;
        int     start_taz_id;
        float   dir_factor;
        if (path_spec.outbound_) {
            start_taz_id = path_spec.destination_taz_id_;
            dir_factor   = 1.0;
        } else {
            start_taz_id = path_spec.origin_taz_id_;
            dir_factor   = -1.0;
        }

        // are there any egress/access links?
        std::map<int, std::map<int, TazStopCost>>::const_iterator start_links = taz_access_links_.find(start_taz_id);
        if (start_links == taz_access_links_.end()) {
            return false;
        }
        std::map<int, TazStopCost>::const_iterator link_iter;
        for (link_iter  = start_links->second.begin();
             link_iter != start_links->second.end(); ++link_iter)
        {
            int stop_id = link_iter->first;

            // outbound: departure time = destination - access
            // inbound:  arrival time   = origin      + access
            float deparr_time = path_spec.preferred_time_ - (link_iter->second.time_*dir_factor);

            float cost;
            if (path_spec.hyperpath_) {
                // todo: why the 1+
                cost = 1.0 + (path_spec.outbound_ ? link_iter->second.egress_cost_ : link_iter->second.access_cost_);
            } else {
                cost = link_iter->second.time_;
            }

            StopState ss = {
                cost,
                deparr_time,
                path_spec.outbound_ ? PathFinder::MODE_EGRESS : PathFinder::MODE_ACCESS,
                start_taz_id,
                link_iter->second.time_,
                cost,
                PathFinder::MAX_TIME };
            stop_states[stop_id].push_back(ss);
            CostStop cs = { cost, stop_id };
            cost_stop_queue.push( cs );

            if (path_spec.trace_) {
                trace_file << (path_spec.outbound_ ? " +egress" : " +access") << "   ";
                printStopState(trace_file, stop_id, ss);
                trace_file << std::endl;
            }
        }

        return true;
    }

    void PathFinder::printStopState(std::ostream& ostr, int stop_id, const StopState& ss) const
    {
        ostr << std::setw( 8) << std::setfill(' ') << stop_id   << ": ";
        printTimeDuration(ostr, ss.label_);
        ostr << "  ";
        printTime(ostr, ss.deparr_time_);
        ostr << "  ";
        if (ss.deparr_mode_ == PathFinder::MODE_ACCESS) {
            ostr << std::setw(10) << std::setfill(' ') << "Access";
        } else if (ss.deparr_mode_ == PathFinder::MODE_EGRESS) {
            ostr << std::setw(10) << std::setfill(' ') << "Egress";
        } else if (ss.deparr_mode_ == PathFinder::MODE_TRANSFER) {
            ostr << std::setw(10) << std::setfill(' ') << "Transfer";
        } else {
            ostr << std::setw(10) << std::setfill(' ') << ss.deparr_mode_;
        }
        ostr << "  ";
        ostr << std::setw(10) << std::setfill(' ') << ss.succpred_;
        ostr << "  ";
        printTimeDuration(ostr, ss.link_time_);
        ostr << "  ";
        printTimeDuration(ostr, ss.cost_);
        ostr << "  ";
        printTime(ostr, ss.arrdep_time_);
    }

    /*
     * Assuming that timedur is duration in minutes, prints a formatted version.
     */
    void PathFinder::printTimeDuration(std::ostream& ostr, const float& timedur) const
    {
        double minpart, secpart;
        secpart = modf(timedur, &minpart);
        secpart = secpart*60.0;
        // double intpart, fracpart;
        // fracpart = modf(secpart, &intpart);
        ostr << std::setw( 2) << std::setfill(' ') << static_cast<int>(timedur/60.0) << ":"; // hours
        ostr << std::setw( 2) << std::setfill('0') << static_cast<int>(minpart)      << ":"; // minutes
        int width = 5;
        if (secpart < 10) { ostr << "0"; width = 4; }
        ostr << std::left << std::setw(width) << std::setprecision( 4) << std::setfill(' ') << secpart << std::right; // seconds
    }

    /*
     * Assuming that timemin is a time in minutes after midnight, prints a formatted version.
     */
    void PathFinder::printTime(std::ostream& ostr, const float& timemin) const
    {
        double minpart, secpart;
        int    hour = static_cast<int>(timemin/60.0);

        secpart = modf(timemin, &minpart); // split into minutes and seconds
        minpart = minpart - hour*60.0;
        secpart = secpart*60.0;
        ostr << std::setw( 2) << std::setfill('0') << hour                       << ":"; // hour
        ostr << std::setw( 2) << std::setfill('0') << static_cast<int>(minpart)  << ":"; // minutes
        ostr << std::setw( 2) << std::setfill('0') << static_cast<int>(secpart);
    }

}