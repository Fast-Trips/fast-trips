#include "pathfinder.h"

#include <assert.h>
#include <sstream>
#include <iomanip>
#include <math.h>
#include <algorithm>

#if __APPLE__
#include <tr1/unordered_set>
#elif _WIN32
#include <unordered_set>
#endif

const char kPathSeparator =
#ifdef _WIN32
                            '\\';
#else
                            '/';
#endif
 
namespace fasttrips {

    const float PathFinder::DISPERSION_PARAMETER = 1.0;


    PathFinder::PathFinder() : process_num_(-1)
    {
        std::cout << "PathFinder constructor" << std::endl;
    }

    void PathFinder::initializeSupply(
        const char* output_dir,
        int         process_num,
        int*        taz_access_index,
        float*      taz_access_cost,
        int         num_links,
        int*        stoptime_index,
        float*      stoptime_times,
        int         num_stoptimes)
    {
        output_dir_  = output_dir;
        process_num_ = process_num;

        // std::ostringstream ss;
        // ss << "fasttrips_ext_" << std::setw (3) << std::setfill ('0') << process_num_ << ".log";
        // logfile_.open(ss.str().c_str());
        // logfile_ << "PathFinder InitializeSupply" << std::endl;

        for(int i=0; i<num_links; ++i) {
            taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]] = TazStopCost(taz_access_cost[3*i], taz_access_cost[3*i+1], taz_access_cost[3*i+2]);
            if ((process_num_<= 1) && ((i<5) || (i>num_links-5))) {
                printf("access_links[%4d][%4d]=%f, %f, %f\n", taz_access_index[2*i], taz_access_index[2*i+1],
                       taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]].time_,
                       taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]].access_cost_,
                       taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]].egress_cost_);
            }
        }

        for (int i=0; i<num_stoptimes; ++i) {
            StopTripTime stt = {
                stoptime_index[3*i],    // trip id
                stoptime_index[3*i+1],  // sequence
                stoptime_index[3*i+2],  // stop id
                stoptime_times[2*i],    // arrive time
                stoptime_times[2*i+1]   // depart time
            };
            // verify the sequence number makes sense: sequential, starts with 1
            assert(stt.sequence_ == trip_stop_times_[stt.trip_id_].size()+1);

            trip_stop_times_[stt.trip_id_].push_back(stt);
            stop_trip_times_[stt.stop_id_].push_back(stt);
            if ((process_num <= 1) && ((i<5) || (i>num_stoptimes-5))) {
                printf("stoptimes[%4d][%4d][%4d]=%f, %f\n", stoptime_index[3*i], stoptime_index[3*i+1], stoptime_index[3*i+2],
                       stoptime_times[2*i], stoptime_times[2*i+1]);
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
        // if (!path_spec.trace_) { return; }

        std::ofstream trace_file;
        if (path_spec.trace_) {
            std::ostringstream ss;
            ss << output_dir_ << kPathSeparator;
            ss << "fasttrips_trace_" << path_spec.path_id_ << ".log";
            trace_file.open(ss.str().c_str());
            trace_file << "Tracing assignment of passenger " << path_spec.path_id_ << std::endl;
        }

        StopStates      stop_states;
        LabelStopQueue  label_stop_queue;
        // todo: handle failure
        initializeStopStates(path_spec, trace_file, stop_states, label_stop_queue);

        labelStops(path_spec, trace_file, stop_states, label_stop_queue);

        trace_file.close();
    }

    /*
     * Initialize the stop states from the access (for inbound) or egress (for outbound) links
     * from the start TAZ.
     * Returns success.  This method will only fail if there are no access/egress links for the starting TAZ.
     */
    bool PathFinder::initializeStopStates(const struct PathSpecification& path_spec,
                                          std::ofstream& trace_file,
                                          StopStates& stop_states,
                                          LabelStopQueue& label_stop_queue) const
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
        std::map<int, std::map<int, TazStopCost> >::const_iterator start_links = taz_access_links_.find(start_taz_id);
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
            LabelStop ls = { cost, stop_id };
            label_stop_queue.push( ls );

            if (path_spec.trace_) {
                trace_file << (path_spec.outbound_ ? " +egress" : " +access") << "   ";
                printStopState(trace_file, stop_id, ss, path_spec);
                trace_file << std::endl;
            }
        }

        return true;
    }

    void PathFinder::labelStops(const struct PathSpecification& path_spec,
                                          std::ofstream& trace_file,
                                          StopStates& stop_states,
                                          LabelStopQueue& label_stop_queue) const
    {
        int label_iterations = 0;
        std::tr1::unordered_set<int> stop_done;
        std::tr1::unordered_set<int> trips_done;
        float dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

        while (!label_stop_queue.empty()) {
            /***************************************************************************************
            * for outbound: we can depart from *stop_id*
            *                      via *departure mode*
            *                      at *departure time*
            *                      and get to stop *successor*
            *                      and the total cost from *stop_id* to the destination TAZ is *label*
            * for inbound: we can arrive at *stop_id*
            *                     via *arrival mode*
            *                     at *arrival time*
            *                     from stop *predecessor*
            *                     and the total cost from the origin TAZ to the *stop_id* is *label*
            **************************************************************************************/
            LabelStop current_label_stop = label_stop_queue.top();
            label_stop_queue.pop();

            // stop is already processed
            if (stop_done.find(current_label_stop.stop_id_) != stop_done.end()) continue;
            // no transfers to the stop
            // TODO
            // process this stop now - just once
            stop_done.insert(current_label_stop.stop_id_);

            // current_stop_state is a vector
            std::vector<StopState>& current_stop_state = stop_states[current_label_stop.stop_id_];

             if (path_spec.trace_) {
                trace_file << "Pulling from label_stop_queue (iteration " << label_iterations << ", label ";
                if (path_spec.hyperpath_) {
                    trace_file << std::setprecision(4) << current_label_stop.label_;
                }
                else {
                    printTimeDuration(trace_file, current_label_stop.label_);
                }
                trace_file << ", stop " << current_label_stop.stop_id_ << ") :======" << std::endl;
                for (std::vector<StopState>::const_iterator ssi  = current_stop_state.begin();
                                                            ssi != current_stop_state.end(); ++ssi) {
                    trace_file << "            ";
                    printStopState(trace_file, current_label_stop.stop_id_, *ssi, path_spec);
                    trace_file << std::endl;
                }
                trace_file << "==============================" << std::endl;
            }

            int     current_mode            = current_stop_state[0].deparr_mode_;      // why index 0?
            // latest departure for outbound, earliest arrival for inbound
            float   latest_dep_earliest_arr = current_stop_state[0].deparr_time_;
            for (std::vector<StopState>::const_iterator ssi  = current_stop_state.begin();
                                                        ssi != current_stop_state.end(); ++ssi) {
                if (path_spec.outbound_) {
                    latest_dep_earliest_arr = std::max(latest_dep_earliest_arr, ssi->deparr_time_);
                } else {
                    latest_dep_earliest_arr = std::min(latest_dep_earliest_arr, ssi->deparr_time_);
                }
            }

            if (path_spec.trace_) {
                trace_file << "  current mode:     " << std::left;
                printMode(trace_file, current_mode);
                trace_file << std::endl;
                trace_file << (path_spec.outbound_ ? "  latest_departure: " : "  earliest_arrival: ");
                printTime(trace_file, latest_dep_earliest_arr);
                trace_file << std::endl;
            }

            // Update by transfer

            // Update by trips
            std::vector<StopTripTime> relevant_trips;
            getTripsWithinTime(current_label_stop.stop_id_, path_spec.outbound_, latest_dep_earliest_arr, relevant_trips);
            for (std::vector<StopTripTime>::const_iterator it=relevant_trips.begin(); it != relevant_trips.end(); ++it) {
                if (path_spec.trace_) {
                    trace_file << "valid trips: " << it->trip_id_ << " " << it->seq_ << " ";
                    printTime(trace_file, path_spec.outbound_ ? it->arrive_time_ : it->depart_time_);
                    trace_file << std::endl;
                }

                // trip is already processed
                if (trips_done.find(it->trip_id_) != trips_done.end()) continue;

                // trip arrival time (outbound) / trip departure time (inbound)
                float arrdep_time = path_spec.outbound_ ? it->arrive_time_ : it->depart_time_;
                float wait_time = (latest_dep_earliest_arr - arrdep_time)*dir_factor;

                // deterministic path-finding: check capacities
                if (!path_spec.hyperpath_) {
                    // TODO
                }

                // get the StopTripTimes for this trip
                std::map<int, std::vector<StopTripTime> >::const_iterator tstiter = trip_stop_times_.find(it->trip_id_);
                assert(tstiter != trip_stop_times_.end());
                const std::vector<StopTripTime>& possible_stops = tstiter->second;

                // these are the relevant potential trips/stops; iterate through them
                unsigned int start_seq = path_spec.outbound_ ? 1 : it->seq_+1;
                unsigned int end_seq   = path_spec.outbound_ ? it->seq_-1 : possible_stops.size();
                for (unsigned int seq_num = start_seq; seq_num <= end_seq; ++seq_num) {
                    // possible board for outbound / alight for inbound
                    const StopTripTime& possible_board_alight = possible_stops.at(seq_num-1);

                    // new label = length of trip so far if the passenger boards/alights at this stop
                    int board_alight_stop = possible_board_alight.stop_id_;
                    StopStates::const_iterator possible_stop_state_iter = stop_states.find(board_alight_stop);

                    // hyperpath: potential successor/predessor can't be access or egress
                    if (path_spec.hyperpath_) {
                        if (possible_stop_state_iter != stop_states.end() && possible_stop_state_iter->second.size()>0) {
                            int possible_mode = possible_stop_state_iter->second.front().deparr_mode_; // first mode; why 0 index?
                            if ((possible_mode == PathFinder::MODE_ACCESS) || (possible_mode == PathFinder::MODE_EGRESS)) { continue; }
                        }
                    }

                    float   deparr_time     = path_spec.outbound_ ? possible_board_alight.depart_time_ : possible_board_alight.arrive_time_;
                    float   in_vehicle_time = (arrdep_time - deparr_time)*dir_factor;
                    bool    use_new_state   = false;
                    float   cost, new_label;

                    // stochastic/hyperpath: cost update
                    if (path_spec.hyperpath_) {
                        // TODO: genericize??
                        cost = current_label_stop.label_  +
                               in_vehicle_time            +
                               wait_time*1.77             +
                               0.00;                      // FARE/VALUE OF TIME
                        if ((current_mode == PathFinder::MODE_ACCESS) || (current_mode == PathFinder::MODE_EGRESS)) {
                            cost += (float)47.73;                      // TRANSFER PENALTY
                        }
                        float old_label = 999999; // MAX_COST
                        new_label       = cost;
                        if (possible_stop_state_iter != stop_states.end()) {
                            old_label = possible_stop_state_iter->second.back().label_;
                            new_label = float(exp(-1.0*PathFinder::DISPERSION_PARAMETER*old_label) +
                                              exp(-1.0*PathFinder::DISPERSION_PARAMETER*cost));
                            new_label = std::max(0.01, -1.0/PathFinder::DISPERSION_PARAMETER*log(new_label));
                        }
                        if ((new_label < 999999) && (new_label > 0)) { use_new_state = true; }
                    }
                    // deterministic: cost is just additive
                    else {
                        cost        = in_vehicle_time + wait_time;
                        new_label   = current_label_stop.label_ + cost;
                        float old_label = 999.999; // MAX_TIME
                        if (possible_stop_state_iter != stop_states.end()) {
                            old_label = possible_stop_state_iter->second.front().label_;
                            if (new_label < old_label) {
                                use_new_state = true;
                                // clear it - we only have one
                                stop_states[board_alight_stop].clear();
                            } else if (path_spec.trace_) {
                                StopState rej_ss = {
                                    new_label,                      // label
                                    deparr_time,                    // departure/arrival time
                                    possible_board_alight.trip_id_, // trip id
                                    current_label_stop.stop_id_,    // successor/predecessor
                                    in_vehicle_time+wait_time,      // link time
                                    cost,                           // cost
                                    arrdep_time                     // arrival/departure time
                                };
                                trace_file << " -trip     ";
                                printStopState(trace_file, board_alight_stop, rej_ss, path_spec);
                                trace_file << " - old_label ";
                                printTimeDuration(trace_file, old_label);
                                trace_file << std::endl;
                            }
                        } else {
                            use_new_state = true;
                        }
                    }

                    if (use_new_state) {
                        StopState ss = {
                            new_label,                      // label
                            deparr_time,                    // departure/arrival time
                            possible_board_alight.trip_id_, // trip id
                            current_label_stop.stop_id_,    // successor/predecessor
                            in_vehicle_time+wait_time,      // link time
                            cost,                           // cost
                            arrdep_time                     // arrival/departure time
                        };
                        stop_states[board_alight_stop].push_back(ss);
                        LabelStop ls = { new_label, board_alight_stop };
                        label_stop_queue.push( ls );

                        if (path_spec.trace_) {
                            trace_file << " +trip     ";
                            printStopState(trace_file, board_alight_stop, ss, path_spec);
                            trace_file << std::endl;
                        }
                    }
                }
                trips_done.insert(it->trip_id_);
            }

            //  Done with this label iteration!
            label_iterations += 1;
        }
    }


    /**
     * If outbound, then we're searching backwards, so this returns trips that arrive at the stop in time to depart at timepoint.
     * If inbound,  then we're searching forwards,  so this returns trips that depart at the stop time after timepoint.
     *
     * TODO: pass time_window parameter
     */
    void PathFinder::getTripsWithinTime(int stop_id, bool outbound, float timepoint, std::vector<StopTripTime>& return_trips, float time_window) const
    {
        // are there any trips for this stop?
        std::map<int, std::vector<StopTripTime> >::const_iterator mapiter = stop_trip_times_.find(stop_id);
        if (mapiter == stop_trip_times_.end()) {
            return;
        }
        for (std::vector<StopTripTime>::const_iterator it  = mapiter->second.begin();
                                                       it != mapiter->second.end();   ++it) {
            if (outbound && (it->arrive_time_ < timepoint) && (it->arrive_time_ > timepoint-time_window)) {
                return_trips.push_back(*it);
            } else if (!outbound && (it->depart_time_ > timepoint) && (it->depart_time_ < timepoint+time_window)) {
                return_trips.push_back(*it);
            }
        }
    }

    void PathFinder::printStopState(std::ostream& ostr, int stop_id, const StopState& ss, const struct PathSpecification& path_spec) const
    {
        ostr << std::setw( 8) << std::setfill(' ') << std::right << stop_id << ": ";
        if (path_spec.hyperpath_) {
            // label is a cost
            ostr << std::setw(10) << std::setprecision(4) << std::fixed << std::setfill(' ') << ss.label_;
        } else {
            // label is a time duration
            printTimeDuration(ostr, ss.label_);
        }
        ostr << "  ";
        printTime(ostr, ss.deparr_time_);
        ostr << "  ";
        printMode(ostr, ss.deparr_mode_);
        ostr << "  ";
        ostr << std::setw(10) << std::setfill(' ') << ss.succpred_;
        ostr << "  ";
        printTimeDuration(ostr, ss.link_time_);
        ostr << "  ";
        if (path_spec.hyperpath_) {
            ostr << std::setw(10) << std::setprecision(4) << std::fixed << std::setfill(' ') << ss.cost_;
        } else {
            // cost is a time duration
            printTimeDuration(ostr, ss.cost_);
        }
        ostr << "  ";
        printTime(ostr, ss.arrdep_time_);
    }

    /*
     * Assuming that timedur is duration in minutes, prints a formatted version.
     */
    void PathFinder::printTimeDuration(std::ostream& ostr, const float& timedur) const
    {
        int hours = static_cast<int>(timedur/60.0);
        float minutes = timedur - 60.0*hours;
        double minpart, secpart;
        secpart = modf(minutes, &minpart);
        secpart = secpart*60.0;
        // double intpart, fracpart;
        // fracpart = modf(secpart, &intpart);
        ostr << std::setw( 2) << std::setfill(' ') << std::right << hours << ":"; // hours
        ostr << std::setw( 2) << std::setfill('0') << static_cast<int>(minpart)      << ":"; // minutes
        int width = 5;
        if (secpart < 10) { ostr << "0"; width = 4; }
        ostr << std::left << std::setw(width) << std::setprecision( 4) << std::fixed << std::setfill(' ') << secpart << std::right; // seconds
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

    void PathFinder::printMode(std::ostream& ostr, const int& mode) const
    {
        if (mode == PathFinder::MODE_ACCESS) {
            ostr << std::setw(10) << std::setfill(' ') << "Access";
        } else if (mode == PathFinder::MODE_EGRESS) {
            ostr << std::setw(10) << std::setfill(' ') << "Egress";
        } else if (mode == PathFinder::MODE_TRANSFER) {
            ostr << std::setw(10) << std::setfill(' ') << "Transfer";
        } else {
            ostr << std::setw(10) << std::setfill(' ') << mode;
        }
    }

}
