#include "pathfinder.h"

#include <assert.h>
#include <sstream>
#include <ios>
#include <iostream>
#include <iomanip>
#include <string>
#include <math.h>
#include <algorithm>

const char kPathSeparator =
#ifdef _WIN32
                            '\\';
#else
                            '/';
#endif

#define SSTR( x ) dynamic_cast< std::ostringstream & >( std::ostringstream() << std::dec << x ).str()

namespace fasttrips {

    const double PathFinder::MAX_COST = 999999;
    const double PathFinder::MAX_TIME = 999.999;

    /**
     * This doesn't really do anything.
     */
    PathFinder::PathFinder() : process_num_(-1), TIME_WINDOW_(-1), BUMP_BUFFER_(-1), STOCH_PATHSET_SIZE_(-1), STOCH_DISPERSION_(-1)
    {
    }

    void PathFinder::initializeParameters(
        double     time_window,
        double     bump_buffer,
        int        stoch_pathset_size,
        double     stoch_dispersion)
    {
        TIME_WINDOW_        = time_window;
        BUMP_BUFFER_        = bump_buffer;
        STOCH_PATHSET_SIZE_ = stoch_pathset_size;
        STOCH_DISPERSION_   = stoch_dispersion;
    }

    void PathFinder::initializeCostCoefficients(
        double  in_vehicle_time_weight,
        double  wait_time_weight,
        double  walk_access_time_weight,
        double  walk_egress_time_weight,
        double  walk_transfer_time_weight,
        double  transfer_penalty,
        double  schedule_delay_weight,
        double  fare_per_boarding,
        double  value_of_time)
    {
        IN_VEHICLE_TIME_WEIGHT_     = in_vehicle_time_weight;
        WAIT_TIME_WEIGHT_           = wait_time_weight;
        WALK_ACCESS_TIME_WEIGHT_    = walk_access_time_weight;
        WALK_EGRESS_TIME_WEIGHT_    = walk_egress_time_weight;
        WALK_TRANSFER_TIME_WEIGHT_  = walk_transfer_time_weight;
        TRANSFER_PENALTY_           = transfer_penalty;
        SCHEDULE_DELAY_WEIGHT_      = schedule_delay_weight;
        FARE_PER_BOARDING_          = fare_per_boarding;
        VALUE_OF_TIME_              = value_of_time;
    }

    void PathFinder::initializeSupply(
        const char* output_dir,
        int         process_num,
        int*        taz_access_index,
        double*     taz_access_cost,
        int         num_tazlinks,
        int*        stoptime_index,
        double*     stoptime_times,
        int         num_stoptimes,
        int*        xfer_index,
        double*     xfer_data,
        int         num_xfers)
    {
        output_dir_  = output_dir;
        process_num_ = process_num;

        for(int i=0; i<num_tazlinks; ++i) {
            TazStopCost tsc = {
                taz_access_cost[3*i],
                taz_access_cost[3*i+1],
                taz_access_cost[3*i+2]
            };
            taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]] = tsc;
            if (false && (process_num_<= 1) && ((i<5) || (i>num_tazlinks-5))) {
                printf("access_links[%4d][%4d]=%f, %f, %f\n", taz_access_index[2*i], taz_access_index[2*i+1],
                       taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]].time_,
                       taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]].access_cost_,
                       taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]].egress_cost_);
            }
        }

        for (int i=0; i<num_stoptimes; ++i) {
            TripStopTime stt = {
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
            if (false && (process_num <= 1) && ((i<5) || (i>num_stoptimes-5))) {
                printf("stoptimes[%4d][%4d][%4d]=%f, %f\n", stoptime_index[3*i], stoptime_index[3*i+1], stoptime_index[3*i+2],
                       stoptime_times[2*i], stoptime_times[2*i+1]);
            }
        }

        for (int i=0; i<num_xfers; ++i) {
            TransferCost tc = { xfer_data[2*i], xfer_data[2*i+1] };
            transfer_links_o_d_[xfer_index[2*i]][xfer_index[2*i+1]] = tc;  // o -> d
            transfer_links_d_o_[xfer_index[2*i+1]][xfer_index[2*i]] = tc;  // d -> o
            if (false && (process_num <= 1) && ((i<5) || (i>num_stoptimes-5))) {
                printf("xfers[%4d][%4d]=%f, %f\n", xfer_index[2*i], xfer_index[2*i+1],
                       xfer_data[2*i], xfer_data[2*i+1]);
            }
        }

        // stops and trips have been renumbered by fasttrips.  Read string IDs for printing to be less confusing.
        std::string string_num_id, string_id;
        int num_id;

        // Trip num -> id
        std::ifstream trip_id_file;
        std::ostringstream ss_trip;
        ss_trip << output_dir_ << kPathSeparator << "ft_output_trip_id.txt";
        trip_id_file.open(ss_trip.str().c_str(), std::ios_base::in);
        trip_id_file >> string_num_id >> string_id;
        if (process_num <= 1) { printf("Reading %s: [%s] [%s]\n", ss_trip.str().c_str(), string_num_id.c_str(), string_id.c_str()); }
        while (trip_id_file >> num_id >> string_id) {
            if (false && (process_num <= 1) && (num_id < 5)) {
                printf("num_id=[%d] string2=[%s]\n", num_id, string_id.c_str());
            }
            trip_num_to_str_[num_id] = string_id;
        }
        trip_id_file.close();

        // Stop num -> id
        std::ifstream stop_id_file;
        std::ostringstream ss_stop;
        ss_stop << output_dir_ << kPathSeparator << "ft_output_stop_id.txt";
        stop_id_file.open(ss_stop.str().c_str(), std::ios_base::in);
        stop_id_file >> string_num_id >> string_id;
        if (process_num <= 1) { printf("Reading %s: [%s] [%s]\n", ss_stop.str().c_str(), string_num_id.c_str(), string_id.c_str()); }
        while (stop_id_file >> num_id >> string_id) {
            if (false && (process_num <= 1) && (num_id < 5)) {
                printf("num_id=[%d] string2=[%s]\n", num_id, string_id.c_str());
            }
            stop_num_to_str_[num_id] = string_id;
        }
        stop_id_file.close();
    }

    void PathFinder::setBumpWait(int*       bw_index,
                                 double*    bw_data,
                                 int        num_bw)
    {
        for (int i=0; i<num_bw; ++i) {
            TripStop ts = { bw_index[3*i], bw_index[3*i+1], bw_index[3*i+2] };
            bump_wait_[ts] = bw_data[i];
            if (true && (process_num_ <= 1) && ((i<5) || (i>num_bw-5))) {
                printf("bump_wait[%6d %6d %6d] = %f\n",
                       bw_index[3*i], bw_index[3*i+1], bw_index[3*i+2], bw_data[i] );
            }
        }
    }

    /// This doesn't really do anything because the instance variables are all STL structures
    /// which take care of freeing memory.
    PathFinder::~PathFinder()
    {
        // std::cout << "PathFinder destructor" << std::endl;
    }

    void PathFinder::findPath(PathSpecification path_spec,
                              Path& path,
                              PathInfo          &path_info) const
    {
        // for now we'll just trace
        // if (!path_spec.trace_) { return; }

        std::ofstream trace_file;
        if (path_spec.trace_) {
            std::ostringstream ss;
            ss << output_dir_ << kPathSeparator;
            ss << "fasttrips_trace_" << path_spec.path_id_ << ".log";
            // append because this will happen across iterations
            trace_file.open(ss.str().c_str(), (std::ios_base::out | std::ios_base::app));
            trace_file << "Tracing assignment of passenger " << path_spec.passenger_id_ << " with path id " << path_spec.path_id_ << std::endl;
            trace_file << "outbound_        = " << path_spec.outbound_ << std::endl;
            trace_file << "hyperpath_       = " << path_spec.hyperpath_ << std::endl;
            trace_file << "preferred_time_  = ";
            printTime(trace_file, path_spec.preferred_time_);
            trace_file << " (" << path_spec.preferred_time_ << ")" << std::endl;
        }

        StopStates      stop_states;
        LabelStopQueue  label_stop_queue;
        // todo: handle failure
        initializeStopStates(path_spec, trace_file, stop_states, label_stop_queue);

        labelStops(path_spec, trace_file, stop_states, label_stop_queue);

        std::vector<StopState> taz_state;
        finalizeTazState(path_spec, trace_file, stop_states, taz_state);

        getFoundPath(path_spec, trace_file, stop_states, taz_state, path, path_info);

        trace_file.close();
    }

    bool PathFinder::initializeStopStates(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue) const
    {
        int     start_taz_id = path_spec.outbound_ ? path_spec.destination_taz_id_ : path_spec.origin_taz_id_;
        double  dir_factor   = path_spec.outbound_ ? 1.0 : -1.0;

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
            double deparr_time = path_spec.preferred_time_ - (link_iter->second.time_*dir_factor);

            double cost;
            if (path_spec.hyperpath_) {
                cost = (path_spec.outbound_ ? link_iter->second.egress_cost_ : link_iter->second.access_cost_);
            } else {
                cost = link_iter->second.time_;
            }

            StopState ss = {
                cost,                                                                       // label
                deparr_time,                                                                // departure/arrival time
                path_spec.outbound_ ? PathFinder::MODE_EGRESS : PathFinder::MODE_ACCESS,    // departure/arrival mode
                start_taz_id,                                                               // successor/predecessor
                -1,                                                                         // sequence
                -1,                                                                         // sequence succ/pred
                link_iter->second.time_,                                                    // link time
                cost,                                                                       // cost
                PathFinder::MAX_DATETIME };                                                 // arrival/departure time
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

    void PathFinder::updateStopStatesForTransfers(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue,
        const LabelStop& current_label_stop,
        double latest_dep_earliest_arr) const
    {
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

        // current_stop_state is a vector
        std::vector<StopState>& current_stop_state = stop_states[current_label_stop.stop_id_];
        int current_mode = current_stop_state[0].deparr_mode_;      // why index 0?

        // no transfer to/from access or egress
        if (current_mode == PathFinder::MODE_EGRESS) return;
        if (current_mode == PathFinder::MODE_ACCESS) return;
        // if not hyperpath, transfer not ok
        if (!path_spec.hyperpath_ && current_mode == PathFinder::MODE_TRANSFER) return;


        double nonwalk_label = 0;
        if (path_spec.hyperpath_) {
            nonwalk_label = calculateNonwalkLabel(current_stop_state);
            if (path_spec.trace_) { trace_file << "  nonwalk label:    " << nonwalk_label << std::endl; }
        }
        // are there relevant transfers?
        std::map<int, std::map<int, TransferCost> >::const_iterator transfer_map_it;
        bool found_transfers = false;
        if (path_spec.outbound_) {
            // if outbound, going backwards, so transfer TO this current stop
            transfer_map_it = transfer_links_d_o_.find(current_label_stop.stop_id_);
            found_transfers = (transfer_map_it != transfer_links_d_o_.end());
        } else {
            // if inbound, going forwards, so transfer FROM this current stop
            transfer_map_it = transfer_links_o_d_.find(current_label_stop.stop_id_);
            found_transfers = (transfer_map_it != transfer_links_o_d_.end());
        }
        if (!found_transfers) { return; }
        for (std::map<int, TransferCost>::const_iterator transfer_it = transfer_map_it->second.begin();
             transfer_it != transfer_map_it->second.end(); ++transfer_it)
        {
            int     xfer_stop_id    = transfer_it->first;
            double  transfer_time   = transfer_it->second.time_;
            double  transfer_cost   = transfer_it->second.cost_;
            // outbound: departure time = latest departure - transfer
            //  inbound: arrival time   = earliest arrival + transfer
            double  deparr_time     = latest_dep_earliest_arr - (transfer_time*dir_factor);
            bool    use_new_state   = false;
            double  cost, new_label;
            StopStates::const_iterator possible_xfer_iter = stop_states.find(xfer_stop_id);

            // stochastic/hyperpath: cost update
            if (path_spec.hyperpath_)
            {
                cost                = nonwalk_label + transfer_cost;
                double old_label    = PathFinder::MAX_COST;
                new_label           = cost;
                if (possible_xfer_iter != stop_states.end())
                {
                    old_label       = possible_xfer_iter->second.back().label_;
                    new_label       = exp(-1.0*STOCH_DISPERSION_*old_label) +
                                      exp(-1.0*STOCH_DISPERSION_*cost);
                    new_label       = std::max(0.01, -1.0/STOCH_DISPERSION_*log(new_label));
                }
                if ((new_label < PathFinder::MAX_COST) && (new_label > 0.0)) { use_new_state = true; }

            }
            // deterministic: cost is just additive
            else
            {
                cost                = transfer_time;
                new_label           = current_label_stop.label_ + cost;

                // check (departure mode, stop) if someone's waiting already
                // curious... this only applies to OUTBOUND
                if (path_spec.outbound_)
                {
                    TripStop ts = { current_mode, current_stop_state[0].seq_, current_label_stop.stop_id_ };
                    std::map<TripStop, double, struct TripStopCompare>::const_iterator bwi = bump_wait_.find(ts);
                    if (bwi != bump_wait_.end())
                    {
                        // time a bumped passenger started waiting
                        float latest_time = bwi->second;
                        // we can't come in time
                        if (deparr_time - TIME_WINDOW_ > latest_time) { continue; }
                        // leave earlier -- to get in line 5 minutes before bump wait time
                        // (confused... We don't resimulate previous bumping passenger so why does this make sense?)
                        new_label       = new_label + (current_stop_state[0].deparr_time_ - latest_time) + BUMP_BUFFER_;
                        deparr_time     = latest_time - transfer_time - BUMP_BUFFER_;
                    }
                }
                if (possible_xfer_iter != stop_states.end())
                {
                    double old_label = possible_xfer_iter->second.front().label_;
                    if (new_label < old_label) {
                        use_new_state = true;
                        stop_states[xfer_stop_id].clear();
                    }
                } else {
                    use_new_state = true;
                }
            }

            if (use_new_state)
            {
                StopState ss = {
                    new_label,                      // label
                    deparr_time,                    // departure/arrival time
                    PathFinder::MODE_TRANSFER,      // departure/arrival mode
                    current_label_stop.stop_id_,    // successor/predecessor
                    -1,                             // sequence
                    -1,                             // sequence succ/pred
                    transfer_time,                  // link time
                    cost,                           // cost
                    PathFinder::MAX_DATETIME        // arrival/departure time
                };
                stop_states[xfer_stop_id].push_back(ss);
                LabelStop ls = { new_label, xfer_stop_id };
                label_stop_queue.push( ls );

                if (path_spec.trace_) {
                    trace_file << " +transfer ";
                    printStopState(trace_file, xfer_stop_id, ss, path_spec);
                    trace_file << std::endl;
                }
            }
        }
    }

    void PathFinder::updateStopStatesForTrips(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue,
        const LabelStop& current_label_stop,
        double latest_dep_earliest_arr,
        std::tr1::unordered_set<int>& trips_done) const
    {
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

        // current_stop_state is a vector
        std::vector<StopState>& current_stop_state = stop_states[current_label_stop.stop_id_];
        int current_mode = current_stop_state[0].deparr_mode_;      // why index 0?

        // Update by trips
        std::vector<TripStopTime> relevant_trips;
        getTripsWithinTime(current_label_stop.stop_id_, path_spec.outbound_, latest_dep_earliest_arr, relevant_trips);
        for (std::vector<TripStopTime>::const_iterator it=relevant_trips.begin(); it != relevant_trips.end(); ++it) {

            if (false && path_spec.trace_) {
                trace_file << "valid trips: " << it->trip_id_ << " " << it->seq_ << " ";
                printTime(trace_file, path_spec.outbound_ ? it->arrive_time_ : it->depart_time_);
                trace_file << std::endl;
            }

            // trip is already processed
            if (trips_done.find(it->trip_id_) != trips_done.end()) continue;

            // trip arrival time (outbound) / trip departure time (inbound)
            double arrdep_time = path_spec.outbound_ ? it->arrive_time_ : it->depart_time_;
            double wait_time = (latest_dep_earliest_arr - arrdep_time)*dir_factor;
            double arrive_time;

            // deterministic path-finding: check capacities
            if (!path_spec.hyperpath_) {
                TripStop check_for_bump_wait;
                if (path_spec.outbound_) {
                    // if outbound, this trip loop is possible trips *before* the current trip
                    // checking that we get here in time for the current trip
                    check_for_bump_wait.trip_id_ = current_stop_state[0].deparr_mode_;
                    check_for_bump_wait.seq_     = current_stop_state[0].seq_;
                    check_for_bump_wait.stop_id_ = current_label_stop.stop_id_;
                    //  arrive from the loop trip
                    arrive_time = arrdep_time;
                } else {
                    // if inbound, the trip is the next trip
                    // checking that we can get here in time for that trip
                    check_for_bump_wait.trip_id_ = it->trip_id_;
                    check_for_bump_wait.seq_     = it->seq_;
                    check_for_bump_wait.stop_id_ = current_label_stop.stop_id_;
                    // arrive for this trip
                    arrive_time = current_stop_state[0].deparr_time_;
                }
                std::map<TripStop, double, struct TripStopCompare>::const_iterator bwi = bump_wait_.find(check_for_bump_wait);
                if (bwi != bump_wait_.end()) {
                    // time a bumped passenger started waiting
                    float latest_time = bwi->second;
                    if (path_spec.trace_) {
                        trace_file << "checking latest_time ";
                        printTime(trace_file, latest_time);
                        trace_file << " vs arrive_time ";
                        printTime(trace_file, arrive_time);
                        trace_file << " for potential trip " << it->trip_id_ << std::endl;
                    }
                    if ((arrive_time + 0.01 >= latest_time) &&
                        (current_stop_state[0].deparr_mode_ != it->trip_id_)) {
                        if (path_spec.trace_) { trace_file << "Continuing" << std::endl; }
                        continue;
                    }
                }
            }

            // get the TripStopTimes for this trip
            std::map<int, std::vector<TripStopTime> >::const_iterator tstiter = trip_stop_times_.find(it->trip_id_);
            assert(tstiter != trip_stop_times_.end());
            const std::vector<TripStopTime>& possible_stops = tstiter->second;

            // these are the relevant potential trips/stops; iterate through them
            unsigned int start_seq = path_spec.outbound_ ? 1 : it->seq_+1;
            unsigned int end_seq   = path_spec.outbound_ ? it->seq_-1 : possible_stops.size();
            for (unsigned int seq_num = start_seq; seq_num <= end_seq; ++seq_num) {
                // possible board for outbound / alight for inbound
                const TripStopTime& possible_board_alight = possible_stops.at(seq_num-1);

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

                double  deparr_time     = path_spec.outbound_ ? possible_board_alight.depart_time_ : possible_board_alight.arrive_time_;
                double  in_vehicle_time = (arrdep_time - deparr_time)*dir_factor;
                bool    use_new_state   = false;
                double  cost, new_label;

                // stochastic/hyperpath: cost update
                if (path_spec.hyperpath_) {
                    // TODO: genericize??
                    if ((current_mode == PathFinder::MODE_ACCESS) || (current_mode == PathFinder::MODE_EGRESS)) {
                        cost = current_label_stop.label_                +
                               in_vehicle_time*IN_VEHICLE_TIME_WEIGHT_  +
                               0.00                                     + // wait
                               0.00                                     + // FARE/VALUE OF TIME
                               0.00;                                      // TRANSFER PENALTY
                    } else {
                        cost = current_label_stop.label_                +
                               in_vehicle_time*IN_VEHICLE_TIME_WEIGHT_  +
                               wait_time*WAIT_TIME_WEIGHT_              + // wait
                               0.00                                     + // FARE/VALUE OF TIME
                               TRANSFER_PENALTY_;                         // TRANSFER PENALTY
                    }

                    double old_label = PathFinder::MAX_COST;
                    new_label       = cost;
                    if (possible_stop_state_iter != stop_states.end()) {
                        old_label = possible_stop_state_iter->second.back().label_;
                        new_label = double(exp(-1.0*STOCH_DISPERSION_*old_label) +
                                          exp(-1.0*STOCH_DISPERSION_*cost));
                        new_label = std::max(0.01, -1.0/STOCH_DISPERSION_*log(new_label));
                    }
                    if ((new_label < PathFinder::MAX_COST) && (new_label > 0)) { use_new_state = true; }
                }
                // deterministic: cost is just additive
                else {
                    cost        = in_vehicle_time + wait_time;
                    new_label   = current_label_stop.label_ + cost;
                    double old_label = PathFinder::MAX_TIME;
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
                                possible_board_alight.seq_,     // sequence
                                it->seq_,                       // sequence succ/pred
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
                        possible_board_alight.seq_,     // sequence
                        it->seq_,                       // sequence succ/pred
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
    }

    void PathFinder::labelStops(const PathSpecification& path_spec,
                                          std::ofstream& trace_file,
                                          StopStates& stop_states,
                                          LabelStopQueue& label_stop_queue) const
    {
        int label_iterations = 0;
        std::tr1::unordered_set<int> stop_done;
        std::tr1::unordered_set<int> trips_done;
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

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
            // todo? continue if there are no transfers to/from the stop?

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
                trace_file << ", stop " << stop_num_to_str_.find(current_label_stop.stop_id_)->second << ", len "
                           << current_stop_state.size() << ") :======" << std::endl;
                trace_file << "            ";
                printStopStateHeader(trace_file, path_spec);
                trace_file << std::endl;
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
            double  latest_dep_earliest_arr = current_stop_state[0].deparr_time_;
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

            updateStopStatesForTransfers(path_spec,
                                         trace_file,
                                         stop_states,
                                         label_stop_queue,
                                         current_label_stop,
                                         latest_dep_earliest_arr);

            updateStopStatesForTrips(path_spec,
                                     trace_file,
                                     stop_states,
                                     label_stop_queue,
                                     current_label_stop,
                                     latest_dep_earliest_arr,
                                     trips_done);


            //  Done with this label iteration!
            label_iterations += 1;
        }
    }


    bool PathFinder::finalizeTazState(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const StopStates& stop_states,
        std::vector<StopState>& taz_state) const
    {
        int end_taz_id = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

        // are there any egress/access links?
        std::map<int, std::map<int, TazStopCost> >::const_iterator end_links = taz_access_links_.find(end_taz_id);
        if (end_links == taz_access_links_.end()) {
            return false;
        }

        std::map<int, TazStopCost>::const_iterator link_iter;
        for (link_iter  = end_links->second.begin();
             link_iter != end_links->second.end(); ++link_iter)
        {
            int     stop_id                 = link_iter->first;
            double  access_time             = link_iter->second.time_;

            double  earliest_dep_latest_arr = PathFinder::MAX_DATETIME;
            double  nonwalk_label           = PathFinder::MAX_COST;

            bool    use_new_state           = false;
            double  deparr_time, new_label, new_cost;

            std::map<int, std::vector<StopState> >::const_iterator stop_states_iter = stop_states.find(stop_id);
            if (stop_states_iter == stop_states.end()) { continue; }

            const std::vector<StopState>& current_stop_state = stop_states_iter->second;
            earliest_dep_latest_arr = current_stop_state[0].deparr_time_;

            if (path_spec.hyperpath_)
            {
                for (std::vector<StopState>::const_iterator ssi  = current_stop_state.begin();
                                                            ssi != current_stop_state.end(); ++ssi)
                {
                    if (path_spec.outbound_) {
                        earliest_dep_latest_arr = std::min(earliest_dep_latest_arr, ssi->deparr_time_);
                    } else {
                        earliest_dep_latest_arr = std::max(earliest_dep_latest_arr, ssi->deparr_time_);
                    }
                }
                nonwalk_label = calculateNonwalkLabel(current_stop_state);

                // todo: this should be - (access_time*dir_factor), right??
                deparr_time = earliest_dep_latest_arr - access_time;

                new_cost        = nonwalk_label + (path_spec.outbound_ ? link_iter->second.access_cost_ : link_iter->second.egress_cost_);
                double old_label= PathFinder::MAX_COST;
                new_label       = new_cost;

                if (taz_state.size() > 0)
                {
                    old_label = taz_state.back().label_;
                    new_label = exp(-1.0*STOCH_DISPERSION_*old_label) +
                                exp(-1.0*STOCH_DISPERSION_*new_label);
                    new_label = std::max(0.01, -1.0/STOCH_DISPERSION_*log(new_label));
                }
                if ((new_label < PathFinder::MAX_COST) && (new_label > 0)) { use_new_state = true; }
            }
            // deterministic
            else
            {
                deparr_time = earliest_dep_latest_arr - (access_time*dir_factor);

                // first leg has to be a trip
                if (current_stop_state.front().deparr_mode_ == PathFinder::MODE_TRANSFER) { continue; }
                if (current_stop_state.front().deparr_mode_ == PathFinder::MODE_EGRESS  ) { continue; }
                if (current_stop_state.front().deparr_mode_ == PathFinder::MODE_ACCESS  ) { continue; }
                new_cost  = (path_spec.outbound_ ? link_iter->second.time_ : link_iter->second.time_);
                new_label = current_stop_state.front().label_ + new_cost;

                // capacity check
                if (path_spec.outbound_)
                {
                    TripStop ts = { current_stop_state[0].deparr_mode_, current_stop_state[0].seq_, stop_id };
                    std::map<TripStop, double, struct TripStopCompare>::const_iterator bwi = bump_wait_.find(ts);
                    if (bwi != bump_wait_.end()) {
                        // time a bumped passenger started waiting
                        float latest_time = bwi->second;
                        // we can't come in time
                        if (deparr_time - TIME_WINDOW_ > latest_time) { continue; }
                        // leave earlier -- to get in line 5 minutes before bump wait time
                        new_label   = new_label + (current_stop_state[0].deparr_time_ - latest_time) + BUMP_BUFFER_;
                        deparr_time = latest_time - access_time - BUMP_BUFFER_;
                    }
                }

                double old_label = PathFinder::MAX_TIME;
                if (taz_state.size() > 0)
                {
                    old_label = taz_state.back().label_;
                    if (new_label < old_label)
                    {
                        use_new_state = true;
                        taz_state.clear();
                    }
                }
                else { use_new_state = true; }

            }

            if (use_new_state)
            {
                StopState ts = {
                    new_label,                                                                  // label
                    deparr_time,                                                                // departure/arrival time
                    path_spec.outbound_ ? PathFinder::MODE_ACCESS : PathFinder::MODE_EGRESS,    // departure/arrival mode
                    stop_id,                                                                    // successor/predecessor
                    -1,                                                                         // sequence
                    -1,                                                                         // sequence succ/pred
                    access_time,                                                                // link time
                    new_cost,                                                                   // cost
                    PathFinder::MAX_DATETIME                                                    // arrival/departure time
                };
                taz_state.push_back(ts);
                if (path_spec.trace_)
                {
                    trace_file << (path_spec.outbound_ ? " +access   " : " +egress   ");
                    printStopState(trace_file, end_taz_id, ts, path_spec);
                    trace_file << std::endl;
                }
            }
        }
    }


    bool PathFinder::hyperpathGeneratePath(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const StopStates& stop_states,
        const std::vector<StopState>& taz_state,
        Path& path) const
    {
        int    start_state_id   = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;
        double dir_factor       = path_spec.outbound_ ? 1 : -1;

        double taz_label        = taz_state.back().label_;
        int    cost_cutoff      = 1;

        // setup access/egress probabilities
        std::vector<ProbabilityStop> access_cum_prob; // access/egress cumulative probabilities
        for (size_t state_index = 0; state_index < taz_state.size(); ++state_index)
        {
            double probability = exp(-1.0*STOCH_DISPERSION_*taz_state[state_index].cost_) /
                                 exp(-1.0*STOCH_DISPERSION_*taz_label);
            // why?  :p
            int prob_i = static_cast<int>(RAND_MAX*probability);
            // too small to consider
            if (prob_i < cost_cutoff) { continue; }
            if (access_cum_prob.size() == 0) {
                ProbabilityStop pb = { probability, prob_i, taz_state[state_index].stop_succpred_, state_index };
                access_cum_prob.push_back( pb );
            } else {
                ProbabilityStop pb = { probability, access_cum_prob.back().prob_i_ + prob_i, taz_state[state_index].stop_succpred_, state_index };
                access_cum_prob.push_back( pb );
            }
            if (path_spec.trace_) {
                trace_file << std::setw( 6) << std::setfill(' ') << access_cum_prob.back().stop_id_ << " ";
                printMode(trace_file, taz_state[state_index].deparr_mode_);
                trace_file << ": prob ";
                trace_file << std::setw(10) << probability << " cum_prob ";
                trace_file << std::setw( 6) << access_cum_prob.back().prob_i_;
                trace_file << "; index " << access_cum_prob.back().index_ << std::endl;
            }
        }

        size_t chosen_index = chooseState(path_spec, trace_file, access_cum_prob);
        StopState ss = taz_state[chosen_index];
        path.stops_.push_back(start_state_id);
        path.states_[start_state_id] = ss;

        if (path_spec.trace_)
        {
            trace_file << " -> Chose access/egress ";
            printStopState(trace_file, start_state_id, ss, path_spec);
            trace_file << std::endl;
        }

        int     current_stop_id = ss.stop_succpred_;
        // outbound: arrival time
        //  inbound: departure time
        double  arrdep_time     = ss.deparr_time_ + (ss.link_time_*dir_factor);
        int     prev_mode       = ss.deparr_mode_;
        int     prev_stop_id    = start_state_id;
        int     prev_prev_stop_id = -1;
        while (true)
        {
            // setup probabilities
            if (path_spec.trace_) {
                trace_file << "current_stop=" << stop_num_to_str_.find(current_stop_id)->second;
                trace_file << (path_spec.outbound_ ? "; arrival_time=" : "; departure_time=");
                printTime(trace_file, arrdep_time);
                trace_file << "; prev_mode=" << prev_mode << std::endl;
                trace_file << "            ";
                printStopStateHeader(trace_file, path_spec);
                trace_file << std::endl;
            }
            std::vector<ProbabilityStop> stop_cum_prob;
            double sum_exp = 0;
            StopStates::const_iterator ssi = stop_states.find(current_stop_id);
            for (size_t stop_state_index = 0; stop_state_index < ssi->second.size(); ++stop_state_index)
            {
                const StopState& state = ssi->second[stop_state_index];

                // no double walk
                if (path_spec.outbound_ &&
                    ((state.deparr_mode_ == PathFinder::MODE_EGRESS) || (state.deparr_mode_ == PathFinder::MODE_TRANSFER)) &&
                    ((         prev_mode == PathFinder::MODE_ACCESS) || (         prev_mode == PathFinder::MODE_TRANSFER))) { continue; }
                if (!path_spec.outbound_ &&
                    ((state.deparr_mode_ == PathFinder::MODE_ACCESS) || (state.deparr_mode_ == PathFinder::MODE_TRANSFER)) &&
                    ((         prev_mode == PathFinder::MODE_EGRESS) || (         prev_mode == PathFinder::MODE_TRANSFER))) { continue; }

                // outbound: we cannot depart before we arrive
                if (path_spec.outbound_ && state.deparr_time_ < arrdep_time) { continue; }
                // inbound: we cannot arrive after we depart
                if (!path_spec.outbound_ && state.deparr_time_ > arrdep_time) { continue; }

                // calculating denominator
                sum_exp += exp(-1.0*STOCH_DISPERSION_*state.cost_);
                // probabilities will be filled in later - use cost for now
                ProbabilityStop pb = { state.cost_, 0, state.stop_succpred_, stop_state_index };
                stop_cum_prob.push_back(pb);

                if (path_spec.trace_) {
                    trace_file << "            ";
                    printStopState(trace_file, current_stop_id, state, path_spec);
                    trace_file << "  sum_exp = " << std::scientific << sum_exp << std::endl;
                }
            }

            // dead end
            if (stop_cum_prob.size() == 0) {
                return false;
            }
            if (sum_exp == 0) {
                return false;
            }

            // denom found - cum prob time
            for (size_t idx = 0; idx < stop_cum_prob.size(); ++idx) {
                double probability = exp(-1.0*STOCH_DISPERSION_*stop_cum_prob[idx].probability_) / sum_exp;

                // why?  :p
                int prob_i = static_cast<int>(RAND_MAX*probability);
                stop_cum_prob[idx].probability_ = probability;
                if (idx == 0) {
                    stop_cum_prob[idx].prob_i_ = prob_i;
                } else {
                    stop_cum_prob[idx].prob_i_ = prob_i + stop_cum_prob[idx-1].prob_i_;
                }
                if (path_spec.trace_) {
                    trace_file << std::setw( 6) << std::setfill(' ') << std::fixed << stop_cum_prob[idx].stop_id_ << " ";
                    trace_file << ": prob ";
                    trace_file << std::setw(10) << probability << " cum_prob ";
                    trace_file << std::setw( 6) << stop_cum_prob[idx].prob_i_ << std::endl;
                }
            }

            // choose!
            size_t chosen_index = chooseState(path_spec, trace_file, stop_cum_prob);
            StopState next_ss   = ssi->second[chosen_index];

            if (path_spec.trace_) {
                trace_file << " -> Chose stop link ";
                printStopState(trace_file, current_stop_id, next_ss, path_spec);
                trace_file << std::endl;
            }

            // UPDATES to states
            // Hyperpaths have some uncertainty built in which we need to rectify as we go through and choose
            // concrete path states.

            // OUTBOUND: We are choosing links in chronological order.
            if (path_spec.outbound_)
            {
                // Leave origin as late as possible
                if (prev_mode == PathFinder::MODE_ACCESS) {
                    double dep_time = getScheduledDeparture(next_ss.deparr_mode_, current_stop_id, next_ss.seq_);
                    // set departure time for the access link to perfectly catch the vehicle
                    // todo: what if there is a wait queue?
                    path.states_[prev_stop_id].arrdep_time_ = dep_time;
                    path.states_[prev_stop_id].deparr_time_ = dep_time - path.states_[start_state_id].link_time_;
                    // no wait time for the trip
                    next_ss.link_time_ = next_ss.arrdep_time_ - next_ss.deparr_time_;
                }
                // *Fix trip time*
                else if (isTrip(next_ss.deparr_mode_)) {
                    // link time is arrival time - previous arrival time
                    next_ss.link_time_ = next_ss.arrdep_time_ - arrdep_time;
                }
                // *Fix transfer times*
                else if (next_ss.deparr_mode_ == PathFinder::MODE_TRANSFER) {
                    next_ss.deparr_time_ = path.states_[prev_stop_id].arrdep_time_;   // start transferring immediately
                    next_ss.arrdep_time_ = next_ss.deparr_time_ + next_ss.link_time_;
                }
                // Egress: don't wait, just walk. Get to destination as early as possible
                else if (next_ss.deparr_mode_ == PathFinder::MODE_EGRESS) {
                    next_ss.deparr_time_ = path.states_[prev_stop_id].arrdep_time_;
                    next_ss.arrdep_time_ = next_ss.deparr_time_ + next_ss.link_time_;
                }
            }
            // INBOUND: We are choosing links in REVERSE chronological order
            else
            {
                // Leave origin as late as possible
                if (next_ss.deparr_mode_ == PathFinder::MODE_ACCESS) {
                    double dep_time = getScheduledDeparture(path.states_[prev_stop_id].deparr_mode_, current_stop_id, path.states_[prev_stop_id].seq_succpred_);
                    // set arrival time for the access link to perfectly catch the vehicle
                    // todo: what if there is a wait queue?
                    next_ss.deparr_time_ = dep_time;
                    next_ss.arrdep_time_ = next_ss.deparr_time_ - next_ss.link_time_;
                    // no wait time for the trip
                    path.states_[prev_stop_id].link_time_ = path.states_[prev_stop_id].deparr_time_ - path.states_[prev_stop_id].arrdep_time_;
                }
                // *Fix trip time*: we are choosing in reverse so pretend the wait time is zero for now to
                // accurately evaluate possible transfers in next choice.
                else if (isTrip(next_ss.deparr_mode_)) {
                    next_ss.link_time_ = next_ss.deparr_time_ - next_ss.arrdep_time_;
                    // If we just picked this trip and the previous (next in time) is transfer then we know the wait now
                    // and we can update the transfer and the trip with the real wait
                    if (prev_mode == PathFinder::MODE_TRANSFER) {
                        // move transfer time so we do it right after arriving
                        path.states_[prev_stop_id].arrdep_time_ = next_ss.deparr_time_; // depart right away
                        path.states_[prev_stop_id].deparr_time_ = next_ss.deparr_time_ + path.states_[prev_stop_id].link_time_; // arrive after walk
                        // give the wait time to the previous trip
                        path.states_[prev_prev_stop_id].link_time_ = path.states_[prev_prev_stop_id].deparr_time_ - path.states_[prev_stop_id].deparr_time_;
                    }
                    // If the previous (next in time) is another trip (so zero-walk transfer) give it wait time
                    else if (isTrip(prev_mode)) {
                        path.states_[prev_stop_id].link_time_ = path.states_[prev_stop_id].deparr_time_ - next_ss.deparr_time_;
                    }
                }
                // *Fix transfer depart/arrive times*: transfer as late as possible to preserve options for earlier trip
                else if (next_ss.deparr_mode_ == PathFinder::MODE_TRANSFER) {
                    next_ss.deparr_time_ = path.states_[prev_stop_id].arrdep_time_;
                    next_ss.arrdep_time_ = next_ss.deparr_time_ - next_ss.link_time_;
                }
                // Egress: don't wait, just walk. Get to destination as early as possible
                if (prev_mode == PathFinder::MODE_EGRESS) {
                    path.states_[prev_stop_id].arrdep_time_ = next_ss.deparr_time_;
                    path.states_[prev_stop_id].deparr_time_ = path.states_[prev_stop_id].arrdep_time_ + path.states_[prev_stop_id].link_time_;
                }
            }


            // record the choice
            path.stops_.push_back(current_stop_id);
            path.states_[current_stop_id] = next_ss;

            // move on to the next
            prev_prev_stop_id   = prev_stop_id;
            prev_stop_id        = current_stop_id;
            current_stop_id     = next_ss.stop_succpred_;
            prev_mode           = next_ss.deparr_mode_;

            // update arrival / departure time
            arrdep_time = next_ss.arrdep_time_;

            if (path_spec.trace_) {
                trace_file << " ->    Updated link ";
                printStopState(trace_file, prev_stop_id, path.states_[prev_stop_id], path_spec);
                trace_file << std::endl;
            }

            // are we done?
            if (( path_spec.outbound_ && next_ss.deparr_mode_ == PathFinder::MODE_EGRESS) ||
                (!path_spec.outbound_ && next_ss.deparr_mode_ == PathFinder::MODE_ACCESS)) {
                break;
            }

        }
        return true;
    }

    Path PathFinder::choosePath(const PathSpecification& path_spec,
        std::ofstream& trace_file,
        PathSet& paths,
        int max_prob_i) const
    {
        int random_num = rand();
        if (path_spec.trace_) { trace_file << "random_num " << random_num << " -> "; }

        // mod it by max prob
        random_num = random_num % max_prob_i;
        if (path_spec.trace_) { trace_file << random_num << std::endl; }

        for (PathSet::const_iterator psi = paths.begin(); psi != paths.end(); ++psi)
        {
            if (psi->second.prob_i_==0) { continue; }
            if (random_num <= psi->second.prob_i_) { return psi->first; }
        }
        // shouldn't get here
        printf("PathFinder::choosePath() This should never happen!\n");
    }

    size_t PathFinder::chooseState(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const std::vector<ProbabilityStop>& prob_stops) const
    {
        int random_num = rand();
        if (path_spec.trace_) { trace_file << "random_num " << random_num << " -> "; }

        // mod it by max prob
        random_num = random_num % (prob_stops.back().prob_i_);
        if (path_spec.trace_) { trace_file << random_num << std::endl; }

        for (size_t ind = 0; ind < prob_stops.size(); ++ind)
        {
            if (prob_stops[ind].prob_i_==0) { continue; }
            if (random_num <= prob_stops[ind].prob_i_) { return prob_stops[ind].index_; }
        }
        // shouldn't get here
        printf("PathFinder::chooseState() This should never happen!\n");
    }

    /** TODO: Test calculating this in python?  Or other ways of making it more flexible without compiling...
     *  For now, the magic numbers are just in here...
     */
    void PathFinder::calculatePathCost(const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const Path& path,
        PathInfo& path_info) const
    {
        // no stops - nothing to do
        if (path.stops_.size()==0) { return; }

        double num_transfers        = 0;
        double in_vehicle_min       = 0;
        double initial_wait_min     = 0;
        double transfer_wait_min    = 0;
        double transfer_walk_min    = 0;
        double access_walk_min      = 0;
        double egress_walk_min      = 0;
        double preference_delay     = 0; // arrival/departure time compared to preferred
        double fare                 = 0;

        double orig_departure_time  = 0;
        double dest_arrival_time    = 0;

        bool   first_trip           = true;
        double dir_factor           = path_spec.outbound_ ? 1.0 : -1.0;

        // iterate through the states in chronological order
        int start_ind = path_spec.outbound_ ? 0 : path.stops_.size()-1;
        int end_ind   = path_spec.outbound_ ? path.stops_.size() : -1;
        int inc       = path_spec.outbound_ ? 1 : -1;
        for (int index = start_ind; index != end_ind; index += inc)
        {
            int stop_id = path.stops_[index];
            std::map<int, StopState>::const_iterator psi = path.states_.find(stop_id);

            // ============= access =============
            if (psi->second.deparr_mode_ == PathFinder::MODE_ACCESS)
            {
                access_walk_min     = psi->second.link_time_;
                orig_departure_time = (path_spec.outbound_ ? psi->second.deparr_time_ : psi->second.deparr_time_ - access_walk_min);
            }
            // ============= egress =============
            else if (psi->second.deparr_mode_ == PathFinder::MODE_EGRESS)
            {
                egress_walk_min     = psi->second.link_time_;
                dest_arrival_time   = (path_spec.outbound_ ? psi->second.deparr_time_ + egress_walk_min : psi->second.deparr_time_);
            }
            // ============= transfer =============
            else if (psi->second.deparr_mode_ == PathFinder::MODE_TRANSFER)
            {
                transfer_walk_min  += psi->second.link_time_;
            }
            // ============= trip =============
            else
            {
                double trip_ivt_min = (psi->second.arrdep_time_ - psi->second.deparr_time_)*dir_factor;
                double wait_min     = psi->second.link_time_ - trip_ivt_min;
                if (first_trip) {
                    initial_wait_min = wait_min;
                    first_trip      = false;
                } else {
                    transfer_wait_min += wait_min;
                    num_transfers   += true;
                }
                in_vehicle_min      += trip_ivt_min;

                fare += FARE_PER_BOARDING_;
            }
        }
        // TODO: remove this stuff?!  It's just here to match FAST-TrIPs
        // orig_departure_time =  trunc(100.0*orig_departure_time)/100.0;

        if (path_spec.outbound_) {
            // outbound: preferred arrival
            preference_delay = path_spec.preferred_time_ - dest_arrival_time;
            // TODO: wrong but matches FAST-TrIPs
            preference_delay = 0.0;
        } else {
            preference_delay = orig_departure_time - path_spec.preferred_time_;
        }

        path_info.cost_ = (IN_VEHICLE_TIME_WEIGHT_*in_vehicle_min                ) +
                          (WAIT_TIME_WEIGHT_*(initial_wait_min+transfer_wait_min)) +
                          (WALK_ACCESS_TIME_WEIGHT_*access_walk_min              ) +
                          (WALK_EGRESS_TIME_WEIGHT_*egress_walk_min              ) +
                          (WALK_TRANSFER_TIME_WEIGHT_*transfer_walk_min          ) +
                          (TRANSFER_PENALTY_*num_transfers                       ) +
                          (SCHEDULE_DELAY_WEIGHT_*preference_delay               ) +
                          (60.0*fare/VALUE_OF_TIME_                              );

        if (path_spec.trace_) {
            trace_file << "calculatePathCost:" << std::endl;
            printPath(trace_file, path_spec, path);
            trace_file << std::endl;
            trace_file << "     origin departure time: ";
            printTime(trace_file, orig_departure_time);
            trace_file << " (" << orig_departure_time << ")" << std::endl;
            trace_file << "  destination arrival time: ";
            printTime(trace_file, dest_arrival_time);
            trace_file << " (" << dest_arrival_time << ")" << std::endl;
            trace_file << "            in vehicle min: " << in_vehicle_min    << std::endl;
            trace_file << "          initial wait min: " << initial_wait_min  << std::endl;
            trace_file << "         transfer wait min: " << transfer_wait_min << std::endl;
            trace_file << "           access walk min: " << access_walk_min   << std::endl;
            trace_file << "           egress walk min: " << egress_walk_min   << std::endl;
            trace_file << "         transfer walk min: " << transfer_walk_min << std::endl;
            trace_file << "             num transfers: " << num_transfers     << std::endl;
            trace_file << "           preferred delay: " << preference_delay  << std::endl;
            trace_file << "                      fare: " << fare              << std::endl;
            trace_file << "            ---> path cost: " << path_info.cost_   << std::endl;
        }
    }

    // Return success
    bool PathFinder::getFoundPath(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const StopStates& stop_states,
        const std::vector<StopState>& taz_state,
        Path& path,
        PathInfo& path_info) const
    {
        // no taz states -> no path found
        if (taz_state.size() == 0) { return false; }

        int end_taz_id = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;

        if (path_spec.hyperpath_)
        {
            // find a bunch!
            PathSet paths;
            // random seed
            srand(path_spec.path_id_);
            // find a *set of Paths*
            for (int attempts = 1; attempts <= STOCH_PATHSET_SIZE_; ++attempts)
            {
                Path new_path;
                bool path_found = hyperpathGeneratePath(path_spec, trace_file, stop_states, taz_state, new_path);

                if (path_found) {
                    if (path_spec.trace_) {
                        trace_file << "----> Found path " << attempts << " ";
                        printPathCompat(trace_file, path_spec, new_path);
                        trace_file << std::endl;
                        printPath(trace_file, path_spec, new_path);
                        trace_file << std::endl;
                    }
                    // do we already have this?  if so, increment
                    PathSet::iterator paths_iter = paths.find(new_path);
                    if (paths_iter != paths.end()) {
                        paths_iter->second.count_ += 1;
                    } else {
                        PathInfo pi = { 1, 0, false, 0, 0 };  // count is 1
                        paths[new_path] = pi;
                    }
                    if (path_spec.trace_) { trace_file << "paths size = " << paths.size() << std::endl; }
                }
            }
            // calculate the costs for those paths and the logsum
            double logsum = 0;
            for (PathSet::iterator paths_iter = paths.begin(); paths_iter != paths.end(); ++paths_iter)
            {
                calculatePathCost(path_spec, trace_file, paths_iter->first, paths_iter->second);
                if (paths_iter->second.cost_ > 0)
                {
                    logsum += exp(-1.0*STOCH_DISPERSION_*paths_iter->second.cost_);
                }
            }
            if (logsum == 0) { return false; } // fail

            // for integerized probability*1000000
            int cum_prob    = 0;
            int cost_cutoff = 1;
            // calculate the probabilities for those paths
            for (PathSet::iterator paths_iter = paths.begin(); paths_iter != paths.end(); ++paths_iter)
            {
                paths_iter->second.probability_ = exp(-1.0*STOCH_DISPERSION_*paths_iter->second.cost_)/logsum;
                // why?  :p
                int prob_i = static_cast<int>(RAND_MAX*paths_iter->second.probability_);
                // too small to consider
                if (prob_i < cost_cutoff) { continue; }
                cum_prob += prob_i;
                paths_iter->second.prob_i_ = cum_prob;

                if (path_spec.trace_)
                {
                    trace_file << "-> probability " << std::setfill(' ') << std::setw(8) << paths_iter->second.probability_;
                    trace_file << "; prob_i " << std::setw(8) << paths_iter->second.prob_i_;
                    trace_file << "; count " << std::setw(4) << paths_iter->second.count_;
                    trace_file << "; cost " << std::setw(8) << paths_iter->second.cost_;
                    trace_file << "; cap bad? " << std::setw(2) << paths_iter->second.capacity_problem_;
                    trace_file << "   ";
                    printPathCompat(trace_file, path_spec, paths_iter->first);
                    trace_file << std::endl;
                }
            }
            if (cum_prob == 0) { return false; } // fail

            // choose path
            path = choosePath(path_spec, trace_file, paths, cum_prob);
            path_info = paths[path];
        }
        else
        {
            // outbound: origin to destination
            // inbound:  destination to origin
            int final_state_type = path_spec.outbound_ ? PathFinder::MODE_EGRESS : PathFinder::MODE_ACCESS;

            StopState ss = taz_state.front(); // there's only one
            path.states_[end_taz_id] = ss;
            path.stops_.push_back(end_taz_id);

            int prev_prev_stop_id = -1;
            int prev_stop_id = end_taz_id;
            int prev_mode    = ss.deparr_mode_;

            while (ss.deparr_mode_ != final_state_type)
            {
                int stop_id = ss.stop_succpred_;
                StopStates::const_iterator ssi = stop_states.find(stop_id);
                ss          = ssi->second.front();
                path.states_[stop_id] = ss;
                path.stops_.push_back(stop_id);

                if (path_spec.outbound_)
                {
                    // Leave origin as late as possible
                    if (prev_mode == PathFinder::MODE_ACCESS) {
                        path.states_[prev_stop_id].arrdep_time_ = ss.deparr_time_;
                        path.states_[prev_stop_id].deparr_time_ = path.states_[prev_stop_id].arrdep_time_ - path.states_[prev_stop_id].link_time_;
                        // no wait time for the trip
                        path.states_[stop_id].link_time_ = path.states_[stop_id].arrdep_time_ - path.states_[stop_id].deparr_time_;
                    }
                    // *Fix trip time*
                    else if (isTrip(path.states_[stop_id].deparr_mode_)) {
                        // link time is arrival time - previous arrival time
                        path.states_[stop_id].link_time_ = path.states_[stop_id].arrdep_time_ - path.states_[prev_stop_id].arrdep_time_;
                    }
                    // *Fix transfer times*
                    else if (path.states_[stop_id].deparr_mode_ == PathFinder::MODE_TRANSFER) {
                        path.states_[stop_id].deparr_time_ = path.states_[prev_stop_id].arrdep_time_;   // start transferring immediately
                        path.states_[stop_id].arrdep_time_ = path.states_[stop_id].deparr_time_ + path.states_[stop_id].link_time_;
                    }
                    // Egress: don't wait, just walk. Get to destination as early as possible
                    else if (ss.deparr_mode_ == PathFinder::MODE_EGRESS) {
                        path.states_[stop_id].deparr_time_ = path.states_[prev_stop_id].arrdep_time_;
                        path.states_[stop_id].arrdep_time_ = path.states_[stop_id].deparr_time_ + path.states_[stop_id].link_time_;
                    }
                }
                // INBOUND: We are choosing links in REVERSE chronological order
                else
                {
                    // Leave origin as late as possible
                    if (path.states_[stop_id].deparr_mode_ == PathFinder::MODE_ACCESS) {
                        path.states_[stop_id].deparr_time_ = path.states_[prev_stop_id].arrdep_time_;
                        path.states_[stop_id].arrdep_time_ = path.states_[stop_id].deparr_time_ - path.states_[stop_id].link_time_;
                        // no wait time for the trip
                        path.states_[prev_stop_id].link_time_ = path.states_[prev_stop_id].deparr_time_ - path.states_[prev_stop_id].arrdep_time_;
                    }
                    // *Trip* - fix transfer and next trip if applicable
                    else if (isTrip(path.states_[stop_id].deparr_mode_)) {
                        // If we just picked this trip and the previous (next in time) is transfer then we know the wait now
                        // and we can update the transfer and the trip with the real wait
                        if (prev_mode == PathFinder::MODE_TRANSFER) {
                            // move transfer time so we do it right after arriving
                            path.states_[prev_stop_id].arrdep_time_ = path.states_[stop_id].deparr_time_; // depart right away
                            path.states_[prev_stop_id].deparr_time_ = path.states_[stop_id].deparr_time_ + path.states_[prev_stop_id].link_time_; // arrive after walk
                            // give the wait time to the previous trip
                            path.states_[prev_prev_stop_id].link_time_ = path.states_[prev_prev_stop_id].deparr_time_ - path.states_[prev_stop_id].deparr_time_;
                        }
                        // If the previous (next in time) is another trip (so zero-walk transfer) give it wait time
                        else if (isTrip(prev_mode)) {
                            path.states_[prev_stop_id].link_time_ = path.states_[prev_stop_id].deparr_time_ - path.states_[stop_id].deparr_time_;
                        }
                    }
                    // Egress: don't wait, just walk. Get to destination as early as possible
                    if (prev_mode == PathFinder::MODE_EGRESS) {
                        path.states_[prev_stop_id].arrdep_time_ = ss.deparr_time_;
                        path.states_[prev_stop_id].deparr_time_ = path.states_[prev_stop_id].arrdep_time_ + path.states_[prev_stop_id].link_time_;
                    }
                }
                prev_prev_stop_id = prev_stop_id;
                prev_stop_id = stop_id;
                prev_mode    = ss.deparr_mode_;
            }
            calculatePathCost(path_spec, trace_file, path, path_info);
        }
        if (path_spec.trace_)
        {
            trace_file << "Final path" << std::endl;
            printPath(trace_file, path_spec, path);
        }
    }

    /**
     * Returns the departure time for the transit vehicle from the given stop/seq for the given trip.
     * Returns -1 on failure.
     */
    double PathFinder::getScheduledDeparture(int trip_id, int stop_id, int sequence) const
    {
        std::map<int, std::vector<TripStopTime> >::const_iterator tsti = trip_stop_times_.find(trip_id);
        if (tsti == trip_stop_times_.end()) { return -1; }

        for (size_t stt_index = 0; stt_index < tsti->second.size(); ++stt_index)
        {
            if (tsti->second[stt_index].stop_id_ != stop_id) { continue; }
            // trip id matches and stop id matches -- does sequence match or is it unspecified?
            if ((sequence < 0) || (sequence == tsti->second[stt_index].seq_)) {
                return tsti->second[stt_index].depart_time_;
            }
        }
        return -1;
    }

    /**
     * If outbound, then we're searching backwards, so this returns trips that arrive at the stop in time to depart at timepoint.
     * If inbound,  then we're searching forwards,  so this returns trips that depart at the stop time after timepoint.
     */
    void PathFinder::getTripsWithinTime(int stop_id, bool outbound, double timepoint, std::vector<TripStopTime>& return_trips) const
    {
        // are there any trips for this stop?
        std::map<int, std::vector<TripStopTime> >::const_iterator mapiter = stop_trip_times_.find(stop_id);
        if (mapiter == stop_trip_times_.end()) {
            return;
        }
        for (std::vector<TripStopTime>::const_iterator it  = mapiter->second.begin();
                                                       it != mapiter->second.end();   ++it) {
            if (outbound && (it->arrive_time_ < timepoint) && (it->arrive_time_ > timepoint-TIME_WINDOW_)) {
                return_trips.push_back(*it);
            } else if (!outbound && (it->depart_time_ > timepoint) && (it->depart_time_ < timepoint+TIME_WINDOW_)) {
                return_trips.push_back(*it);
            }
        }
    }

    double PathFinder::calculateNonwalkLabel(const std::vector<StopState>& current_stop_state) const
    {
        double nonwalk_label = 0.0;
        for (std::vector<StopState>::const_iterator it = current_stop_state.begin();
             it != current_stop_state.end(); ++it)
        {
            if ((it->deparr_mode_ != PathFinder::MODE_EGRESS  ) &&
                (it->deparr_mode_ != PathFinder::MODE_TRANSFER) &&
                (it->deparr_mode_ != PathFinder::MODE_ACCESS  ))
            {
                nonwalk_label += exp(-1.0*STOCH_DISPERSION_*it->cost_);
            }
        }

        if (nonwalk_label == 0.0) {
            return PathFinder::MAX_COST;
        }
        return -1.0/STOCH_DISPERSION_*log(nonwalk_label);
    }

    void PathFinder::printPath(std::ostream& ostr, const PathSpecification& path_spec, const Path& path) const
    {
        printStopStateHeader(ostr, path_spec);
        ostr << std::endl;
        for (std::vector<int>::const_iterator stop_id_iter  = path.stops_.begin();
                                              stop_id_iter != path.stops_.end(); ++stop_id_iter)
        {
            std::map<int, StopState>::const_iterator psi = path.states_.find(*stop_id_iter);
            printStopState(ostr, *stop_id_iter, psi->second, path_spec);
            ostr << std::endl;
        }
    }

    void PathFinder::printPathCompat(std::ostream& ostr, const PathSpecification& path_spec, const Path& path) const
    {
        if (path.stops_.size() == 0)
        {
            ostr << "no_path";
            return;
        }
        // board stops, trips, alight stops
        std::string board_stops, trips, alight_stops;
        int start_ind = path_spec.outbound_ ? 0 : path.stops_.size()-1;
        int end_ind   = path_spec.outbound_ ? path.stops_.size() : -1;
        int inc       = path_spec.outbound_ ? 1 : -1;
        for (int index = start_ind; index != end_ind; index += inc)
        {
            int stop_id = path.stops_[index];
            std::map<int, StopState>::const_iterator psi = path.states_.find(stop_id);
            // only want trips
            if (psi->second.deparr_mode_ == PathFinder::MODE_ACCESS  ) { continue; }
            if (psi->second.deparr_mode_ == PathFinder::MODE_EGRESS  ) { continue; }
            if (psi->second.deparr_mode_ == PathFinder::MODE_TRANSFER) { continue; }
            if ( board_stops.length() > 0) {  board_stops += ","; }
            if (       trips.length() > 0) {        trips += ","; }
            if (alight_stops.length() > 0) { alight_stops += ","; }
            board_stops  += "s" + (path_spec.outbound_ ? SSTR(stop_id) : SSTR(psi->second.stop_succpred_));
            trips        += "t" + SSTR(psi->second.deparr_mode_);
            alight_stops += "s" + (path_spec.outbound_ ? SSTR(psi->second.stop_succpred_) : SSTR(stop_id));
        }
        ostr << " " << board_stops << " " << trips << " " << alight_stops;
    }


    void PathFinder::printStopStateHeader(std::ostream& ostr, const PathSpecification& path_spec) const
    {
        ostr << std::setw( 8) << std::setfill(' ') << std::right << "stop" << ": ";
        ostr << std::setw(13) << "label";
        ostr << std::setw(10) << (path_spec.outbound_ ? "dep_time" : "arr_time");
        ostr << std::setw(12) << (path_spec.outbound_ ? "dep_mode" : "arr_mode");
        ostr << std::setw(12) << (path_spec.outbound_ ? "successor" : "predecessor");
        ostr << std::setw( 5) << "seq";
        ostr << std::setw( 5) << (path_spec.outbound_ ? "suc" : "pred");
        ostr << std::setw(15) << "linktime";
        ostr << std::setw(17) << "cost";
        ostr << std::setw(10) << (path_spec.outbound_ ? "arr_time" : "dep_time");
    }

    void PathFinder::printStopState(std::ostream& ostr, int stop_id, const StopState& ss, const PathSpecification& path_spec) const
    {
        ostr << std::setw( 8) << std::setfill(' ') << std::right << stop_num_to_str_.find(stop_id)->second << ": ";
        if (path_spec.hyperpath_) {
            // label is a cost
            ostr << std::setw(13) << std::setprecision(4) << std::fixed << std::setfill(' ') << ss.label_;
        } else {
            // label is a time duration
            printTimeDuration(ostr, ss.label_);
        }
        ostr << "  ";
        printTime(ostr, ss.deparr_time_);
        ostr << "  ";
        printMode(ostr, ss.deparr_mode_);
        ostr << "  ";
        ostr << std::setw(10) << std::setfill(' ') << stop_num_to_str_.find(ss.stop_succpred_)->second;
        ostr << "  ";
        ostr << std::setw(3) << std::setfill(' ') << ss.seq_;
        ostr << "  ";
        ostr << std::setw(3) << std::setfill(' ') << ss.seq_succpred_;
        ostr << "  ";
        printTimeDuration(ostr, ss.link_time_);
        ostr << "  ";
        if (path_spec.hyperpath_) {
            ostr << std::setw(15) << std::setprecision(4) << std::fixed << std::setfill(' ') << ss.cost_;
        } else {
            // cost is a time duration
            ostr << "  ";
            printTimeDuration(ostr, ss.cost_);
        }
        ostr << "  ";
        printTime(ostr, ss.arrdep_time_);
    }

    /*
     * Assuming that timedur is duration in minutes, prints a formatted version.
     */
    void PathFinder::printTimeDuration(std::ostream& ostr, const double& timedur) const
    {
        int hours = static_cast<int>(timedur/60.0);
        double minutes = timedur - 60.0*hours;
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
    void PathFinder::printTime(std::ostream& ostr, const double& timemin) const
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
            // trip
            ostr << std::setw(10) << std::setfill(' ') << trip_num_to_str_.find(mode)->second;
        }
    }

    bool PathFinder::isTrip(const int& mode) const
    {
        if (mode == PathFinder::MODE_ACCESS  ) { return false; }
        if (mode == PathFinder::MODE_EGRESS  ) { return false; }
        if (mode == PathFinder::MODE_TRANSFER) { return false; }
        return true;
    }

}
