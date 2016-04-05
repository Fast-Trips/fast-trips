#include "pathfinder.h"

#include <windows.h>

#include <assert.h>
#include <sstream>
#include <ios>
#include <iostream>
#include <iomanip>
#include <stack>
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

static std::ofstream label_file;
static std::ofstream stopids_file;

namespace fasttrips {

    // access this through getTransferAttributes()
    Attributes* PathFinder::ZERO_WALK_TRANSFER_ATTRIBUTES_ = NULL;

    /**
     * This doesn't really do anything.
     */
    PathFinder::PathFinder() : process_num_(-1), BUMP_BUFFER_(-1), STOCH_PATHSET_SIZE_(-1)
    {
    }

    void PathFinder::initializeParameters(
        double     time_window,
        double     bump_buffer,
        int        stoch_pathset_size,
        double     stoch_dispersion,
        int        stoch_max_stop_process_count)
    {
        BUMP_BUFFER_                    = bump_buffer;
        STOCH_PATHSET_SIZE_             = stoch_pathset_size;
        STOCH_MAX_STOP_PROCESS_COUNT_   = stoch_max_stop_process_count;

        Hyperlink::TIME_WINDOW_         = time_window;
        Hyperlink::STOCH_DISPERSION_    = stoch_dispersion;
    }

    void PathFinder::readIntermediateFiles()
    {
        readTripIds();
        readStopIds();
        readRouteIds();
        readModeIds();
        readAccessLinks();
        readTransferLinks();
        readTripInfo();
        readWeights();
    }

    void PathFinder::readTripIds() {
        // Trips have been renumbered by fasttrips.  Read string IDs.
        // Trip num -> id
        std::ifstream trip_id_file;
        std::ostringstream ss_trip;
        ss_trip << output_dir_ << kPathSeparator << "ft_intermediate_trip_id.txt";
        trip_id_file.open(ss_trip.str().c_str(), std::ios_base::in);

        std::string string_trip_id_num, string_trip_id;
        int trip_id_num;

        trip_id_file >> string_trip_id_num >> string_trip_id;
        if (process_num_ <= 1) { 
            std::cout << "Reading " << ss_trip.str() << ": ";
            std::cout << "[" << string_trip_id_num   << "] ";
            std::cout << "[" << string_trip_id       << "] ";
        }
        while (trip_id_file >> trip_id_num >> string_trip_id) {
            trip_num_to_str_[trip_id_num] = string_trip_id;
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << trip_num_to_str_.size() << " lines" << std::endl;
        }
        trip_id_file.close();
    }

    void PathFinder::readStopIds() {
        // Stops have been renumbered by fasttrips.  Read string IDs.
        // Stop num -> id
        std::ifstream stop_id_file;
        std::ostringstream ss_stop;
        ss_stop << output_dir_ << kPathSeparator << "ft_intermediate_stop_id.txt";
        stop_id_file.open(ss_stop.str().c_str(), std::ios_base::in);

        std::string string_stop_id_num, string_stop_id;
        int stop_id_num;

        stop_id_file >> string_stop_id_num >> string_stop_id;
        if (process_num_ <= 1) { 
            std::cout << "Reading " << ss_stop.str() << ": ";
            std::cout << "[" << string_stop_id_num   << "] ";
            std::cout << "[" << string_stop_id       << "] ";
        }
        while (stop_id_file >> stop_id_num >> string_stop_id) {
            stop_num_to_str_[stop_id_num] = string_stop_id;
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << stop_num_to_str_.size() << " lines" << std::endl;
        }
        stop_id_file.close();
    }

    void PathFinder::readRouteIds() {
        // Routes have been renumbered by fasttrips. Read string IDs.
        // Route num -> id
        std::ifstream route_id_file;
        std::ostringstream ss_route;
        ss_route << output_dir_ << kPathSeparator << "ft_intermediate_route_id.txt";
        route_id_file.open(ss_route.str().c_str(), std::ios_base::in);

        std::string string_route_id_num, string_route_id;
        int route_id_num;

        route_id_file >> string_route_id_num >> string_route_id;
        if (process_num_ <= 1) { 
            std::cout << "Reading " << ss_route.str() << ": ";
            std::cout << "[" << string_route_id_num   << "] ";
            std::cout << "[" << string_route_id       << "] ";
        }
        while (route_id_file >> route_id_num >> string_route_id) {
            route_num_to_str_[route_id_num] = string_route_id;
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << route_num_to_str_.size() << " lines" << std::endl;
        }
        route_id_file.close();
    }

    void PathFinder::readModeIds() {
        // Supply modes have been renumbered by fasttrips. Read string IDs.
        // Supply mode num -> id
        std::ifstream mode_id_file;
        std::ostringstream ss_mode;
        ss_mode << output_dir_ << kPathSeparator << "ft_intermediate_supply_mode_id.txt";
        mode_id_file.open(ss_mode.str().c_str(), std::ios_base::in);

        std::string string_mode_num, string_mode;
        int mode_num;

        mode_id_file >> string_mode_num >> string_mode;
        if (process_num_ <= 1) {
            std::cout << "Reading " << ss_mode.str() << ": ";
            std::cout << "[" << string_mode_num      << "] ";
            std::cout << "[" << string_mode          << "] ";
        }
        while (mode_id_file >> mode_num >> string_mode) {
            mode_num_to_str_[mode_num] = string_mode;
            if (string_mode == "transfer") { transfer_supply_mode_ = mode_num; }
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << mode_num_to_str_.size() << " lines" << std::endl;
        }
        mode_id_file.close();
    }

    void PathFinder::readAccessLinks() {
        // Taz Access and Egress links (various supply modes)
        std::ifstream acceggr_file;
        std::ostringstream ss_accegr;
        ss_accegr << output_dir_ << kPathSeparator << "ft_intermediate_access_egress.txt";
        acceggr_file.open(ss_accegr.str().c_str(), std::ios_base::in);


        std::string string_taz_num, string_supply_mode_num, string_stop_id_num, attr_name, string_attr_value;
        int taz_num, supply_mode_num, stop_id_num;
        double attr_value;

        acceggr_file >> string_taz_num >> string_supply_mode_num >> string_stop_id_num >> attr_name >> string_attr_value;
        if (process_num_ <= 1) {
            std::cout << "Reading " << ss_accegr.str() << ": ";
            std::cout << "[" << string_taz_num         << "] ";
            std::cout << "[" << string_supply_mode_num << "] ";
            std::cout << "[" << string_stop_id_num     << "] ";
            std::cout << "[" << attr_name              << "] ";
            std::cout << "[" << string_attr_value      << "] ";
        }
        int attrs_read = 0;
        while (acceggr_file >> taz_num >> supply_mode_num >> stop_id_num >> attr_name >> attr_value) {
            taz_access_links_[taz_num][supply_mode_num][stop_id_num][attr_name] = attr_value;
            attrs_read++;
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << attrs_read << " lines" << std::endl;
        }
        acceggr_file.close();
    }

    void PathFinder::readTransferLinks() {
        // Transfer links
        std::ifstream transfer_file;
        std::ostringstream ss_transfer;
        ss_transfer << output_dir_ << kPathSeparator << "ft_intermediate_transfers.txt";
        transfer_file.open(ss_transfer.str().c_str(), std::ios_base::in);

        std::string string_from_stop_id_num, string_to_stop_id_num, attr_name, string_attr_value;
        int from_stop_id_num, to_stop_id_num;
        double attr_value;

        transfer_file >> string_from_stop_id_num >> string_to_stop_id_num >> attr_name >> string_attr_value;
        if (process_num_ <= 1) {
            std::cout << "Reading " << ss_transfer.str() << ": ";
            std::cout << "[" << string_from_stop_id_num  << "] ";
            std::cout << "[" << string_to_stop_id_num    << "] ";
            std::cout << "[" << attr_name                << "] ";
            std::cout << "[" << string_attr_value        << "] ";
        }
        int attrs_read = 0;
        while (transfer_file >> from_stop_id_num >> to_stop_id_num >> attr_name >> attr_value) {
            // o -> d -> attrs
            transfer_links_o_d_[from_stop_id_num][to_stop_id_num][attr_name] = attr_value;

            // d -> o -> attrs
            transfer_links_d_o_[to_stop_id_num][from_stop_id_num][attr_name] = attr_value;
            attrs_read++;
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << attrs_read << " lines" << std::endl;
        }
        transfer_file.close();
    }

    void PathFinder::readTripInfo() {
        std::ifstream tripinfo_file;
        std::ostringstream ss_tripinfo;
        ss_tripinfo << output_dir_ << kPathSeparator << "ft_intermediate_trip_info.txt";
        tripinfo_file.open(ss_tripinfo.str().c_str(), std::ios_base::in);

        std::string string_trip_id_num, attr_name, string_attr_value;
        int trip_id_num;
        double attr_value;

        tripinfo_file >> string_trip_id_num >> attr_name >> string_attr_value;
        if (process_num_ <= 1) {
            std::cout << "Reading " << ss_tripinfo.str() << ": ";
            std::cout << "[" << string_trip_id_num       << "] ";
            std::cout << "[" << attr_name                << "] ";
            std::cout << "[" << string_attr_value        << "] ";
        }
        int attrs_read = 0;
        while (tripinfo_file >> trip_id_num >> attr_name >> attr_value) {

            // these are special
            if (attr_name == "mode_num") {
                trip_info_[trip_id_num].supply_mode_num_ = int(attr_value);
            } else if (attr_name == "route_id_num") {
                trip_info_[trip_id_num].route_id_ = int(attr_value);
            } else {
                trip_info_[trip_id_num].trip_attr_[attr_name] = attr_value;
            }
            attrs_read++;
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << attrs_read << " lines" << std::endl;
        }
        tripinfo_file.close();
    }

    void PathFinder::readWeights() {
        // Weights
        std::ifstream weights_file;
        std::ostringstream ss_weights;
        ss_weights << output_dir_ << kPathSeparator << "ft_intermediate_weights.txt";
        weights_file.open(ss_weights.str().c_str(), std::ios_base::in);

        std::string user_class, demand_mode_type, demand_mode, string_supply_mode_num, weight_name, string_weight_value;
        int supply_mode_num;
        double weight_value;

        weights_file >> user_class >> demand_mode_type >> demand_mode >> string_supply_mode_num >> weight_name >> string_weight_value;
        if (process_num_ <= 1) {
            std::cout << "Reading " << ss_weights.str() << ": ";
            std::cout << "[" << user_class              << "] ";
            std::cout << "[" << demand_mode_type        << "] ";
            std::cout << "[" << demand_mode             << "] ";
            std::cout << "[" << string_supply_mode_num  << "] ";
            std::cout << "[" << weight_name             << "] ";
            std::cout << "[" << string_weight_value     << "] ";
        }
        int weights_read = 0;
        while (weights_file >> user_class >> demand_mode_type >> demand_mode >> supply_mode_num >> weight_name >> weight_value) {
            UserClassMode ucm = { user_class, fasttrips::MODE_ACCESS, demand_mode };
            if      (demand_mode_type == "access"  ) { ucm.demand_mode_type_ = MODE_ACCESS;  }
            else if (demand_mode_type == "egress"  ) { ucm.demand_mode_type_ = MODE_EGRESS;  }
            else if (demand_mode_type == "transit" ) { ucm.demand_mode_type_ = MODE_TRANSIT; }
            else if (demand_mode_type == "transfer") { ucm.demand_mode_type_ = MODE_TRANSFER;}
            else {
                std::cerr << "Do not understand demand_mode_type [" << demand_mode_type << "] in " << ss_weights.str() << std::endl;
                exit(2);
            }

            weight_lookup_[ucm][supply_mode_num][weight_name] = weight_value;
            weights_read++;
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << weights_read << " lines" << std::endl;
        }
        weights_file.close();
    }

    const NamedWeights* PathFinder::getNamedWeights(
        const std::string& user_class,
        DemandModeType     demand_mode_type,
        const std::string& demand_mode,
        int                suppy_mode_num) const
    {
        UserClassMode ucm = { user_class, demand_mode_type, demand_mode};
        WeightLookup::const_iterator iter_wl = weight_lookup_.find(ucm);
        if (iter_wl == weight_lookup_.end()) { return NULL; }
        SupplyModeToNamedWeights::const_iterator iter_sm2nw = iter_wl->second.find(suppy_mode_num);
        if (iter_sm2nw == iter_wl->second.end()) { return NULL; }

        return &(iter_sm2nw->second);
    }

    const Attributes* PathFinder::getAccessAttributes(
        int taz_id,
        int supply_mode_num,
        int stop_id) const
    {
        TAZSupplyStopToAttr::const_iterator tssa_iter = taz_access_links_.find(taz_id);
        if (tssa_iter == taz_access_links_.end()) { return NULL; }

        //  tssa_iter->second is a SupplyStopToAttr
        SupplyStopToAttr::const_iterator ssa_iter = tssa_iter->second.find(supply_mode_num);
        if (ssa_iter == tssa_iter->second.end()) { return NULL; }

        // ssa_iter->second is a StopToAttr
        StopToAttr::const_iterator sa_iter = ssa_iter->second.find(stop_id);
        if (sa_iter == ssa_iter->second.end()) { return NULL; }

        // sa_iter->second is Attributes
        return &(sa_iter->second);
    }

    const Attributes* PathFinder::getTransferAttributes(
        int origin_stop_id,
        int destination_stop_id) const
    {
        if (PathFinder::ZERO_WALK_TRANSFER_ATTRIBUTES_ == NULL) {
            PathFinder::ZERO_WALK_TRANSFER_ATTRIBUTES_ = new Attributes();
            // TODO: make this configurable
            (*PathFinder::ZERO_WALK_TRANSFER_ATTRIBUTES_)["walk_time_min"   ] = 0.0;
            (*PathFinder::ZERO_WALK_TRANSFER_ATTRIBUTES_)["transfer_penalty"] = 1.0;
        }

        if (origin_stop_id == destination_stop_id) {
            return PathFinder::ZERO_WALK_TRANSFER_ATTRIBUTES_;
        }
        StopStopToAttr::const_iterator ssa_iter = transfer_links_o_d_.find(origin_stop_id);
        if (ssa_iter == transfer_links_o_d_.end()) { return NULL; }

        // ssa_iter->second is a StopToAttr
        StopToAttr::const_iterator sa_iter = ssa_iter->second.find(destination_stop_id);
        if (sa_iter == ssa_iter->second.end()) { return NULL; }

        // sa_iter->second is Attributes
        return &(sa_iter->second);
    }

    const TripInfo* PathFinder::getTripInfo(int trip_id_num) const
    {
        std::map<int, TripInfo>::const_iterator it = trip_info_.find(trip_id_num);
        if (it == trip_info_.end()) { return NULL; }

        return &(it->second);
    }

    void PathFinder::initializeSupply(
        const char* output_dir,
        int         process_num,
        int*        stoptime_index,
        double*     stoptime_times,
        int         num_stoptimes)
    {
        output_dir_  = output_dir;
        process_num_ = process_num;
        readIntermediateFiles();

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
                              Path              &path,
                              PathInfo          &path_info,
                              PerformanceInfo   &performance_info) const
    {
        // for now we'll just trace
        // if (!path_spec.trace_) { return; }

        std::ofstream trace_file;
        if (path_spec.trace_) {
            std::ostringstream ss;
            ss << output_dir_ << kPathSeparator;
            ss << "fasttrips_trace_" << path_spec.path_id_ << ".log";
            // append because this will happen across iterations
            trace_file.open(ss.str().c_str(), (std::ios_base::out | (path_spec.iteration_ == 1 ? 0 : std::ios_base::app)));
            trace_file << "Tracing assignment of passenger " << path_spec.passenger_id_ << " with path id " << path_spec.path_id_ << std::endl;
            trace_file << "iteration_       = " << path_spec.iteration_ << std::endl;
            trace_file << "outbound_        = " << path_spec.outbound_  << std::endl;
            trace_file << "hyperpath_       = " << path_spec.hyperpath_ << std::endl;
            trace_file << "preferred_time_  = ";
            printTime(trace_file, path_spec.preferred_time_);
            trace_file << " (" << path_spec.preferred_time_ << ")" << std::endl;
            trace_file << "user_class_      = " << path_spec.user_class_   << std::endl;
            trace_file << "access_mode_     = " << path_spec.access_mode_  << std::endl;
            trace_file << "transit_mode_    = " << path_spec.transit_mode_ << std::endl;
            trace_file << "egress_mode_     = " << path_spec.egress_mode_  << std::endl;
            trace_file << "orig_taz_id_     = " << stop_num_to_str_.find(path_spec.origin_taz_id_     )->second << std::endl;
            trace_file << "dest_taz_id_     = " << stop_num_to_str_.find(path_spec.destination_taz_id_)->second << std::endl;

            std::ostringstream ss2;
            ss2 << output_dir_ << kPathSeparator;
            ss2 << "fasttrips_labels_ids_" << path_spec.path_id_ << ".csv";
            stopids_file.open(ss2.str().c_str(), (std::ios_base::out | (path_spec.iteration_ == 1 ? 0 : std::ios_base::app)));
            stopids_file << "stop_id,stop_id_label_iter" << std::endl;
        }

        StopStates           stop_states;
        LabelStopQueue       label_stop_queue;

        // TODO: make this platform-agnostic.  probably with std::chrono.
        // QueryPerformanceFrequency reference: https://msdn.microsoft.com/en-us/library/windows/desktop/dn553408(v=vs.85).aspx
        LARGE_INTEGER        frequency;
        LARGE_INTEGER        labeling_start_time, labeling_end_time, pathfind_end_time;
        LARGE_INTEGER        label_elapsed, pathfind_elapsed;
        QueryPerformanceFrequency(&frequency);
        QueryPerformanceCounter(&labeling_start_time);

        // todo: handle failure
        bool success = initializeStopStates(path_spec, trace_file, stop_states, label_stop_queue);

        performance_info.label_iterations_ = labelStops(path_spec, trace_file, stop_states, label_stop_queue, performance_info.max_process_count_);

        finalizeTazState(path_spec, trace_file, stop_states, label_stop_queue, performance_info.label_iterations_);

        QueryPerformanceCounter(&labeling_end_time);

        getFoundPath(path_spec, trace_file, stop_states, path, path_info);

        QueryPerformanceCounter(&pathfind_end_time);

        label_elapsed.QuadPart                = labeling_end_time.QuadPart - labeling_start_time.QuadPart;
        pathfind_elapsed.QuadPart             = pathfind_end_time.QuadPart - labeling_end_time.QuadPart;

        // We now have the elapsed number of ticks, along with the
        // number of ticks-per-second. We use these values
        // to convert to the number of elapsed milliseconds.
        // To guard against loss-of-precision, we convert
        // to microseconds *before* dividing by ticks-per-second.
        label_elapsed.QuadPart    *= 1000;
        label_elapsed.QuadPart    /= frequency.QuadPart;
        pathfind_elapsed.QuadPart *= 1000;
        pathfind_elapsed.QuadPart /= frequency.QuadPart;

        performance_info.milliseconds_labeling_    = (long)label_elapsed.QuadPart;
        performance_info.milliseconds_enumerating_ = (long)pathfind_elapsed.QuadPart;

        if (path_spec.trace_) {

            trace_file << "        label iterations: " << performance_info.label_iterations_    << std::endl;
            trace_file << "       max process count: " << performance_info.max_process_count_   << std::endl;
            trace_file << "   milliseconds labeling: " << performance_info.milliseconds_labeling_    << std::endl;
            trace_file << "milliseconds enumerating: " << performance_info.milliseconds_enumerating_ << std::endl;
            trace_file.close();
            label_file.close();
            stopids_file.close();
        }
    }

    double PathFinder::tallyLinkCost(
        const int supply_mode_num,
        const PathSpecification& path_spec,
        std::ostream& trace_file,
        const NamedWeights& weights,
        const Attributes& attributes) const
    {
        // iterate through the weights
        double cost = 0;
        if (true && path_spec.trace_) {
            trace_file << "Link cost for " << std::setw(15) << std::setfill(' ') << std::left << mode_num_to_str_.find(supply_mode_num)->second;
            trace_file << std::setw(15) << std::setfill(' ') << std::right << "weight" << " x attribute" <<std::endl;
        }

        NamedWeights::const_iterator iter_weights;
        for (iter_weights  = weights.begin();
             iter_weights != weights.end(); ++iter_weights) {

            // look for the attribute
            Attributes::const_iterator iter_attr = attributes.find(iter_weights->first);
            if (iter_attr == attributes.end()) {
                // error out??
                if (path_spec.trace_) {
                    trace_file << " => NO ATTRIBUTE CALLED " << iter_weights->first << std::endl;
                }
                std::cerr << " => NO ATTRIBUTE CALLED " << iter_weights->first << std::endl;
                continue;
            }

            cost += iter_weights->second * iter_attr->second;
            if (true && path_spec.trace_) {
                trace_file << std::setw(26) << std::setfill(' ') << std::right << iter_weights->first << ":  + ";
                trace_file << std::setw(13) << std::setprecision(4) << std::fixed << iter_weights->second;
                trace_file << " x " << iter_attr->second << std::endl;
            }
        }
        if (true && path_spec.trace_) {
            trace_file << std::setw(26) << std::setfill(' ') << "final cost" << ":  = ";
            trace_file << std::setw(13) << std::setprecision(4) << std::fixed << cost << std::endl;
        }
        return cost;
    }

    void PathFinder::addStopState(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const int stop_id,
        const StopState& ss,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue) const
    {
        // do we even want to incorporate this link to our stop state?
        bool rejected = false;

        // initialize the hyperlink if we need to
        if (stop_states.find(stop_id) == stop_states.end()) {
            Hyperlink h(stop_id);
            stop_states[stop_id] = h;
        }

        Hyperlink& hyperlink = stop_states[stop_id];

        // keep track if the state changed (label or time window)
        // if so, we'll want to trigger dealing with the effects by adding it to the queue
        bool update_state = hyperlink.addLink(ss, rejected, trace_file, path_spec, *this);

        if (update_state) {
            LabelStop ls = { hyperlink.hyperpathCost(isTrip(ss.deparr_mode_)), stop_id, isTrip(ss.deparr_mode_) };

            // push this stop and it's departure time / arrival time for processing
            label_stop_queue.push( ls );
        }

        // the rest is for debugging
        if (!path_spec.trace_) { return; }

        if (rejected) { return; }

        static int link_num = 1;        // unique ID for the link
        static int last_iter = -1;   // last stop id that got numbered

        if (!label_file.is_open()) {
            link_num = 1;  // reset

            std::ostringstream ss;
            ss << output_dir_ << kPathSeparator;
            ss << "fasttrips_labels_" << path_spec.path_id_ << ".csv";
            label_file.open(ss.str().c_str(), (std::ios_base::out | (path_spec.iteration_ == 1 ? 0 : std::ios_base::app)));
            label_file << "label_iteration,link,node ID,time,mode,trip_id,link_time,link_cost,cost,AB" << std::endl;
        }

        // write the labels out to the label csv
        for (int o_d = 0; o_d < 2; ++o_d) {
            // print it into the labels file
            label_file << ss.iteration_ << ",";
            label_file << link_num      << ",";

            if (o_d == 0) { label_file << stop_num_to_str_.find(stop_id)->second << ","; }
            else          { label_file << stop_num_to_str_.find(ss.stop_succpred_)->second << ","; }

            if (o_d == 0) { label_file << ss.deparr_time_ << ","; }
            else          { label_file << ss.arrdep_time_ << ","; }

            // mode
            printMode(label_file, ss.deparr_mode_, ss.trip_id_);
            label_file << ",";

            // trip id
            if (ss.deparr_mode_ == MODE_TRANSIT) {
                label_file << trip_num_to_str_.find(ss.trip_id_)->second << ",";
            } else {
                label_file << mode_num_to_str_.find(ss.trip_id_)->second << ",";
            }
            label_file << ss.link_time_ << ",";
            label_file << ss.link_cost_ << ",";
            label_file << std::fixed << ss.cost_ << ",";
            if      ( path_spec.outbound_ && o_d == 0) { label_file << "A" << std::endl; }
            else if (!path_spec.outbound_ && o_d == 1) { label_file << "A" << std::endl; }
            else                                       { label_file << "B" << std::endl; }
        }
        ++link_num;
    }

    bool PathFinder::initializeStopStates(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue) const
    {
        int     start_taz_id = path_spec.outbound_ ? path_spec.destination_taz_id_ : path_spec.origin_taz_id_;
        double  dir_factor   = path_spec.outbound_ ? 1.0 : -1.0;

        // are there any egress/access links for this TAZ?
        TAZSupplyStopToAttr::const_iterator iter_tss2a = taz_access_links_.find(start_taz_id);
        if (iter_tss2a == taz_access_links_.end()) {
            return false;
        }

        // Are there any supply modes for this demand mode?
        UserClassMode ucm = { path_spec.user_class_,
                              path_spec.outbound_ ? MODE_EGRESS: MODE_ACCESS,
                              path_spec.outbound_ ? path_spec.egress_mode_ : path_spec.access_mode_
                            };
        WeightLookup::const_iterator iter_weights = weight_lookup_.find(ucm);
        if (iter_weights == weight_lookup_.end()) {
            std::cerr << "Couldn't find any weights configured for user class [" << path_spec.user_class_ << "], ";
            std::cerr << (path_spec.outbound_ ? "egress mode [" : "access mode [");
            std::cerr << (path_spec.outbound_ ? path_spec.egress_mode_ : path_spec.access_mode_) << "]" << std::endl;
            return false;
        }

        if (path_spec.trace_) {
            stopids_file << stop_num_to_str_.find(start_taz_id)->second << ",0" << std::endl;
        }

        // Iterate through valid supply modes
        SupplyModeToNamedWeights::const_iterator iter_s2w;
        for (iter_s2w  = iter_weights->second.begin();
             iter_s2w != iter_weights->second.end(); ++iter_s2w) {
            int supply_mode_num = iter_s2w->first;

            if (path_spec.trace_) {
                trace_file << "Weights exist for supply mode " << supply_mode_num << " => ";
                trace_file << mode_num_to_str_.find(supply_mode_num)->second << std::endl;
            }

            // Are there any egress/access links for the supply mode?
            SupplyStopToAttr::const_iterator iter_ss2a = iter_tss2a->second.find(supply_mode_num);
            if (iter_ss2a == iter_tss2a->second.end()) {
                if (path_spec.trace_) {
                    trace_file << "No links for this supply mode" << std::endl;
                }
                continue;
            }

            // Iterate through the links for the given supply mode
            StopToAttr::const_iterator link_iter;
            for (link_iter  = iter_ss2a->second.begin();
                 link_iter != iter_ss2a->second.end(); ++link_iter)
            {
                int stop_id = link_iter->first;
                Attributes link_attr = link_iter->second;
                double attr_time = link_attr.find("time_min")->second;

                // outbound: departure time = destination - access
                // inbound:  arrival time   = origin      + access
                double deparr_time = path_spec.preferred_time_ - (attr_time*dir_factor);
                // we start out with no delay
                link_attr["preferred_delay_min"] = 0.0;

                double cost;
                if (path_spec.hyperpath_) {
                    cost = tallyLinkCost(supply_mode_num, path_spec, trace_file, iter_s2w->second, link_attr);
                } else {
                    cost = attr_time;
                }

                StopState ss = {
                    deparr_time,                                                                // departure/arrival time
                    path_spec.outbound_ ? MODE_EGRESS : MODE_ACCESS,                            // departure/arrival mode
                    supply_mode_num,                                                            // trip id
                    start_taz_id,                                                               // successor/predecessor
                    -1,                                                                         // sequence
                    -1,                                                                         // sequence succ/pred
                    attr_time,                                                                  // link time
                    cost,                                                                       // link cost
                    cost,                                                                       // cost
                    0,                                                                          // iteration
                    path_spec.preferred_time_ };                                                 // arrival/departure time
                addStopState(path_spec, trace_file, stop_id, ss, stop_states, label_stop_queue);

            } // end iteration through links for the given supply mode
        } // end iteration through valid supply modes

        if (label_stop_queue.size() > 0)
            return true;
        return false;
    }

    /**
     * Part of the labeling loop. Assuming the *current_label_stop* was just pulled off the
     * *label_stop_queue*, this method will iterate through transfers to (for outbound) or
     * from (for inbound) the current stop and update the next stop given the current stop state.
     **/
    void PathFinder::updateStopStatesForTransfers(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue,
        int label_iteration,
        const LabelStop& current_label_stop) const
    {
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

        // current_stop_state is a hyperlink
        // It should have trip-states in it, because otherwise it wouldn't have come up in the label stop queue to process
        Hyperlink& current_stop_state  = stop_states[current_label_stop.stop_id_];
        int    current_mode            = current_stop_state.lowestCostStopState(true).deparr_mode_;
        int    current_trip            = current_stop_state.lowestCostStopState(true).trip_id_;
        double current_deparr_time     = current_stop_state.lowestCostStopState(true).deparr_time_;

        double nonwalk_label           = current_stop_state.hyperpathCost(true);

        // Lookup transfer weights
        // TODO: returning here is probably terrible and we shouldn't be silent... We should have zero weights if we don't want to penalize.
        const NamedWeights* transfer_weights = getNamedWeights(path_spec.user_class_, MODE_TRANSFER, "transfer", transfer_supply_mode_);
        if (transfer_weights == NULL) { return; }

        // add zero-walk transfer to this stop
        int               xfer_stop_id  = current_label_stop.stop_id_;
        const Attributes* zerowalk_xfer = getTransferAttributes(xfer_stop_id, xfer_stop_id);
        double            transfer_time = zerowalk_xfer->find("walk_time_min")->second;  // todo: make this a different time?
        double            deparr_time   = current_deparr_time - (transfer_time*dir_factor);
        double            link_cost, cost;
        if (path_spec.hyperpath_)
        {
            link_cost = tallyLinkCost(transfer_supply_mode_, path_spec, trace_file, *transfer_weights, *zerowalk_xfer);
            cost      = nonwalk_label + link_cost;
        } else {
            link_cost = transfer_time;
            cost      = current_label_stop.label_ + link_cost;
        }
        // addStopState will handle logic of updating total cost
        StopState ss = {
            deparr_time,                    // departure/arrival time
            MODE_TRANSFER,                  // departure/arrival mode
            1 ,                             // trip id
            current_label_stop.stop_id_,    // successor/predecessor
            -1,                             // sequence
            -1,                             // sequence succ/pred
            transfer_time,                  // link time
            link_cost,                      // link cost
            cost,                           // cost
            label_iteration,                // label iteration
            current_deparr_time             // arrival/departure time
        };
        addStopState(path_spec, trace_file, xfer_stop_id, ss, stop_states, label_stop_queue);

        // are there other relevant transfers?
        // if outbound, going backwards, so transfer TO this current stop
        // if inbound, going forwards, so transfer FROM this current stop
        const StopStopToAttr&          transfer_links  = (path_spec.outbound_ ? transfer_links_d_o_ : transfer_links_o_d_);
        StopStopToAttr::const_iterator transfer_map_it = transfer_links.find(current_label_stop.stop_id_);
        bool                           found_transfers = (transfer_map_it != transfer_links.end());

        if (!found_transfers) { return; }

        for (StopToAttr::const_iterator transfer_it = transfer_map_it->second.begin();
             transfer_it != transfer_map_it->second.end(); ++transfer_it)
        {
            xfer_stop_id    = transfer_it->first;
            transfer_time   = transfer_it->second.find("time_min")->second;
            // outbound: departure time = latest departure - transfer
            //  inbound: arrival time   = earliest arrival + transfer
            deparr_time     = current_deparr_time - (transfer_time*dir_factor);

            // stochastic/hyperpath: cost update
            if (path_spec.hyperpath_)
            {
                Attributes link_attr            = transfer_it->second;
                link_attr["transfer_penalty"]   = 1.0;
                link_cost                       = tallyLinkCost(transfer_supply_mode_, path_spec, trace_file, *transfer_weights, link_attr);
                cost                            = nonwalk_label + link_cost;
            }
            // deterministic: label = cost = total time, just additive
            else
            {
                link_cost           = transfer_time;
                cost                = current_label_stop.label_ + link_cost;

                // check (departure mode, stop) if someone's waiting already
                // curious... this only applies to OUTBOUND
                // TODO: capacity stuff
                if (path_spec.outbound_)
                {
                    TripStop ts = { current_trip, current_stop_state.lowestCostStopState(true).seq_, current_label_stop.stop_id_ };
                    std::map<TripStop, double, struct TripStopCompare>::const_iterator bwi = bump_wait_.find(ts);
                    if (bwi != bump_wait_.end())
                    {
                        // time a bumped passenger started waiting
                        double latest_time = bwi->second;
                        // we can't come in time
                        if (deparr_time - Hyperlink::TIME_WINDOW_ > latest_time) { continue; }
                        // leave earlier -- to get in line 5 minutes before bump wait time
                        // (confused... We don't resimulate previous bumping passenger so why does this make sense?)
                        cost            = cost + (current_stop_state.lowestCostStopState(true).deparr_time_ - latest_time) + BUMP_BUFFER_;
                        deparr_time     = latest_time - transfer_time - BUMP_BUFFER_;
                    }
                }
            }

            // addStopState will handle logic of updating total cost
            StopState ss = {
                deparr_time,                    // departure/arrival time
                MODE_TRANSFER,                  // departure/arrival mode
                1 ,                             // trip id
                current_label_stop.stop_id_,    // successor/predecessor
                -1,                             // sequence
                -1,                             // sequence succ/pred
                transfer_time,                  // link time
                link_cost,                      // link cost
                cost,                           // cost
                label_iteration,                // label iteration
                current_deparr_time             // arrival/departure time
            };
            addStopState(path_spec, trace_file, xfer_stop_id, ss, stop_states, label_stop_queue);
        }
    }

    void PathFinder::updateStopStatesForTrips(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue,
        int label_iteration,
        const LabelStop& current_label_stop,
        std::tr1::unordered_set<int>& trips_done) const
    {
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

        // for weight lookup
        UserClassMode ucm = { path_spec.user_class_, MODE_TRANSIT, path_spec.transit_mode_};
        WeightLookup::const_iterator iter_weights = weight_lookup_.find(ucm);
        if (iter_weights == weight_lookup_.end()) {
            return;
        }

        // current_stop_state is a hyperlink
        Hyperlink& current_stop_state       = stop_states[current_label_stop.stop_id_];
        // this is the latest departure/earliest arriving walk link
        double     latest_dep_earliest_arr  = current_stop_state.latestDepartureEarliestArrival(false);

        // Update by trips
        std::vector<TripStopTime> relevant_trips;
        getTripsWithinTime(current_label_stop.stop_id_, path_spec.outbound_, latest_dep_earliest_arr, relevant_trips);
        for (std::vector<TripStopTime>::const_iterator it=relevant_trips.begin(); it != relevant_trips.end(); ++it) {

            // the trip info for this trip
            const TripInfo& trip_info = trip_info_.find(it->trip_id_)->second;

            // get the weights applicable for this trip
            SupplyModeToNamedWeights::const_iterator iter_sm2nw = iter_weights->second.find(trip_info.supply_mode_num_);
            if (iter_sm2nw == iter_weights->second.end()) {
                // this supply mode isn't allowed for the userclass/demand mode
                continue;
            }
            const NamedWeights& named_weights = iter_sm2nw->second;

            if (true && path_spec.trace_) {
                trace_file << "valid trips: " << trip_num_to_str_.find(it->trip_id_)->second << " " << it->seq_ << " ";
                printTime(trace_file, path_spec.outbound_ ? it->arrive_time_ : it->depart_time_);
                trace_file << std::endl;
            }

            // trip arrival time (outbound) / trip departure time (inbound)
            double arrdep_time                = path_spec.outbound_ ? it->arrive_time_ : it->depart_time_;
            // this is our best guess link in the current_stop_state hyperlink that's relevant
            const  StopState& best_guess_link = current_stop_state.bestGuessLink(path_spec.outbound_, arrdep_time);
            double wait_time                  = (best_guess_link.deparr_time_ - arrdep_time)*dir_factor;
            if (wait_time < 0) {
                std::cerr << "wait_time < 0 -- this shouldn't happen!" << std::endl;
                if (path_spec.trace_) { trace_file << "wait_time < 0 -- this shouldn't happen!" << std::endl; }
            }

            // deterministic path-finding: check capacities
            if (!path_spec.hyperpath_) {
                TripStop check_for_bump_wait;
                double arrive_time;
                if (path_spec.outbound_) {
                    // if outbound, this trip loop is possible trips *before* the current trip
                    // checking that we get here in time for the current trip
                    check_for_bump_wait.trip_id_ = current_stop_state.lowestCostStopState(false).trip_id_;
                    check_for_bump_wait.seq_     = current_stop_state.lowestCostStopState(false).seq_;
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
                    arrive_time = current_stop_state.lowestCostStopState(false).deparr_time_;
                }
                std::map<TripStop, double, struct TripStopCompare>::const_iterator bwi = bump_wait_.find(check_for_bump_wait);
                if (bwi != bump_wait_.end()) {
                    // time a bumped passenger started waiting
                    double latest_time = bwi->second;
                    if (path_spec.trace_) {
                        trace_file << "checking latest_time ";
                        printTime(trace_file, latest_time);
                        trace_file << " vs arrive_time ";
                        printTime(trace_file, arrive_time);
                        trace_file << " for potential trip " << it->trip_id_ << std::endl;
                    }
                    if ((arrive_time + 0.01 >= latest_time) &&
                        (current_stop_state.lowestCostStopState(false).trip_id_ != it->trip_id_)) {
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
                /*
                if (path_spec.hyperpath_) {
                    if (possible_stop_state_iter != stop_states.end() && possible_stop_state_iter->second.size()>0) {
                        int possible_mode = possible_stop_state_iter->second.lowestCostStopState().deparr_mode_; // first mode; why 0 index?
                        if ((possible_mode == MODE_ACCESS) || (possible_mode == MODE_EGRESS)) { continue; }
                    }
                }
                */

                double  deparr_time     = path_spec.outbound_ ? possible_board_alight.depart_time_ : possible_board_alight.arrive_time_;
                // the schedule crossed midnight
                if (path_spec.outbound_ && arrdep_time < deparr_time) {
                    deparr_time -= 24*60;
                    if (path_spec.trace_) { trace_file << "trip crossed midnight; adjusting deparr_time" << std::endl; }
                } else if (!path_spec.outbound_ && deparr_time < arrdep_time) {
                    deparr_time += 24*60;
                    if (path_spec.trace_) { trace_file << "trip crossed midnight; adjusting deparr_time" << std::endl; }
                }
                double  in_vehicle_time = (arrdep_time - deparr_time)*dir_factor;
                double  cost      = 0;
                double  link_cost = 0;

                if (in_vehicle_time < 0) {
                    printf("in_vehicle_time < 0 -- this shouldn't happen\n");
                    if (path_spec.trace_) { trace_file << "in_vehicle_time < 0 -- this shouldn't happen!" << std::endl; }
                }

                // stochastic/hyperpath: cost update
                if (path_spec.hyperpath_) {

                    // start with trip info attributes
                    Attributes link_attr = trip_info.trip_attr_;
                    link_attr["in_vehicle_time_min"] = in_vehicle_time;
                    link_attr["wait_time_min"      ] = wait_time;

                    link_cost = 0;
                    // If outbound, and the current link is egress, then it's as late as possible and the wait time isn't accurate.
                    // It should be a preferred delay time instead
                    // ditto for inbound and access
                    if (( path_spec.outbound_ && best_guess_link.deparr_mode_ == MODE_EGRESS) ||
                        (!path_spec.outbound_ && best_guess_link.deparr_mode_ == MODE_ACCESS)) {
                        link_attr["wait_time_min"      ] = 0;


                        // TODO: this is awkward... setting this all up again.  Plus we don't have all the attributes set.  Cache something?
                        Attributes delay_attr;
                        delay_attr["time_min"           ] = 0;
                        delay_attr["preferred_delay_min"] = wait_time;
                        UserClassMode delay_ucm = { path_spec.user_class_,
                                                    path_spec.outbound_ ? MODE_EGRESS: MODE_ACCESS,
                                                    path_spec.outbound_ ? path_spec.egress_mode_ : path_spec.access_mode_
                                                  };
                        WeightLookup::const_iterator delay_iter_weights = weight_lookup_.find(delay_ucm);
                        if (delay_iter_weights != weight_lookup_.end()) {
                            SupplyModeToNamedWeights::const_iterator delay_iter_s2w = delay_iter_weights->second.find(best_guess_link.trip_id_);
                            if (delay_iter_s2w != delay_iter_weights->second.end()) {
                                link_cost = tallyLinkCost(best_guess_link.trip_id_, path_spec, trace_file, delay_iter_s2w->second, delay_attr);
                            }
                        }
                    }

                    // This is for if we calculate the transfer penalty on the transit links.
                    // I think we can't do this as it's problematic
                    // TODO: devise test to demonstrate
                    if ((best_guess_link.deparr_mode_ == MODE_ACCESS) || (best_guess_link.deparr_mode_ == MODE_EGRESS)) {
                        link_attr["transfer_penalty"] = 0.0;
                    } else {
                        link_attr["transfer_penalty"] = 1.0;
                    }

                    link_cost = link_cost + tallyLinkCost(trip_info.supply_mode_num_, path_spec, trace_file, named_weights, link_attr);
                    cost      = current_stop_state.hyperpathCost(false) + link_cost;

                }
                // deterministic: label = cost = total time, just additive
                else {
                    link_cost   = in_vehicle_time + wait_time;
                    cost        = current_stop_state.lowestCostStopState(false).cost_ + link_cost;
                }

                StopState ss = {
                    deparr_time,                    // departure/arrival time
                    MODE_TRANSIT,                   // departure/arrival mode
                    possible_board_alight.trip_id_, // trip id
                    current_label_stop.stop_id_,    // successor/predecessor
                    possible_board_alight.seq_,     // sequence
                    it->seq_,                       // sequence succ/pred
                    in_vehicle_time+wait_time,      // link time
                    link_cost,                      // link cost
                    cost,                           // cost
                    label_iteration,                // label iteration
                    arrdep_time                     // arrival/departure time
                };
                addStopState(path_spec, trace_file, board_alight_stop, ss, stop_states, label_stop_queue);

            }
            trips_done.insert(it->trip_id_);
        }
    }

    int PathFinder::labelStops(const PathSpecification& path_spec,
                                          std::ofstream& trace_file,
                                          StopStates& stop_states,
                                          LabelStopQueue& label_stop_queue,
                                          int& max_process_count) const
    {
        int label_iterations = 1;
        std::tr1::unordered_set<int> stop_done;
        std::tr1::unordered_set<int> trips_done;
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;
        LabelStop last_label_stop;

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
            LabelStop current_label_stop = label_stop_queue.pop_top(stop_num_to_str_, path_spec.trace_, trace_file);

            // if we just processed this one, then skip since it'll be a no-op
            if ((current_label_stop.stop_id_ == last_label_stop.stop_id_) && (current_label_stop.is_trip_ == last_label_stop.is_trip_)) { continue; }

            // hyperpath only
            if (path_spec.hyperpath_) {
                // have we hit the configured limit?
                if ((STOCH_MAX_STOP_PROCESS_COUNT_ > 0) &&
                    (stop_states[current_label_stop.stop_id_].processCount(current_label_stop.is_trip_) == STOCH_MAX_STOP_PROCESS_COUNT_)) {
                    if (path_spec.trace_) {
                        trace_file << "Pulling from label_stop_queue but stop " << stop_num_to_str_.find(current_label_stop.stop_id_)->second;
                        trace_file << " is_trip " << current_label_stop.is_trip_;
                        trace_file << " has been processed the limit " << STOCH_MAX_STOP_PROCESS_COUNT_ << " times so skipping." << std::endl;
                    }
                    continue;
                }
                // stop is processing
                stop_states[current_label_stop.stop_id_].incrementProcessCount(current_label_stop.is_trip_);
                max_process_count = max(max_process_count, stop_states[current_label_stop.stop_id_].processCount(current_label_stop.is_trip_));
            }

            // no transfers to the stop
            // todo? continue if there are no transfers to/from the stop?

            // current_stop_state is a hyperlink
            Hyperlink& current_stop_state = stop_states[current_label_stop.stop_id_];

            if (path_spec.trace_) {
                trace_file << "Pulling from label_stop_queue (iteration " << std::setw( 6) << std::setfill(' ') << label_iterations;
                trace_file << ", stop " << stop_num_to_str_.find(current_label_stop.stop_id_)->second;
                trace_file << ", is_trip " << current_label_stop.is_trip_;
                if (path_spec.hyperpath_) {
                    trace_file << ", label ";
                    trace_file << std::setprecision(6) << current_label_stop.label_;
                }
                trace_file << ") :======" << std::endl;
                current_stop_state.print(trace_file, path_spec, *this);
                trace_file << "==============================" << std::endl;

                stopids_file << stop_num_to_str_.find(current_label_stop.stop_id_)->second << "," << label_iterations << std::endl;
            }

            // if the low cost is trip ids, process transfers
            if (current_label_stop.is_trip_)
            {
                updateStopStatesForTransfers(path_spec,
                                             trace_file,
                                             stop_states,
                                             label_stop_queue,
                                             label_iterations,
                                             current_label_stop);
            }
            // else the low cost is walk links, so process trips
            else
            {
                updateStopStatesForTrips(path_spec,
                                         trace_file,
                                         stop_states,
                                         label_stop_queue,
                                         label_iterations,
                                         current_label_stop,
                                         trips_done);
            }

            //  Done with this label iteration!
            label_iterations += 1;

            last_label_stop = current_label_stop;
        }
        return label_iterations;
    }


    bool PathFinder::finalizeTazState(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue,
        int label_iteration) const
    {
        int end_taz_id = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

        // are there any egress/access links?
        TAZSupplyStopToAttr::const_iterator iter_tss2a = taz_access_links_.find(end_taz_id);
        if (iter_tss2a == taz_access_links_.end()) {
            return false;
        }

        // Are there any supply modes for this demand mode?
        UserClassMode ucm = { path_spec.user_class_,
                              path_spec.outbound_ ? MODE_ACCESS: MODE_EGRESS,
                              path_spec.outbound_ ? path_spec.access_mode_ : path_spec.egress_mode_
                            };
        WeightLookup::const_iterator iter_weights = weight_lookup_.find(ucm);
        if (iter_weights == weight_lookup_.end()) {
            std::cerr << "Couldn't find any weights configured for user class [" << path_spec.user_class_ << "], ";
            std::cerr << (path_spec.outbound_ ? "egress mode [" : "access mode [");
            std::cerr << (path_spec.outbound_ ? path_spec.egress_mode_ : path_spec.access_mode_) << "]" << std::endl;
            return false;
        }

        if (path_spec.trace_) {
            stopids_file << stop_num_to_str_.find(end_taz_id)->second << "," << label_iteration << std::endl;
        }

        // Iterate through valid supply modes
        SupplyModeToNamedWeights::const_iterator iter_s2w;
        for (iter_s2w  = iter_weights->second.begin();
             iter_s2w != iter_weights->second.end(); ++iter_s2w) {
            int supply_mode_num = iter_s2w->first;

            if (path_spec.trace_) {
                trace_file << "Weights exist for supply mode " << supply_mode_num << " => ";
                trace_file << mode_num_to_str_.find(supply_mode_num)->second << std::endl;
            }

            // Are there any egress/access links for the supply mode?
            SupplyStopToAttr::const_iterator iter_ss2a = iter_tss2a->second.find(supply_mode_num);
            if (iter_ss2a == iter_tss2a->second.end()) {
                if (path_spec.trace_) {
                    trace_file << "No links for this supply mode" << std::endl;
                }
                continue;
            }

            // Iterate through the links for the given supply mode
            StopToAttr::const_iterator link_iter;
            for (link_iter  = iter_ss2a->second.begin();
                 link_iter != iter_ss2a->second.end(); ++link_iter)
            {
                int     stop_id                 = link_iter->first;
                Attributes link_attr            = link_iter->second;
                link_attr["preferred_delay_min"]= 0.0;

                double  access_time             = link_attr.find("time_min")->second;

                double  earliest_dep_latest_arr = PathFinder::MAX_DATETIME;

                bool    use_new_state           = false;
                double  deparr_time, link_cost, cost;

                StopStates::const_iterator stop_states_iter = stop_states.find(stop_id);
                if (stop_states_iter == stop_states.end()) { continue; }

                const Hyperlink& current_stop_state = stop_states_iter->second;
                // if there are no trip links, this isn't viable
                if (current_stop_state.size(true) == 0) { continue; }

                earliest_dep_latest_arr = current_stop_state.lowestCostStopState(true).deparr_time_;

                if (path_spec.hyperpath_)
                {
                    earliest_dep_latest_arr = current_stop_state.earliestDepartureLatestArrival(path_spec.outbound_, true);
                    double    nonwalk_label = current_stop_state.calculateNonwalkLabel();
                    // if nonwalk label == MAX_COST then the only way to reach this stop is via transfer so we don't want to walk again
                    if (nonwalk_label == MAX_COST) continue;

                    deparr_time = earliest_dep_latest_arr - (access_time*dir_factor);

                    link_cost       = tallyLinkCost(supply_mode_num, path_spec, trace_file, iter_s2w->second, link_attr);
                    cost            = nonwalk_label + link_cost;

                }
                // deterministic
                else
                {
                    deparr_time = earliest_dep_latest_arr - (access_time*dir_factor);

                    // first leg has to be a trip
                    if (current_stop_state.lowestCostStopState(true).deparr_mode_ == MODE_TRANSFER) { continue; }
                    if (current_stop_state.lowestCostStopState(true).deparr_mode_ == MODE_EGRESS  ) { continue; }
                    if (current_stop_state.lowestCostStopState(true).deparr_mode_ == MODE_ACCESS  ) { continue; }
                    link_cost = access_time;
                    cost      = current_stop_state.lowestCostStopState(true).cost_ + link_cost;

                    // capacity check
                    if (path_spec.outbound_)
                    {
                        TripStop ts = { current_stop_state.lowestCostStopState(true).deparr_mode_, current_stop_state.lowestCostStopState(true).seq_, stop_id };
                        std::map<TripStop, double, struct TripStopCompare>::const_iterator bwi = bump_wait_.find(ts);
                        if (bwi != bump_wait_.end()) {
                            // time a bumped passenger started waiting
                            double latest_time = bwi->second;
                            // we can't come in time
                            if (deparr_time - Hyperlink::TIME_WINDOW_ > latest_time) { continue; }
                            // leave earlier -- to get in line 5 minutes before bump wait time
                            cost   = cost + (current_stop_state.lowestCostStopState(true).deparr_time_ - latest_time) + BUMP_BUFFER_;
                            deparr_time = latest_time - access_time - BUMP_BUFFER_;
                        }
                    }

                }

                StopState ts = {
                    deparr_time,                                                                // departure/arrival time
                    path_spec.outbound_ ? MODE_ACCESS : MODE_EGRESS,                            // departure/arrival mode
                    supply_mode_num,                                                            // trip id
                    stop_id,                                                                    // successor/predecessor
                    -1,                                                                         // sequence
                    -1,                                                                         // sequence succ/pred
                    access_time,                                                                // link time
                    link_cost,                                                                  // link cost
                    cost,                                                                       // cost
                    label_iteration,                                                            // label iteration
                    earliest_dep_latest_arr                                                     // arrival/departure time
                };
                addStopState(path_spec, trace_file, end_taz_id, ts, stop_states, label_stop_queue);

            } // end iteration through links for the given supply mode
        } // end iteration through valid supply modes
    }


    bool PathFinder::hyperpathGeneratePath(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const StopStates& stop_states,
        Path& path) const
    {
        int    start_state_id   = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;
        double dir_factor       = path_spec.outbound_ ? 1 : -1;

        const Hyperlink& taz_state = stop_states.find(start_state_id)->second;
        double taz_label        = taz_state.hyperpathCost(false);
        int    cost_cutoff      = 1;

        // setup access/egress probabilities
        std::vector<ProbabilityStopState> access_cum_prob;
        taz_state.setupProbabilities(path_spec, trace_file, *this, access_cum_prob);
        if (access_cum_prob.size() == 0) { return false; }

        // choose the state and store it
        if (path_spec.trace_) { trace_file << " -> Chose access/egress " << std::endl; }
        path.addLink(start_state_id,
                     taz_state.chooseState(path_spec, trace_file, access_cum_prob),
                     trace_file, path_spec, *this);

        // moving on, ss is now the previous link
        while (true)
        {
            const StopState& ss = path.back().second;
            int current_stop_id = ss.stop_succpred_;

            StopStates::const_iterator ssi = stop_states.find(current_stop_id);
            if (ssi == stop_states.end()) { return false; }

            if (path_spec.trace_) {
                trace_file << "current_stop=" << stop_num_to_str_.find(current_stop_id)->second;
                trace_file << (path_spec.outbound_ ? "; arrival_time=" : "; departure_time=");
                printTime(trace_file, ss.arrdep_time_);
                trace_file << "; prev_mode=";
                printMode(trace_file, ss.deparr_mode_, ss.trip_id_);
                trace_file << std::endl;
            }

            // setup probabilities
            std::vector<ProbabilityStopState> stop_cum_prob;
            const Hyperlink& current_hyperlink = ssi->second;
            current_hyperlink.setupProbabilities(path_spec, trace_file, *this, stop_cum_prob, &ss);

            if (stop_cum_prob.size() == 0) { return false; }

            // choose next link and add it to the path
            if (path_spec.trace_) { trace_file << " -> Chose stop link " << std::endl; }
            path.addLink(current_stop_id,
                         current_hyperlink.chooseState(path_spec, trace_file, stop_cum_prob, &ss),
                         trace_file, path_spec, *this);

            // are we done?
            if (( path_spec.outbound_ && path.back().second.deparr_mode_ == MODE_EGRESS) ||
                (!path_spec.outbound_ && path.back().second.deparr_mode_ == MODE_ACCESS)) {
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

    // Return success
    bool PathFinder::getFoundPath(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const StopStates& stop_states,
        Path& path,
        PathInfo& path_info) const
    {
        int end_taz_id = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;

        // no taz states -> no path found
        StopStates::const_iterator ssi_iter = stop_states.find(end_taz_id);
        if (ssi_iter == stop_states.end()) { return false; }

        const Hyperlink& taz_state = ssi_iter->second;
        if (taz_state.size() == 0) { return false; }

        if (path_spec.hyperpath_)
        {
            // find a bunch!
            PathSet paths, paths_updated_cost;
            // random seed
            srand(path_spec.path_id_);
            // find a *set of Paths*
            for (int attempts = 1; attempts <= STOCH_PATHSET_SIZE_; ++attempts)
            {
                Path new_path(path_spec.outbound_, true);
                bool path_found = hyperpathGeneratePath(path_spec, trace_file, stop_states, new_path);

                if (path_found) {
                    if (path_spec.trace_) {
                        trace_file << "----> Found path " << attempts << " ";
                        new_path.printCompat(trace_file, path_spec, *this);
                        trace_file << std::endl;
                        new_path.print(trace_file, path_spec, *this);
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
                } else {
                    if (path_spec.trace_) {
                        trace_file << "----> No path found" << std::endl;
                    }
                }
            }
            // calculate the costs for those paths and the logsum
            double logsum = 0;
            for (PathSet::iterator paths_iter = paths.begin(); paths_iter != paths.end(); ++paths_iter)
            {
                // updated cost version
                Path     path_updated     = paths_iter->first;
                PathInfo pathinfo_updated = paths_iter->second;
                pathinfo_updated.cost_    = path_updated.calculateCost(trace_file, path_spec, *this);
                // save it into the new map
                paths_updated_cost[path_updated] = pathinfo_updated;
                if (pathinfo_updated.cost_ > 0)
                {
                    logsum += exp(-1.0*Hyperlink::STOCH_DISPERSION_*pathinfo_updated.cost_);
                }
            }
            if (logsum == 0) { return false; } // fail

            // debug -- print pet set to file
            std::ofstream pathset_file;
            std::ostringstream ss;
            ss << output_dir_ << kPathSeparator;
            ss << "ft_pathset";
            if (process_num_ > 0) {
                ss << "_worker" << std::setfill('0') << std::setw(2) <<  process_num_;
            }
            ss << ".txt";
            // append
            pathset_file.open(ss.str().c_str(), (std::ios_base::out | std::ios_base::app));

            // for integerized probability*1000000
            int cum_prob    = 0;
            int cost_cutoff = 1;
            // calculate the probabilities for those paths
            for (PathSet::iterator paths_iter = paths_updated_cost.begin(); paths_iter != paths_updated_cost.end(); ++paths_iter)
            {
                paths_iter->second.probability_ = exp(-1.0*Hyperlink::STOCH_DISPERSION_*paths_iter->second.cost_)/logsum;
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
                    paths_iter->first.printCompat(trace_file, path_spec, *this);
                    trace_file << std::endl;
                }
                // print path to pathset file
                pathset_file << path_spec.iteration_ << " ";  // Iteration
                pathset_file << path_spec.passenger_id_ << " ";         // The passenger ID
                pathset_file << path_spec.path_id_ << " ";              // The path ID - uniquely identifies a passenger+path
                pathset_file << std::setw(8) << std::fixed << std::setprecision(2) << paths_iter->second.cost_ << " ";
                pathset_file << std::setw(8) << std::fixed << std::setprecision(6) << paths_iter->second.probability_ << " ";
                paths_iter->first.printCompat(pathset_file, path_spec, *this);
                pathset_file << std::endl;
            }

            pathset_file.close();

            if (cum_prob == 0) { return false; } // fail

            // choose path
            path = choosePath(path_spec, trace_file, paths_updated_cost, cum_prob);
            path_info = paths_updated_cost[path];
        }
        else
        {
            // outbound: origin to destination
            // inbound:  destination to origin
            int final_state_type = path_spec.outbound_ ? MODE_EGRESS : MODE_ACCESS;

            path.addLink(end_taz_id, taz_state.lowestCostStopState(false), trace_file, path_spec, *this);

            while (path.back().second.deparr_mode_ != final_state_type)
            {
                const StopState& last_link = path.back().second;
                int stop_id = last_link.stop_succpred_;
                StopStates::const_iterator ssi = stop_states.find(stop_id);
                path.addLink(stop_id,
                             ssi->second.lowestCostStopState(!isTrip(last_link.deparr_mode_)),
                             trace_file,
                             path_spec, *this);

            }
            path_info.cost_ = path.calculateCost(trace_file, path_spec, *this);
        }
        if (path_spec.trace_)
        {
            trace_file << "Final path" << std::endl;
            path.print(trace_file, path_spec, *this);
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
     * If outbound, then we're searching backwards, so this returns trips that arrive at the stop in time to depart at timepoint (timepoint-TIME_WINDOW_, timepoint]
     * If inbound,  then we're searching forwards,  so this returns trips that depart at the stop time after timepoint           [timepoint, timepoint+TIME_WINDOW_)
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
            if (outbound && (it->arrive_time_ <= timepoint) && (it->arrive_time_ > timepoint-Hyperlink::TIME_WINDOW_)) {
                return_trips.push_back(*it);
            } else if (!outbound && (it->depart_time_ >= timepoint) && (it->depart_time_ < timepoint+Hyperlink::TIME_WINDOW_)) {
                return_trips.push_back(*it);
            }
        }
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
        ostr << std::right;
        ostr << std::setw( 2) << std::setfill(' ') << std::right << hours << ":"; // hours
        ostr << std::setw( 2) << std::setfill('0') << static_cast<int>(minpart)      << ":"; // minutes
        int width = 3;
        if (secpart < 10) { ostr << "0"; width = 2; }
        ostr << std::left << std::setw(width) << std::setprecision( 1) << std::fixed << std::setfill(' ') << secpart << std::right; // seconds
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
        ostr << std::right;
        ostr << std::setw( 2) << std::setfill('0') << hour                       << ":"; // hour
        ostr << std::setw( 2) << std::setfill('0') << static_cast<int>(minpart)  << ":"; // minutes
        ostr << std::setw( 2) << std::setfill('0') << static_cast<int>(secpart);
    }

    void PathFinder::printMode(std::ostream& ostr, const int& mode, const int& trip_id) const
    {
        if (mode == MODE_ACCESS) {
            ostr << std::setw(10) << std::setfill(' ') << "Access";
        } else if (mode == MODE_EGRESS) {
            ostr << std::setw(10) << std::setfill(' ') << "Egress";
        } else if (mode == MODE_TRANSFER) {
            ostr << std::setw(10) << std::setfill(' ') << "Transfer";
        } else if (mode == MODE_TRANSIT) {
            // show the supply mode
            int supply_mode_num = trip_info_.find(trip_id)->second.supply_mode_num_;
            ostr << std::setw(10) << std::setfill(' ') << mode_num_to_str_.find(supply_mode_num)->second;
        } else {
            // trip
            ostr << std::setw(10) << std::setfill(' ') << "???";
        }
    }

}
