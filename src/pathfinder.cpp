#include "pathfinder.h"

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#include <psapi.h>
#else
#include <sys/time.h>
#endif

#include <assert.h>
#include <ctime>
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

// Uncomment for debug detail for link cost.
// #define DEBUG_LINKCOST

// Debug macros. If debug is not on, the if (trace) isn't even executed.
#ifdef DEBUG_LINKCOST
#  define D_LINKCOST(x) do { if (path_spec.trace_) { x } } while (0)
#else
#  define D_LINKCOST(x) do {} while (0)
#endif

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
        int        stoch_max_stop_process_count,
        bool       transfer_fare_ignore_pf,
        bool       transfer_fare_ignore_pe,
        int        max_num_paths,
        double     min_path_probability)
    {
        BUMP_BUFFER_                    = bump_buffer;
        STOCH_PATHSET_SIZE_             = stoch_pathset_size;
        STOCH_MAX_STOP_PROCESS_COUNT_   = stoch_max_stop_process_count;
        MAX_NUM_PATHS_                  = max_num_paths;
        MIN_PATH_PROBABILITY_           = min_path_probability;

        Hyperlink::TIME_WINDOW_         = time_window;
        Hyperlink::STOCH_DISPERSION_    = stoch_dispersion;
        Hyperlink::TRANSFER_FARE_IGNORE_PATHFINDING_ = transfer_fare_ignore_pf;
        Hyperlink::TRANSFER_FARE_IGNORE_PATHENUM_    = transfer_fare_ignore_pe;
    }

    void PathFinder::readIntermediateFiles()
    {
        readTripIds();
        readStopIds();
        readRouteIds();
        readFarePeriods();
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

        std::string line, string_stop_id_num, string_stop_id, string_zone_num, string_zone_id;
        int stop_id_num, zone_num;

        stop_id_file >> string_stop_id_num >> string_stop_id >> string_zone_num >> string_zone_id;
        if (process_num_ <= 1) {
            std::cout << "Reading " << ss_stop.str() << ": ";
            std::cout << "[" << string_stop_id_num   << "] ";
            std::cout << "[" << string_stop_id       << "] ";
            std::cout << "[" << string_zone_num      << "] ";
            std::cout << "[" << string_zone_id       << "] ";
        }
        while (!stop_id_file.eof()) {
            getline(stop_id_file, line);
            std::istringstream iss(line);
            iss >> stop_id_num >> string_stop_id >> zone_num >> string_zone_id;
            stop_num_to_stop_[stop_id_num].stop_str_ = string_stop_id;
            stop_num_to_stop_[stop_id_num].zone_num_ = zone_num;  // -1 means none
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << stop_num_to_stop_.size() << " lines" << std::endl;
        }
        stop_id_file.close();
    }

    void PathFinder::readRouteIds() {
        // Routes have been renumbered by fasttrips. Read string IDs.
        // Route num -> id, fare_id
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

    void PathFinder::readFarePeriods() {
        std::ifstream fare_period_file;
        std::ostringstream ss_fare;
        ss_fare << output_dir_ << kPathSeparator << "ft_intermediate_fare.txt";
        fare_period_file.open(ss_fare.str().c_str(), std::ios_base::in);

        //          fare_id_num         fare_id         fare_class          route_id_num         origin_id_num         destination_id_num, start_time              end_time
        std::string string_fare_id_num, string_fare_id, string_fare_period, string_route_id_num, string_origin_id_num, string_dest_id_num, string_fare_start_time, string_fare_end_time;
        //          price              transfers,             transfer_duration
        std::string string_fare_price, string_fare_transfers, string_tdur;
        int fare_id_num;

        fare_period_file >> string_fare_id_num >> string_fare_id >> string_fare_period
                         >> string_route_id_num >> string_origin_id_num >> string_dest_id_num
                         >> string_fare_start_time >> string_fare_end_time >> string_fare_price >> string_fare_transfers >> string_tdur;
        if (process_num_ <= 1) {
            std::cout << "Reading " << ss_fare.str()   << ": ";
            std::cout << "[" << string_fare_id_num     << "] ";
            std::cout << "[" << string_fare_id         << "] ";
            std::cout << "[" << string_fare_period     << "] ";
            std::cout << "[" << string_route_id_num    << "] ";
            std::cout << "[" << string_origin_id_num   << "] ";
            std::cout << "[" << string_dest_id_num     << "] ";
            std::cout << "[" << string_fare_start_time << "] ";
            std::cout << "[" << string_fare_end_time   << "] ";
            std::cout << "[" << string_fare_price      << "] ";
            std::cout << "[" << string_fare_transfers  << "] ";
            std::cout << "[" << string_tdur            << "] ";
        }
        RouteStopZone rsz;
        FarePeriod fp;
        while (fare_period_file >> fare_id_num >> fp.fare_id_ >> fp.fare_period_ >> rsz.route_id_ >> rsz.origin_zone_ >> rsz.destination_zone_ >> fp.start_time_ >> fp.end_time_
                                >> fp.price_ >> fp.transfers_ >> fp.transfer_duration_)
        {
            fare_periods_.insert(std::pair<RouteStopZone,FarePeriod>(rsz,fp));
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << fare_periods_.size() << " fare periods" << std::endl;
        }
        fare_period_file.close();

        // read fare transfer rules
        std::ifstream fare_transfer_file;
        std::ostringstream ss_transfer_fare;
        ss_transfer_fare << output_dir_ << kPathSeparator << "ft_intermediate_fare_transfers.txt";
        fare_transfer_file.open(ss_transfer_fare.str().c_str(), std::ios_base::in);

        std::string string_xferfrom, string_xferto, string_xfertype, string_xferamount;
        fare_transfer_file >> string_xferfrom >> string_xferto >> string_xfertype >> string_xferamount;
        if (process_num_ <= 1) {
            std::cout << "Reading " << ss_transfer_fare.str() << ": ";
            std::cout << "[" << string_xferfrom   << "]";
            std::cout << "[" << string_xferto     << "]";
            std::cout << "[" << string_xfertype   << "]";
            std::cout << "[" << string_xferamount << "]";
        }

        FareTransfer faretransfer;
        while (fare_transfer_file >> string_xferfrom >> string_xferto >> string_xfertype >> faretransfer.amount_) {
            if (string_xfertype == "transfer_free") {
                faretransfer.type_ = TRANSFER_FREE;
            } else  if (string_xfertype == "transfer_discount") {
                faretransfer.type_ = TRANSFER_DISCOUNT;
            } else if (string_xfertype == "transfer_cost") {
                faretransfer.type_ = TRANSFER_COST;
            } else {
                std::cerr << "Don't understand trasnfer_fare_type [" << string_xfertype << "]" << std::endl;
                exit(2);
            }
            fare_transfer_rules_[ std::make_pair(string_xferfrom, string_xferto)] = faretransfer;
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << fare_transfer_rules_.size() << " fare transfer rules" << std::endl;
        }
        fare_transfer_file.close();
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

        access_egress_links_.readLinks(acceggr_file, process_num_ <= 1);
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

        std::string user_class, purpose, demand_mode_type, demand_mode, string_supply_mode_num, weight_name, string_weight_value;
        int supply_mode_num;
        double weight_value;

        weights_file >> user_class >> purpose >> demand_mode_type >> demand_mode >> string_supply_mode_num >> weight_name >> string_weight_value;
        if (process_num_ <= 1) {
            std::cout << "Reading " << ss_weights.str() << ": ";
            std::cout << "[" << user_class              << "] ";
            std::cout << "[" << purpose                 << "] ";
            std::cout << "[" << demand_mode_type        << "] ";
            std::cout << "[" << demand_mode             << "] ";
            std::cout << "[" << string_supply_mode_num  << "] ";
            std::cout << "[" << weight_name             << "] ";
            std::cout << "[" << string_weight_value     << "] ";
        }
        int weights_read = 0;
        while (weights_file >> user_class >> purpose >> demand_mode_type >> demand_mode >> supply_mode_num >> weight_name >> weight_value) {
            UserClassPurposeMode ucpm = { user_class, purpose, fasttrips::MODE_ACCESS, demand_mode };
            if      (demand_mode_type == "access"  ) { ucpm.demand_mode_type_ = MODE_ACCESS;  }
            else if (demand_mode_type == "egress"  ) { ucpm.demand_mode_type_ = MODE_EGRESS;  }
            else if (demand_mode_type == "transit" ) { ucpm.demand_mode_type_ = MODE_TRANSIT; }
            else if (demand_mode_type == "transfer") { ucpm.demand_mode_type_ = MODE_TRANSFER;}
            else {
                std::cerr << "Do not understand demand_mode_type [" << demand_mode_type << "] in " << ss_weights.str() << std::endl;
                exit(2);
            }

            weight_lookup_[ucpm][supply_mode_num][weight_name] = weight_value;
            weights_read++;
        }
        if (process_num_ <= 1) {
            std::cout << " => Read " << weights_read << " lines" << std::endl;
        }
        weights_file.close();
    }

    const NamedWeights* PathFinder::getNamedWeights(
        const std::string& user_class,
        const std::string& purpose,
        DemandModeType     demand_mode_type,
        const std::string& demand_mode,
        int                suppy_mode_num) const
    {
        UserClassPurposeMode ucpm = { user_class, purpose, demand_mode_type, demand_mode};
        WeightLookup::const_iterator iter_wl = weight_lookup_.find(ucpm);
        if (iter_wl == weight_lookup_.end()) { return NULL; }
        SupplyModeToNamedWeights::const_iterator iter_sm2nw = iter_wl->second.find(suppy_mode_num);
        if (iter_sm2nw == iter_wl->second.end()) { return NULL; }

        return &(iter_sm2nw->second);
    }

    const Attributes* PathFinder::getAccessAttributes(
        int taz_id,
        int supply_mode_num,
        int stop_id,
        double tp_time) const
    {
        return access_egress_links_.getAccessAttributes(taz_id, supply_mode_num, stop_id, tp_time);
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
            (*PathFinder::ZERO_WALK_TRANSFER_ATTRIBUTES_)["elevation_gain"  ] = 0.0;
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

    int PathFinder::getRouteIdForTripId(int trip_id_num) const
    {
        const TripInfo* ti = getTripInfo(trip_id_num);
        return ti->route_id_;
    }

    // Accessor for TripStopTime for given trip id, stop sequence
    const TripStopTime& PathFinder::getTripStopTime(int trip_id, int stop_seq) const
    {
        const TripStopTime& tst = trip_stop_times_.find(trip_id)->second[stop_seq-1];  // stop sequences start at 1
        if (tst.seq_ != stop_seq) {
            printf("getTripStopTime: this shouldn't happen!");
        }
        return tst;
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
        if (trip_stop_times_.size() == 0)
        {
            // nothing has run yet -- read intermediate files
            readIntermediateFiles();
        } else
        {
            // previous iterations have run so the network is still valid, but we need to update the stop times
            // reset these
            trip_stop_times_.clear();
            stop_trip_times_.clear();
        }

        for (int i=0; i<num_stoptimes; ++i) {
            TripStopTime stt = {
                stoptime_index[3*i],    // trip id
                stoptime_index[3*i+1],  // sequence
                stoptime_index[3*i+2],  // stop id
                stoptime_times[4*i],    // arrive time
                stoptime_times[4*i+1],  // depart time
                stoptime_times[4*i+2],  // shape_dist_traveled
                stoptime_times[4*i+3]   // overcap
            };
            // verify the sequence number makes sense: sequential, starts with 1
            assert(stt.sequence_ == trip_stop_times_[stt.trip_id_].size()+1);

            trip_stop_times_[stt.trip_id_].push_back(stt);
            stop_trip_times_[stt.stop_id_].push_back(stt);
            // if (false && (process_num <= 1) && ((i<5) || (i>num_stoptimes-5))) {
            if (stt.overcap_ > 0) {
                std::cerr << "stoptimes[" << tripStringForId(stt.trip_id_) << "," << stt.seq_ << "," << stopStringForId(stt.stop_id_) << "] = ";
                std::cerr << " arrtime:";
                printTime(std::cerr, stt.arrive_time_);
                std::cerr << ", depptime:";
                printTime(std::cerr, stt.depart_time_);
                std::cerr << ", overcap:" << stt.overcap_ << std::endl;
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

    void PathFinder::reset()
    {
        weight_lookup_.clear();
        access_egress_links_.clear();

        transfer_links_o_d_.clear();
        transfer_links_d_o_.clear();
        
        trip_info_.clear();
        trip_stop_times_.clear();
        stop_trip_times_.clear();
        route_fares_.clear();
        fare_periods_.clear();
        fare_transfer_rules_.clear();

        trip_num_to_str_.clear();
        stop_num_to_stop_.clear();
        route_num_to_str_.clear();
        mode_num_to_str_.clear();
        
        bump_wait_.clear();
    }

    /// This doesn't really do anything because the instance variables are all STL structures
    /// which take care of freeing memory.
    PathFinder::~PathFinder()
    {
        // std::cout << "PathFinder destructor" << std::endl;
    }

    int PathFinder::findPathSet(
        PathSpecification path_spec,
        PathSet           &pathset,
        PerformanceInfo   &performance_info) const
    {
        // for now we'll just trace
        // if (!path_spec.trace_) { return; }

        if (path_spec.user_class_ == "crash") {
            std::cerr << "Crashing to test" << std::endl;
            exit(2);
        }

        std::ofstream trace_file;
        if (path_spec.trace_) {
            std::ostringstream ss;
            ss << output_dir_ << kPathSeparator;
            ss << "fasttrips_trace_" << path_spec.person_id_ << "-" << path_spec.person_trip_id_ << ".log";
            // append because this will happen across iterations
            std::ios_base::openmode omode = std::ios_base::out;
            if (path_spec.iteration_ > 1 || path_spec.pathfinding_iteration_ > 1) {
                omode = omode | std::ios_base::app; // append
            }
            trace_file.open(ss.str().c_str(), omode);
            trace_file << "Tracing assignment of person " << path_spec.person_id_ << " with person_trip_id" << path_spec.person_trip_id_ << std::endl;
            trace_file << "iteration_       = " << path_spec.iteration_ << std::endl;
            trace_file << "pathfinding_iter = " << path_spec.pathfinding_iteration_ << std::endl;
            trace_file << "outbound_        = " << path_spec.outbound_  << std::endl;
            trace_file << "hyperpath_       = " << path_spec.hyperpath_ << std::endl;
            trace_file << "preferred_time_  = ";
            printTime(trace_file, path_spec.preferred_time_);
            trace_file << " (" << path_spec.preferred_time_ << ")" << std::endl;
            trace_file << "value_of_time_   = " << path_spec.value_of_time_<< std::endl;
            trace_file << "user_class_      = " << path_spec.user_class_   << std::endl;
            trace_file << "purpose_         = " << path_spec.purpose_      << std::endl;
            trace_file << "access_mode_     = " << path_spec.access_mode_  << std::endl;
            trace_file << "transit_mode_    = " << path_spec.transit_mode_ << std::endl;
            trace_file << "egress_mode_     = " << path_spec.egress_mode_  << std::endl;
            trace_file << "orig_taz_id_     = " << stopStringForId(path_spec.origin_taz_id_     ) << std::endl;
            trace_file << "dest_taz_id_     = " << stopStringForId(path_spec.destination_taz_id_)<< std::endl;

            std::ostringstream ss2;
            ss2 << output_dir_ << kPathSeparator;
            ss2 << "fasttrips_labels_ids_" << path_spec.person_id_ << "-" << path_spec.person_trip_id_ << ".csv";
            stopids_file.open(ss2.str().c_str(), omode);
            stopids_file << "stop_id,stop_id_label_iter,is_trip,label_stop_cost" << std::endl;
        }

        StopStates           stop_states;
        LabelStopQueue       label_stop_queue;

#ifdef _WIN32
        // QueryPerformanceFrequency reference: https://msdn.microsoft.com/en-us/library/windows/desktop/dn553408(v=vs.85).aspx
        LARGE_INTEGER        frequency;
        LARGE_INTEGER        labeling_start_time, labeling_end_time, pathfind_end_time;
        LARGE_INTEGER        label_elapsed, pathfind_elapsed;
        QueryPerformanceFrequency(&frequency);
        QueryPerformanceCounter(&labeling_start_time);
#else
        // using gettimeofday() since std::chrono is only c++11
        struct timeval       labeling_start_time, labeling_end_time, pathfind_end_time;
        gettimeofday(&labeling_start_time, NULL);
#endif

        int pf_returnstatus = -1;
        bool success = initializeStopStates(path_spec, trace_file, stop_states, label_stop_queue);
        if (!success) {
            pf_returnstatus = PathFinder::RET_FAIL_INIT_STOP_STATES;
            if (path_spec.trace_) {
                trace_file << "initializeStopStates() failed.  Skipping labeling." << std::endl;
            }
        }

        // These are the stops that are reachable from the final TAZ
        std::map<int, int> reachable_final_stops;
        if (success) {
            success = setReachableFinalStops(path_spec, trace_file, reachable_final_stops);
            if (!success) {
                pf_returnstatus = PathFinder::RET_FAIL_SET_REACHABLE;
                if (path_spec.trace_) {
                    trace_file << "setReachableFinalStops() failed.  Skipping labeling." << std::endl;
                }
            }
        }

        // don't go further if we failed an earlier step
        if (!success) {
            stop_states.clear();

            if (path_spec.trace_) {
                trace_file.close();
                label_file.close();
                stopids_file.close();
            }
            return pf_returnstatus;
        }

        performance_info.label_iterations_ = labelStops(path_spec, trace_file, reachable_final_stops,
                                                        stop_states, label_stop_queue, performance_info.max_process_count_);
        performance_info.num_labeled_stops_ = stop_states.size();

#ifdef _WIN32
        QueryPerformanceCounter(&labeling_end_time);
#else
        gettimeofday(&labeling_end_time, NULL);
#endif

        pf_returnstatus = getPathSet(path_spec, trace_file, stop_states, pathset);

#ifdef _WIN32
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

        PROCESS_MEMORY_COUNTERS_EX pmc;
        if ( GetProcessMemoryInfo(GetCurrentProcess(), (PROCESS_MEMORY_COUNTERS*)&pmc, sizeof(pmc)) )
        {
            performance_info.workingset_bytes_   = pmc.WorkingSetSize;
            performance_info.privateusage_bytes_ = pmc.PrivateUsage;
            performance_info.mem_timestamp_      = (long)time((time_t*)0);
        }
#else
        gettimeofday(&pathfind_end_time, NULL);

        // microseconds
        long int diff = (labeling_end_time.tv_usec   + 1000000*labeling_end_time.tv_sec) -
                        (labeling_start_time.tv_usec + 1000000*labeling_start_time.tv_sec);
        performance_info.milliseconds_labeling_ = 0.001*diff;

        diff = (pathfind_end_time.tv_usec   + 1000000*pathfind_end_time.tv_sec) -
               (labeling_end_time.tv_usec   + 1000000*labeling_end_time.tv_sec);
        performance_info.milliseconds_enumerating_ = 0.001*diff;
#endif

        // clear stop states since they have path pointers
        stop_states.clear();

        if (path_spec.trace_) {

            trace_file << "        label iterations: " << performance_info.label_iterations_    << std::endl;
            trace_file << "       max process count: " << performance_info.max_process_count_   << std::endl;
            trace_file << "   milliseconds labeling: " << performance_info.milliseconds_labeling_    << std::endl;
            trace_file << "milliseconds enumerating: " << performance_info.milliseconds_enumerating_ << std::endl;
            trace_file.close();
            label_file.close();
            stopids_file.close();
        }
        return pf_returnstatus;
    }

    double PathFinder::tallyLinkCost(
        const int supply_mode_num,
        const PathSpecification& path_spec,
        std::ostream& trace_file,
        const NamedWeights& weights,
        const Attributes& attributes,
        bool hush) const
    {
        // iterate through the weights
        double cost = 0;
        D_LINKCOST(
            trace_file << "Link cost for " << std::setw(15) << std::setfill(' ') << std::left << modeStringForNum(supply_mode_num);
            trace_file << std::setw(15) << std::setfill(' ') << std::right << "weight" << " x attribute" <<std::endl;
        );

        NamedWeights::const_iterator iter_weights;
        for (iter_weights  = weights.begin();
             iter_weights != weights.end(); ++iter_weights) {

            // look for the attribute
            Attributes::const_iterator iter_attr = attributes.find(iter_weights->first);
            if (iter_attr == attributes.end()) {
                // error out??
                if (path_spec.trace_) {
                    trace_file << " => NO ATTRIBUTE CALLED " << iter_weights->first << " for " << modeStringForNum(supply_mode_num) << std::endl;
                }
                std::cerr << " => NO ATTRIBUTE CALLED " << iter_weights->first << " for " << modeStringForNum(supply_mode_num) << std::endl;
                continue;
            }

            cost += iter_weights->second * iter_attr->second;
            D_LINKCOST(
                trace_file << std::setw(26) << std::setfill(' ') << std::right << iter_weights->first << ":  + ";
                trace_file << std::setw(13) << std::setprecision(4) << std::fixed << iter_weights->second;
                trace_file << " x " << iter_attr->second << std::endl;
            );
        }
        // fare
        static const std::string fare_str("fare");
        Attributes::const_iterator fare_attr = attributes.find(fare_str);
        // fare is first converted to minutes using vot and then into utils using IVT weight
        static const std::string ivt_str("in_vehicle_time_min");
        NamedWeights::const_iterator ivt_weight = weights.find(ivt_str);
        if ((fare_attr != attributes.end()) && (ivt_weight != weights.end())) {
            //       (60 min/hour)*(hours/vot currency)*(ivt_weight) x (currency)
            cost += (60.0/path_spec.value_of_time_) * ivt_weight->second * fare_attr->second;

            D_LINKCOST(
                trace_file << std::setw(26) << std::setfill(' ') << std::right << "fare" << ":  + ";
                trace_file << std::setw(13) << std::setprecision(4) << std::fixed << (ivt_weight->second*60.0/path_spec.value_of_time_);
                trace_file << " x " << fare_attr->second << std::endl;
            );
        }
        D_LINKCOST(
            trace_file << std::setw(26) << std::setfill(' ') << "final cost" << ":  = ";
            trace_file << std::setw(13) << std::setprecision(4) << std::fixed << cost << std::endl;
        );
        return cost;
    }

    void PathFinder::addStopState(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const int stop_id,
        const StopState& ss,
        const Hyperlink* prev_link,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue) const
    {
        // do we even want to incorporate this link to our stop state?
        bool rejected = false;

        // initialize the hyperlink if we need to
        if (stop_states.find(stop_id) == stop_states.end()) {
            Hyperlink h(stop_id, path_spec.outbound_);
            stop_states[stop_id] = h;
        }

        Hyperlink& hyperlink = stop_states[stop_id];

        // keep track if the state changed (label or time window)
        // if so, we'll want to trigger dealing with the effects by adding it to the queue
        bool update_state = hyperlink.addLink(ss, prev_link, rejected, trace_file, path_spec, *this);

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
            ss << "fasttrips_labels_" << path_spec.person_id_ << "-" << path_spec.person_trip_id_ << ".csv";
            label_file.open(ss.str().c_str(), ((path_spec.iteration_ == 1) && (path_spec.pathfinding_iteration_ == 1))? std::ios_base::out : std::ios_base::out | std::ios_base::app);
            label_file << "label_iteration,link,node ID,time,mode,trip_id,link_time,link_cost,cost,AB" << std::endl;
        }

        // write the labels out to the label csv
        for (int o_d = 0; o_d < 2; ++o_d) {
            // print it into the labels file
            label_file << ss.iteration_ << ",";
            label_file << link_num      << ",";

            if (o_d == 0) { label_file << stopStringForId(stop_id) << ","; }
            else          { label_file << stopStringForId(ss.stop_succpred_) << ","; }

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
        if (access_egress_links_.hasLinksForTaz(start_taz_id) == false) {
            return false;
        }

        // Are there any supply modes for this demand mode?
        UserClassPurposeMode ucpm = {
            path_spec.user_class_,
            path_spec.purpose_,
            path_spec.outbound_ ? MODE_EGRESS: MODE_ACCESS,
            path_spec.outbound_ ? path_spec.egress_mode_ : path_spec.access_mode_
        };
        WeightLookup::const_iterator iter_weights = weight_lookup_.find(ucpm);
        if (iter_weights == weight_lookup_.end()) {
            std::cerr << "Couldn't find any weights configured for user class/purpose (1) [" << path_spec.user_class_ << "/" << path_spec.purpose_ << "], ";
            std::cerr << (path_spec.outbound_ ? "egress mode [" : "access mode [");
            std::cerr << (path_spec.outbound_ ? path_spec.egress_mode_ : path_spec.access_mode_) << "] for person " << path_spec.person_id_ << " trip " << path_spec.person_trip_id_ << std::endl;
            return false;
        }

        if (path_spec.trace_) {
            // stop_id,stop_id_label_iter,is_trip,label_stop_cost
            stopids_file << stopStringForId(start_taz_id) << ",0,0,0" << std::endl;
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

            for (AccessEgressLinkAttr::const_iterator iter_aelk  = access_egress_links_.lower_bound(start_taz_id, supply_mode_num);
                                                      iter_aelk != access_egress_links_.upper_bound(start_taz_id, supply_mode_num); ++iter_aelk)
            {

                const AccessEgressLinkKey& aelk = iter_aelk->first;

                // require preferrd_time_ in [start_time_, end_time)
                if (aelk.start_time_ >  path_spec.preferred_time_) continue;
                if (aelk.end_time_   <= path_spec.preferred_time_) continue;

                int stop_id = aelk.stop_id_;
                Attributes link_attr = iter_aelk->second;
                double attr_time = link_attr.find("time_min")->second;
                double attr_dist = link_attr.find("dist")->second;

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

                StopState ss(
                    deparr_time,                                                                // departure/arrival time
                    path_spec.outbound_ ? MODE_EGRESS : MODE_ACCESS,                            // departure/arrival mode
                    supply_mode_num,                                                            // trip id
                    start_taz_id,                                                               // successor/predecessor
                    -1,                                                                         // sequence
                    -1,                                                                         // sequence succ/pred
                    attr_time,                                                                  // link time
                    0.0,                                                                        // link fare
                    cost,                                                                       // link cost
                    attr_dist,                                                                  // link distance
                    cost,                                                                       // cost
                    0,                                                                          // iteration
                    path_spec.preferred_time_,                                                  // arrival/departure time
					0.0                                                                         // link ivt weight
                );
                addStopState(path_spec, trace_file, stop_id, ss, NULL, stop_states, label_stop_queue);

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
        double current_deparr_time     = current_stop_state.latestDepartureEarliestArrival(true);
        double nonwalk_label           = current_stop_state.hyperpathCost(true);

        // Lookup transfer weights
        // TODO: returning here is probably terrible and we shouldn't be silent... We should have zero weights if we don't want to penalize.
        const NamedWeights* transfer_weights = getNamedWeights(path_spec.user_class_, path_spec.purpose_, MODE_TRANSFER, "transfer", transfer_supply_mode_);
        if (transfer_weights == NULL) { return; }

        // add zero-walk transfer to this stop
        int               xfer_stop_id  = current_label_stop.stop_id_;
        const Attributes* zerowalk_xfer = getTransferAttributes(xfer_stop_id, xfer_stop_id);
        double            transfer_time = zerowalk_xfer->find("walk_time_min")->second;  // todo: make this a different time?
        double            deparr_time   = current_deparr_time - (transfer_time*dir_factor);
        double            link_cost, cost, transfer_dist;
        if (path_spec.hyperpath_)
        {
            link_cost = tallyLinkCost(transfer_supply_mode_, path_spec, trace_file, *transfer_weights, *zerowalk_xfer);
            cost      = nonwalk_label + link_cost;
        } else {
            link_cost = transfer_time;
            cost      = current_label_stop.label_ + link_cost;
        }
        // addStopState will handle logic of updating total cost
        StopState ss(
            deparr_time,                    // departure/arrival time
            MODE_TRANSFER,                  // departure/arrival mode
            1 ,                             // trip id
            current_label_stop.stop_id_,    // successor/predecessor
            -1,                             // sequence
            -1,                             // sequence succ/pred
            transfer_time,                  // link time
            0.0,                            // link fare
            link_cost,                      // link cost
            0.0,                            // link distance
            cost,                           // cost
            label_iteration,                // label iteration
            current_deparr_time,            // arrival/departure time
			0.0                             // link ivt weight
        );
        addStopState(path_spec, trace_file, xfer_stop_id, ss, &current_stop_state, stop_states, label_stop_queue);

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
            transfer_dist   = transfer_it->second.find("dist")->second;
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
                    int current_trip = current_stop_state.lowestCostStopState(true).trip_id_;
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
            StopState ss(
                deparr_time,                    // departure/arrival time
                MODE_TRANSFER,                  // departure/arrival mode
                1 ,                             // trip id
                current_label_stop.stop_id_,    // successor/predecessor
                -1,                             // sequence
                -1,                             // sequence succ/pred
                transfer_time,                  // link time
                0.0,                            // link fare
                link_cost,                      // link cost
                transfer_dist,                  // link distance
                cost,                           // cost
                label_iteration,                // label iteration
                current_deparr_time,            // arrival/departure time
				0.0                             // link ivt weight
            );
            addStopState(path_spec, trace_file, xfer_stop_id, ss, &current_stop_state, stop_states, label_stop_queue);
        }
    }

    /**
     * Part of the labeling loop. Assuming the *current_label_stop* was just pulled off the
     * *label_stop_queue*, this method will iterate through access links to (for outbound) or
     * egress links from (for inbound) the current stop and update the next stop given the current stop state.
     */
    void PathFinder::updateStopStatesForFinalLinks(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const std::map<int, int>& reachable_final_stops,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue,
        int label_iteration,
        const LabelStop& current_label_stop,
        double& est_max_path_cost) const
    {
        // shortcut -- nothing to do if this isn't reachable to end taz
        if (reachable_final_stops.count(current_label_stop.stop_id_) == 0) {
            return;
        }

        // current_stop_state is a hyperlink
        // It should have trip-states in it, because otherwise it wouldn't have come up in the label stop queue to process
        Hyperlink& current_stop_state  = stop_states[current_label_stop.stop_id_];
        double current_deparr_time     = current_stop_state.latestDepartureEarliestArrival(true);
        double nonwalk_label           = current_stop_state.hyperpathCost(true);

        int    end_taz_id = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

        double earliest_dep_latest_arr = PathFinder::MAX_DATETIME;
        if (path_spec.hyperpath_) {
            earliest_dep_latest_arr = current_stop_state.earliestDepartureLatestArrival(path_spec.outbound_, true);
        } else {
            earliest_dep_latest_arr = current_stop_state.lowestCostStopState(true).deparr_time_;
        }
        double earliest_dep_latest_arr_024 = fix_time_range(earliest_dep_latest_arr);


        // are there any egress/access links?
        if (access_egress_links_.hasLinksForTaz(end_taz_id) == false) {
            // this shouldn't happen because of the shortcut
            return;
        }

        // Are there any supply modes for this demand mode?
        UserClassPurposeMode ucpm = {
            path_spec.user_class_,
            path_spec.purpose_,
            path_spec.outbound_ ? MODE_ACCESS: MODE_EGRESS,
            path_spec.outbound_ ? path_spec.access_mode_ : path_spec.egress_mode_
        };
        WeightLookup::const_iterator iter_weights = weight_lookup_.find(ucpm);
        if (iter_weights == weight_lookup_.end()) {
            // this shouldn't happen because of the shortcut
            std::cerr << "Couldn't find any weights configured for user class/purpose (2) [" << path_spec.user_class_ << "/" << path_spec.purpose_ << "], ";
            std::cerr << (path_spec.outbound_ ? "access mode [" : "egress mode [");
            std::cerr << (path_spec.outbound_ ? path_spec.access_mode_ : path_spec.egress_mode_) << "] for person " << path_spec.person_id_ << " trip " << path_spec.person_trip_id_ << std::endl;
            return;
        }

        // Iterate through valid supply modes
        SupplyModeToNamedWeights::const_iterator iter_s2w;
        for (iter_s2w  = iter_weights->second.begin();
             iter_s2w != iter_weights->second.end(); ++iter_s2w) {
            int supply_mode_num = iter_s2w->first;

            for (AccessEgressLinkAttr::const_iterator iter_aelk  = access_egress_links_.lower_bound(end_taz_id, supply_mode_num, current_label_stop.stop_id_);
                                                      iter_aelk != access_egress_links_.upper_bound(end_taz_id, supply_mode_num, current_label_stop.stop_id_); ++iter_aelk)
            {

                const AccessEgressLinkKey& aelk = iter_aelk->first;

                // require earliest_dep_latest_arr in [start_time_, end_time)
                if (aelk.start_time_ >  earliest_dep_latest_arr_024) continue;
                if (aelk.end_time_   <= earliest_dep_latest_arr_024) continue;

                Attributes link_attr            = iter_aelk->second;
                link_attr["preferred_delay_min"]= 0.0;

                double  access_time             = link_attr.find("time_min")->second;
                double  access_dist             = link_attr.find("dist")->second;
                double  deparr_time, link_cost, cost;

                if (path_spec.hyperpath_)
                {
                    deparr_time     = earliest_dep_latest_arr - (access_time*dir_factor);

                    link_cost       = tallyLinkCost(supply_mode_num, path_spec, trace_file, iter_s2w->second, link_attr);
                    cost            = nonwalk_label + link_cost;

                }
                // deterministic
                else
                {
                    deparr_time = earliest_dep_latest_arr - (access_time*dir_factor);
                    link_cost   = access_time;
                    cost        = current_stop_state.lowestCostStopState(true).cost_ + link_cost;

                    // capacity check
                    if (path_spec.outbound_)
                    {
                        TripStop ts = { current_stop_state.lowestCostStopState(true).deparr_mode_, current_stop_state.lowestCostStopState(true).seq_, current_label_stop.stop_id_ };
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

                StopState ts(
                    deparr_time,                                                                // departure/arrival time
                    path_spec.outbound_ ? MODE_ACCESS : MODE_EGRESS,                            // departure/arrival mode
                    supply_mode_num,                                                            // trip id
                    current_label_stop.stop_id_,                                                // successor/predecessor
                    -1,                                                                         // sequence
                    -1,                                                                         // sequence succ/pred
                    access_time,                                                                // link time
                    0.0,                                                                        // link fare
                    link_cost,                                                                  // link cost
                    access_dist,                                                                // link distance
                    cost,                                                                       // cost
                    label_iteration,                                                            // label iteration
                    earliest_dep_latest_arr,                                                    // arrival/departure time
					0.0                                                                         // link ivt weight
                );
                addStopState(path_spec, trace_file, end_taz_id, ts, &current_stop_state, stop_states, label_stop_queue);

                // set label_cutoff
                double low_cost = stop_states[end_taz_id].hyperpathCost(false);
                // estimate of the max path cost that would have probability > MIN_PATH_PROBABILITY
                double max_cost = low_cost - (log(MIN_PATH_PROBABILITY_) - log(1.0-MIN_PATH_PROBABILITY_))/Hyperlink::STOCH_DISPERSION_;
                est_max_path_cost = std::min(est_max_path_cost, max_cost);

            } // end iteration through links for the given supply mode
        } // end iteration through valid supply modes
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
        UserClassPurposeMode ucpm = { path_spec.user_class_, path_spec.purpose_, MODE_TRANSIT, path_spec.transit_mode_};
        WeightLookup::const_iterator iter_weights = weight_lookup_.find(ucpm);
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
            // the trip stop time for this trip
            const TripStopTime& tst = getTripStopTime(it->trip_id_, it->seq_);

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
                double  link_dist = dir_factor*(it->shape_dist_trav_ - possible_board_alight.shape_dist_trav_);
                double  fare      = 0; // only calculate for hyperpath
                double  ivtwt     = 0; // only calculate for hyperpath
                const FarePeriod* fp = 0;

                if (in_vehicle_time < 0) {
                    printf("in_vehicle_time < 0 -- this shouldn't happen\n");
                    if (path_spec.trace_) { trace_file << "in_vehicle_time < 0 -- this shouldn't happen!" << std::endl; }
                }

                // stochastic/hyperpath: cost update
                if (path_spec.hyperpath_) {

                    double overcap     = path_spec.outbound_ ? possible_board_alight.overcap_ : tst.overcap_;
                    double at_capacity = (overcap >= 0 ? 1.0 : 0.0);  // binary, 0 means at capacity
                    if (overcap < 0) { overcap = 0; } // make it non-negative
                    fp = getFarePeriod(trip_info.route_id_,
                                       path_spec.outbound_ ? possible_board_alight.stop_id_ : current_label_stop.stop_id_,
                                       path_spec.outbound_ ? current_label_stop.stop_id_ : possible_board_alight.stop_id_,
                                       path_spec.outbound_ ? deparr_time : arrdep_time);
                    // this is where it gets painful... if we have a fareperiod, try to check transfer fare rules and guess the right fare
                    if (fp) { fare     = current_stop_state.getFareWithTransfer(path_spec, trace_file, *this, *fp, stop_states); }

                    if (false && path_spec.trace_) {
                        if (path_spec.outbound_) {
                            trace_file << "trip " << tripStringForId(possible_board_alight.trip_id_)
                                       << ", stop " << stopStringForId(possible_board_alight.stop_id_)
                                       << ", seq " << possible_board_alight.seq_
                                       << ", overcap " << possible_board_alight.overcap_
                                       << ", route_id " << trip_info.route_id_
                                       << ", fare " << fare
                                       << std::endl;
                        }
                        else {
                            trace_file << "trip " << tripStringForId(it->trip_id_)
                                       << ", stop " << stopStringForId(it->stop_id_)
                                       << ", seq " << it->seq_
                                       << ", overcap " << tst.overcap_
                                       << ", route_id " << trip_info.route_id_
                                       << ", fare " << fare
                                       << std::endl;
                        }
                    }

                    //update link ivtwt so that it is available when fares/fare-utils calculations are updated
                    static const std::string ivt_str("in_vehicle_time_min");
                    NamedWeights::const_iterator ivt_weight = named_weights.find(ivt_str);
                    if (ivt_weight != named_weights.end()) ivtwt = ivt_weight->second;

                    // start with trip info attributes
                    Attributes link_attr = trip_info.trip_attr_;
                    link_attr["in_vehicle_time_min"] = in_vehicle_time;
                    link_attr["wait_time_min"      ] = wait_time;
                    link_attr["overcap"            ] = overcap;
                    link_attr["at_capacity"        ] = at_capacity;
                    link_attr["fare"               ] = fare;

                    link_cost = 0;
                    // If outbound, and the current link is egress, then it's as late as possible and the wait time isn't accurate.
                    // It should be a preferred delay time instead
                    // ditto for inbound and access
                    if (( path_spec.outbound_ && best_guess_link.deparr_mode_ == MODE_EGRESS) ||
                        (!path_spec.outbound_ && best_guess_link.deparr_mode_ == MODE_ACCESS)) {
                        link_attr["wait_time_min"      ] = 0;


                        // TODO: this is awkward... setting this all up again.  Plus we don't have all the attributes set.  Cache something?
                        Attributes delay_attr;
                        delay_attr["time_min"             ] = 0;
                        delay_attr["drive_time_min"       ] = 0;
                        delay_attr["walk_time_min"        ] = 0;
                        delay_attr["elevation_gain"       ] = 0;
                        delay_attr["preferred_delay_min"  ] = wait_time;
                        UserClassPurposeMode delay_ucpm = {
                            path_spec.user_class_, path_spec.purpose_,
                            path_spec.outbound_ ? MODE_EGRESS: MODE_ACCESS,
                            path_spec.outbound_ ? path_spec.egress_mode_ : path_spec.access_mode_
                        };
                        WeightLookup::const_iterator delay_iter_weights = weight_lookup_.find(delay_ucpm);
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

                StopState ss(
                    deparr_time,                    // departure/arrival time
                    MODE_TRANSIT,                   // departure/arrival mode
                    possible_board_alight.trip_id_, // trip id
                    current_label_stop.stop_id_,    // successor/predecessor
                    possible_board_alight.seq_,     // sequence
                    it->seq_,                       // sequence succ/pred
                    in_vehicle_time+wait_time,      // link time
                    fare,                           // link fare
                    link_cost,                      // link cost
                    link_dist,                      // link distance
                    cost,                           // cost
                    label_iteration,                // label iteration
                    arrdep_time,                    // arrival/departure time
					ivtwt,                          // link ivt weight
                    fp                              // fare period
                );
                addStopState(path_spec, trace_file, board_alight_stop, ss, &current_stop_state, stop_states, label_stop_queue);

            }
            trips_done.insert(it->trip_id_);
        }
    }

    int PathFinder::labelStops(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        const std::map<int,int>& reachable_final_stops,
        StopStates& stop_states,
        LabelStopQueue& label_stop_queue,
        int& max_process_count) const
    {
        int label_iterations = 1;
        std::tr1::unordered_set<int> stop_done;
        std::tr1::unordered_set<int> trips_done;
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;
        LabelStop last_label_stop;

        // we'll use this to stop labeling when we're past useful paths
        double est_max_path_cost = MAX_COST;

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
            LabelStop current_label_stop = label_stop_queue.pop_top(stop_num_to_stop_, path_spec.trace_, trace_file);

            // if we just processed this one, then skip since it'll be a no-op
            if ((current_label_stop.stop_id_ == last_label_stop.stop_id_) && (current_label_stop.is_trip_ == last_label_stop.is_trip_)) { continue; }

            // hyperpath only
            if (path_spec.hyperpath_) {
                // have we hit the configured limit?
                if ((STOCH_MAX_STOP_PROCESS_COUNT_ > 0) &&
                    (stop_states[current_label_stop.stop_id_].processCount(current_label_stop.is_trip_) == STOCH_MAX_STOP_PROCESS_COUNT_)) {
                    if (path_spec.trace_) {
                        trace_file << "Pulling from label_stop_queue but stop " << stopStringForId(current_label_stop.stop_id_);
                        trace_file << " is_trip " << current_label_stop.is_trip_;
                        trace_file << " has been processed the limit " << STOCH_MAX_STOP_PROCESS_COUNT_ << " times so skipping." << std::endl;
                    }
                    continue;
                }
                // stop is processing
                stop_states[current_label_stop.stop_id_].incrementProcessCount(current_label_stop.is_trip_);
                max_process_count = std::max(max_process_count, stop_states[current_label_stop.stop_id_].processCount(current_label_stop.is_trip_));
            }

            // current_stop_state is a hyperlink
            Hyperlink& current_stop_state = stop_states[current_label_stop.stop_id_];

            if (path_spec.trace_) {
                trace_file << "Pulling from label_stop_queue (iteration " << std::setw( 6) << std::setfill(' ') << label_iterations;
                trace_file << ", stop " << stopStringForId(current_label_stop.stop_id_);
                trace_file << ", is_trip " << current_label_stop.is_trip_;
                if (path_spec.hyperpath_) {
                    trace_file << ", label ";
                    trace_file << std::setprecision(6) << current_label_stop.label_;
                }
                trace_file << ", est_max_path_cost " << est_max_path_cost;
                trace_file << ") :======" << std::endl;
                current_stop_state.print(trace_file, path_spec, *this);
                trace_file << "==============================" << std::endl;

                // stop_id,stop_id_label_iter,is_trip,label_stop_cost
                stopids_file << stopStringForId(current_label_stop.stop_id_) << "," << label_iterations << ",";
                stopids_file << current_label_stop.is_trip_ << "," << current_label_stop.label_ << std::endl;
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

                updateStopStatesForFinalLinks(path_spec,
                                              trace_file,
                                              reachable_final_stops,
                                              stop_states,
                                              label_stop_queue,
                                              label_iterations,
                                              current_label_stop,
                                              est_max_path_cost);
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

            // Should we call it a day?
            if (current_label_stop.label_ > 2*est_max_path_cost) {
                if (path_spec.trace_) {
                    trace_file << "ENDING LABELING LOOP.  label = " << current_label_stop.label_ << " > 2*est_max_path_cost = " << est_max_path_cost << std::endl;
                }
                break;
            }
        }
        return label_iterations;
    }

    // Returns false if no stops are reachable
    bool PathFinder::setReachableFinalStops(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        std::map<int, int>& reachable_final_stops) const
    {
        int end_taz_id = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;
        double dir_factor = path_spec.outbound_ ? 1.0 : -1.0;

        // are there any egress/access links?
        if (access_egress_links_.hasLinksForTaz(end_taz_id) == false) {
            if (path_spec.trace_) { trace_file << "No links for end_taz_id" << end_taz_id << std::endl; }
            return false;
        }

        // Are there any supply modes for this demand mode?
        UserClassPurposeMode ucpm = {
            path_spec.user_class_,
            path_spec.purpose_,
            path_spec.outbound_ ? MODE_ACCESS: MODE_EGRESS,
            path_spec.outbound_ ? path_spec.access_mode_ : path_spec.egress_mode_
        };
        WeightLookup::const_iterator iter_weights = weight_lookup_.find(ucpm);
        if (iter_weights == weight_lookup_.end()) {
            std::cerr << "Couldn't find any weights configured for user class/purpose (3) [" << path_spec.user_class_ << "/" << path_spec.purpose_ << "], ";
            std::cerr << (path_spec.outbound_ ? "access mode [" : "egress mode [");
            std::cerr << (path_spec.outbound_ ? path_spec.access_mode_ : path_spec.egress_mode_) << "] for person " << path_spec.person_id_ << " trip " << path_spec.person_trip_id_ << std::endl;
            return false;
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

            for (AccessEgressLinkAttr::const_iterator iter_aelk  = access_egress_links_.lower_bound(end_taz_id, supply_mode_num);
                                                      iter_aelk != access_egress_links_.upper_bound(end_taz_id, supply_mode_num); ++iter_aelk)
            {

                // Iterate through the links for the given supply mode
                int stop_id = iter_aelk->first.stop_id_;
                if (reachable_final_stops.count(stop_id) == 0) {
                    reachable_final_stops[stop_id] = 0;
                } else {
                    reachable_final_stops[stop_id] += 1;
                }

                if (path_spec.trace_) {
                    trace_file << "Stop " << stop_id << " reachable by supply mode " << supply_mode_num << std::endl;
                }
            }
        }

        return (reachable_final_stops.size() > 0);
    }

    // This is currently not being used because it has been replaced with updateStopStatesForFinalLinks() but
    // it may come back for skimming so let's leave it in for now.
    /*
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
        if (access_egress_links_.hasLinksForTaz(end_taz_id) == false) {
            return false;
        }

        // Are there any supply modes for this demand mode?
        UserClassPurposeMode ucpm = {
            path_spec.user_class_,
            path_spec.purpose_,
            path_spec.outbound_ ? MODE_ACCESS: MODE_EGRESS,
            path_spec.outbound_ ? path_spec.access_mode_ : path_spec.egress_mode_
        };
        WeightLookup::const_iterator iter_weights = weight_lookup_.find(ucpm);
        if (iter_weights == weight_lookup_.end()) {
            std::cerr << "Couldn't find any weights configured for user class/purpose (4) [" << path_spec.user_class_ << "/" << path_spec.purpose_ << "], ";
            std::cerr << (path_spec.outbound_ ? "access mode [" : "egress mode [");
            std::cerr << (path_spec.outbound_ ? path_spec.access_mode_ : path_spec.egress_mode_) << "] for person " << path_spec.person_id_ << " trip " << path_spec.person_trip_id_ << std::endl;
            return false;
        }

        if (path_spec.trace_) {
            // stop_id,stop_id_label_iter,is_trip,label_stop_cost
            stopids_file << stopStringForId(end_taz_id) << "," << label_iteration << ",0,";
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
            for (AccessEgressLinkAttr::const_iterator iter_aelk  = access_egress_links_.lower_bound(end_taz_id, supply_mode_num);
                                                      iter_aelk != access_egress_links_.upper_bound(end_taz_id, supply_mode_num); ++iter_aelk)
            {
                int     stop_id                 = iter_aelk->first.stop_id_;
                double  earliest_dep_latest_arr = PathFinder::MAX_DATETIME;
                const AccessEgressLinkKey& aelk = iter_aelk->first;

                // require earliest_dep_latest_arr in [start_time_, end_time)
                if (aelk.start_time_ >  earliest_dep_latest_arr) continue;
                if (aelk.end_time_   <= earliest_dep_latest_arr) continue;

                Attributes link_attr            = iter_aelk->second;
                link_attr["preferred_delay_min"]= 0.0;

                double  access_time             = link_attr.find("time_min")->second;
                double  access_dist             = link_attr.find("dist")->second;
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

                StopState ts(
                    deparr_time,                                                                // departure/arrival time
                    path_spec.outbound_ ? MODE_ACCESS : MODE_EGRESS,                            // departure/arrival mode
                    supply_mode_num,                                                            // trip id
                    stop_id,                                                                    // successor/predecessor
                    -1,                                                                         // sequence
                    -1,                                                                         // sequence succ/pred
                    access_time,                                                                // link time
                    0.0,                                                                        // link fare
                    link_cost,                                                                  // link cost
                    access_dist,                                                                // link distance
                    cost,                                                                       // cost
                    label_iteration,                                                            // label iteration
                    earliest_dep_latest_arr,                                                    // arrival/departure time
					0.0                                                                         // link ivt weight
                );
                addStopState(path_spec, trace_file, end_taz_id, ts, &current_stop_state, stop_states, label_stop_queue);

            } // end iteration through links for the given supply mode
        } // end iteration through valid supply modes
    }*/


    bool PathFinder::hyperpathGeneratePath(
        const PathSpecification& path_spec,
        std::ofstream& trace_file,
        StopStates& stop_states,
        Path& path) const
    {
        int    start_state_id   = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;
        double dir_factor       = path_spec.outbound_ ? 1 : -1;

        Hyperlink& taz_state    = stop_states.find(start_state_id)->second;
        double taz_label        = taz_state.hyperpathCost(false);

        // setup access/egress probabilities
        int maxcumi = taz_state.setupProbabilities(path_spec, trace_file, *this, false);
        if (maxcumi == 0) { return false; }

        // choose the state and store it
        if (path_spec.trace_) { trace_file << " -> Chose access/egress " << std::endl; }
        path.addLink(start_state_id,
                     taz_state.chooseState(path_spec, trace_file),
                     trace_file, path_spec, *this);

        // trip_id shouldn't repeat
        int last_trip_id = -1;

        // moving on, ss is now the previous link
        while (true)
        {
            StopState& ss = path.back().second;
            int current_stop_id = ss.stop_succpred_;

            StopStates::iterator ssi = stop_states.find(current_stop_id);
            if (ssi == stop_states.end()) { return false; }

            if (path_spec.trace_) {
                trace_file << "current_stop=" << stopStringForId(current_stop_id);
                trace_file << (path_spec.outbound_ ? "; arrival_time=" : "; departure_time=");
                printTime(trace_file, ss.arrdep_time_);
                trace_file << "; prev_mode=";
                printMode(trace_file, ss.deparr_mode_, ss.trip_id_);
                trace_file << std::endl;
            }

            // setup probabilities
            Hyperlink& current_hyperlink = ssi->second;
            maxcumi = current_hyperlink.setupProbabilities(path_spec, trace_file, *this, !isTrip(ss.deparr_mode_), &path);

            if (maxcumi == 0) { return false; }

            // choose next link and add it to the path
            if (path_spec.trace_) { trace_file << " -> Chose stop link " << std::endl; }
            path.addLink(current_stop_id,
                         current_hyperlink.chooseState(path_spec, trace_file, &ss),
                         trace_file, path_spec, *this);

            // are we done?
            if (( path_spec.outbound_ && path.back().second.deparr_mode_ == MODE_EGRESS) ||
                (!path_spec.outbound_ && path.back().second.deparr_mode_ == MODE_ACCESS)) {
                break;
            }

            if (isTrip(path.back().second.deparr_mode_)) {
                last_trip_id = path.back().second.trip_id_;
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

    // Returns PathFinder::RET_SUCCESS, etc.
    int PathFinder::getPathSet(
        const PathSpecification&    path_spec,
        std::ofstream&              trace_file,
        StopStates&                 stop_states,
        PathSet&                    pathset) const
    {
        int end_taz_id = path_spec.outbound_ ? path_spec.origin_taz_id_ : path_spec.destination_taz_id_;

        // no taz states -> no path found
        StopStates::const_iterator ssi_iter = stop_states.find(end_taz_id);
        if (ssi_iter == stop_states.end()) { return RET_FAIL_END_NOT_FOUND; }

        const Hyperlink& taz_state = ssi_iter->second;
        if (taz_state.size() == 0) { return RET_FAIL_END_NOT_FOUND; }

        // experimental-- look at the low cost path?
        if (false && path_spec.trace_)
        {
            const Path* low_cost_path = taz_state.getLowCostPath(false); // ends in non-trip
            if (low_cost_path) {
                trace_file << "Low cost path: " << low_cost_path->cost() << std::endl;
                low_cost_path->print(trace_file, path_spec, *this);
                trace_file << std::endl;
            } else { trace_file <<  "Low cost path: " << "None" << std::endl; }
        }

        if (path_spec.hyperpath_)
        {
            double logsum = 0;
            // random seed -- possible todo: make this a function of more meaningful attributes, like o/d/time/outbound/userclass/purpose ?
            // srand(path_spec.path_id_);
            srand(42);
            // find a *set of Paths*
            for (int attempts = 1; attempts <= STOCH_PATHSET_SIZE_; ++attempts)
            {
                Path new_path(path_spec.outbound_, true);
                bool path_found = hyperpathGeneratePath(path_spec, trace_file, stop_states, new_path);

                if (path_found) {
                    // we have to calculate the cost in order to find it, since it's ordered by cost also
                    new_path.calculateCost(trace_file, path_spec, *this);

                    if (path_spec.trace_) {
                        trace_file << "----> Found path " << attempts << " ";
                        new_path.printCompat(trace_file, path_spec, *this);
                        trace_file << std::endl;
                        new_path.print(trace_file, path_spec, *this);
                        trace_file << std::endl;
                    }
                    // do we already have this?  if so, increment
                    PathSet::iterator paths_iter = pathset.find(new_path);
                    if (paths_iter != pathset.end()) {
                        paths_iter->second.count_ += 1;
                    } else {
                        PathInfo pi = { 1, 0, 0 };  // count is 1
                        pathset[new_path] = pi;

                        logsum += exp(-1.0*new_path.cost());
                    }
                    if (path_spec.trace_) { trace_file << "pathsset size = " << pathset.size() << " new? " << (paths_iter == pathset.end()) << std::endl; }
                } else {
                    if (path_spec.trace_) {
                        trace_file << "----> No path found" << std::endl;
                    }
                }
            }

            if (logsum == 0) { return PathFinder::RET_FAIL_NO_PATHS_GEN; } // fail

            // for integerized probability*1000000
            int cum_prob    = 0;
            const Path* real_low_cost_path = NULL;
            // calculate the probabilities for those paths

            // if we truncate, start here
            PathSet::iterator trunc_iter = pathset.end();
            int path_count = 0;

            for (PathSet::iterator paths_iter = pathset.begin(); paths_iter != pathset.end(); ++paths_iter)
            {
                paths_iter->second.probability_ = exp(-1.0*paths_iter->first.cost())/logsum;
                path_count += 1;

                // Is this under the min path probability AND we have enough paths?
                // Since they are sorted in decreasing probability, we only want to set it once
                if ((trunc_iter == pathset.end()) &&
                    (paths_iter->second.probability_ < MIN_PATH_PROBABILITY_) &&
                    (MAX_NUM_PATHS_ > 0) &&
                    (path_count > MAX_NUM_PATHS_))
                {
                    trunc_iter = paths_iter;
                }

                // why?  :p
                int prob_i = static_cast<int>(RAND_MAX*paths_iter->second.probability_);

                cum_prob += prob_i;
                paths_iter->second.prob_i_ = cum_prob;

                if (path_spec.trace_)
                {
                    trace_file << "-> probability " << std::setfill(' ') << std::setw(8) << paths_iter->second.probability_;
                    trace_file << "; prob_i " << std::setw(8) << paths_iter->second.prob_i_;
                    trace_file << "; count " << std::setw(4) << paths_iter->second.count_;
                    trace_file << "; cost " << std::setw(8) << paths_iter->first.cost();
                    // trace_file << "; cap bad? " << std::setw(2) << paths_iter->first.capacity_problem_;
                    trace_file << "   ";
                    paths_iter->first.printCompat(trace_file, path_spec, *this);
                    trace_file << std::endl;
                }

                if (real_low_cost_path == NULL) {
                    real_low_cost_path = &(paths_iter->first);
                }
                else if (paths_iter->first.cost() < real_low_cost_path->cost()) {
                    real_low_cost_path = &(paths_iter->first);
                }
            }

            if (cum_prob == 0) { return RET_FAIL_NO_PATH_PROB; } // fail

            // if we have more than the max num paths AND some are low probability, truncate
            if (trunc_iter != pathset.end()) {
                if (path_spec.trace_) {
                    trace_file << "Truncating to ";
                    trunc_iter->first.printCompat(trace_file, path_spec, *this);
                    trace_file << std::endl;
                }
                pathset.erase(trunc_iter, pathset.end());
            }

            // experimental-- verify lowest cost path is low?
            if (false && real_low_cost_path)
            {
                const Path* low_cost_path = taz_state.getLowCostPath(false); // ends in non-trip
                if (low_cost_path == NULL) {
                    std::cerr << "No low cost path found for person " << path_spec.person_id_ << " trip " << path_spec.person_trip_id_ << std::endl;
                } else if (real_low_cost_path->cost() < low_cost_path->cost()) {
                    std::cerr << "Real low cost path not found for person " << path_spec.person_id_ << " trip " << path_spec.person_trip_id_ << std::endl;
                } else {
                    std::cerr << "Real low cost path found" << std::endl;
                }
            }

            // choose path
            // path = choosePath(path_spec, trace_file, pathsset, cum_prob);
            // path_info = paths[path];
            return RET_SUCCESS;
        }
        else
        {
            // outbound: origin to destination
            // inbound:  destination to origin
            int final_state_type = path_spec.outbound_ ? MODE_EGRESS : MODE_ACCESS;

            Path path(path_spec.outbound_, true);
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
            PathInfo pi = { 1, 1, 0 };  // count is 1
            path.calculateCost(trace_file, path_spec, *this);
            pathset[path] = pi;
            if (path_spec.trace_)
            {
                trace_file << "Final path" << std::endl;
                path.print(trace_file, path_spec, *this);
            }
            return RET_SUCCESS;
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
     * Returns the fare for the transit vehicle of the given route.
     */
    const FarePeriod* PathFinder::getFarePeriod(int route_id, int board_stop_id, int alight_stop_id, double trip_depart_time) const
    {
        int board_stop_zone  = stop_num_to_stop_.find(board_stop_id)->second.zone_num_;
        int alight_stop_zone = stop_num_to_stop_.find(alight_stop_id)->second.zone_num_;
        RouteStopZone rsz;

        for (int search_type = 0; search_type < 4; ++search_type) {
            // search for route + origin zone, dest zone
            if (search_type == 0) {
                if (board_stop_zone  < 0) { continue; }
                if (alight_stop_zone < 0) { continue; }
                rsz.route_id_         = route_id;
                rsz.origin_zone_      = board_stop_zone;
                rsz.destination_zone_ = alight_stop_zone;
            } else if (search_type == 1) {
                // search for route only
                rsz.route_id_         = route_id;
                rsz.origin_zone_      = -1;
                rsz.destination_zone_ = -1;
            } else if (search_type == 2) {
                if (board_stop_zone  < 0) { continue; }
                if (alight_stop_zone < 0) { continue; }
                // search for origin zone, dest zone
                rsz.route_id_         = -1;
                rsz.origin_zone_      = board_stop_zone;
                rsz.destination_zone_ = alight_stop_zone;
            } else if (search_type == 3) {
                // search for general fare
                rsz.route_id_         = -1;
                rsz.origin_zone_      = -1;
                rsz.destination_zone_ = -1;
            }

            // find the right fare period
            std::pair<FarePeriodMmap::const_iterator, FarePeriodMmap::const_iterator> iter_range = fare_periods_.equal_range(rsz);
            FarePeriodMmap::const_iterator fp_iter = iter_range.first;
            while (fp_iter != iter_range.second) {
                if ((trip_depart_time >= fp_iter->second.start_time_) && (trip_depart_time < fp_iter->second.end_time_)) {
                    return &(fp_iter->second);
                }
                ++fp_iter;
            }
        }
        return (const FarePeriod*)0;
    }

    /**
     * Returns the fare transfer given two fare periods.
     */
    const FareTransfer* PathFinder::getFareTransfer(const std::string from_fare_period, const std::string to_fare_period) const
    {
        FareTransferMap::const_iterator ftm_iter = fare_transfer_rules_.find( std::make_pair(from_fare_period, to_fare_period) );
        if (ftm_iter != fare_transfer_rules_.end()) {
            return &(ftm_iter->second);
        }
        return (const FareTransfer*)0;
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
        if (secpart < 9.95) { ostr << "0"; width = 2; }
        ostr << std::left << std::setw(width) << std::setprecision( 1) << std::fixed << std::setfill(' ') << secpart << std::right; // seconds
    }

    /*
     * Assuming that timemin is a time in minutes after midnight, prints a formatted version.
     */
    void PathFinder::printTime(std::ostream& ostr, const double& timemin) const
    {
        double minpart, secpart;
        double time_min_024 = timemin;
        char   cross_day    = ' ';

        // this version is in [0,1440)
        if (time_min_024 < 0) {
            time_min_024 += 1440.0;
            cross_day    = '-';
        }
        if (time_min_024 >= 1440.0) {
            time_min_024 -= 1440.0;
            cross_day     = '+';
        }
        int    hour = static_cast<int>(time_min_024/60.0);

        secpart = modf(time_min_024, &minpart); // split into minutes and seconds
        minpart = minpart - hour*60.0;
        secpart = secpart*60.0;
        ostr << std::right;
        ostr << std::setw( 1) << cross_day;
        ostr << std::setw( 2) << std::setfill('0') << hour                       << ":"; // hour
        ostr << std::setw( 2) << std::setfill('0') << static_cast<int>(minpart)  << ":"; // minutes
        ostr << std::setw( 2) << std::setfill('0') << static_cast<int>(secpart);
    }

    void PathFinder::printMode(std::ostream& ostr, const int& mode, const int& trip_id) const
    {
        if (mode == MODE_ACCESS) {
            ostr << std::setw(13) << std::setfill(' ') << "Access";
        } else if (mode == MODE_EGRESS) {
            ostr << std::setw(13) << std::setfill(' ') << "Egress";
        } else if (mode == MODE_TRANSFER) {
            ostr << std::setw(13) << std::setfill(' ') << "Transfer";
        } else if (mode == MODE_TRANSIT) {
            // show the supply mode
            int supply_mode_num = trip_info_.find(trip_id)->second.supply_mode_num_;
            ostr << std::setw(13) << std::setfill(' ') << mode_num_to_str_.find(supply_mode_num)->second;
        } else {
            // trip
            ostr << std::setw(13) << std::setfill(' ') << "???";
        }
    }

}
