/**
 * \file path.cpp
 *
 * Path implementation
 **/

#include <cmath> // fmod
#include "path.h"
#include "pathfinder.h"
#include "hyperlink.h"

namespace fasttrips {
    /// utility function to make sure time is in [0, 24*60) = [0, 1440)
    double fix_time_range(double time) {
        double ret_time = time;
        while (ret_time <     0.0) { ret_time += 1440.0; }
        while (ret_time >= 1440.0) { ret_time -= 1440.0; }
        return ret_time;
    }

    // Default constructor
    Path::Path() :
        outbound_(false),
        enumerating_(false),
        fare_(0),
        cost_(0),
        capacity_problem_(false),
        initial_fare_(0),
        initial_cost_(0)
    {}

    Path::Path(bool outbound, bool enumerating) :
        outbound_(outbound),
        enumerating_(enumerating),
        fare_(0),
        cost_(0),
        capacity_problem_(false),
        initial_fare_(0),
        initial_cost_(0)
    {}

    Path::~Path()
    {}

    // How many links are in this path?
    size_t Path::size() const
    {
        return links_.size();
    }

    double Path::cost() const
    {
        return cost_;
    }

    double Path::fare() const
    {
        return fare_;
    }

    double Path::initialCost() const
    {
        return initial_cost_;
    }

    double Path::initialFare() const
    {
        return initial_fare_;
    }

    // Clear
    void Path::clear()
    {
        links_.clear();
        boards_per_fareperiod_.clear();
        cost_ = 0;
        capacity_problem_ = false;
    }

    // Accessor
    const std::pair<int, StopState>& Path::operator[](size_t n) const
    {
        return links_[n];
    }

    std::pair<int, StopState>& Path::operator[](size_t n)
    {
        return links_[n];
    }

    const std::pair<int, StopState>& Path::back() const
    {
        return links_.back();
    }

    std::pair<int, StopState>& Path::back()
    {
        return links_.back();
    }

    const std::pair<int, StopState>* Path::lastAddedTrip() const
    {
        if (links_.size() <= 1) { return NULL; }
        std::vector< std::pair<int, StopState> >::const_reverse_iterator riter;
        for (riter = links_.rbegin(); riter != links_.rend(); ++riter) {
            const StopState& ss = riter->second;
            if (ss.deparr_mode_ == fasttrips::MODE_TRANSIT) {
                return &(*riter);
            }
        }
        return NULL;
    }

    int Path::boardsForFarePeriod(const std::string& fare_period) const
    {
        std::map< std::string, int >::const_iterator iter = boards_per_fareperiod_.find(fare_period);
        if (iter != boards_per_fareperiod_.end()) {
            return iter->second;
        }
        return 0;
    }


    /// Comparison
    bool Path::operator<(const Path& path2) const
    {
        // sort by cost first -- lowest cost at the top of the PathSet
        if (cost() < path2.cost()) { return true; }
        if (cost() > path2.cost()) { return false; }

        if (size() < path2.size()) { return true; }
        if (size() > path2.size()) { return false; }
        // if number of stops matches, check the stop ids and deparr_mode_
        for (int ind=0; ind<size(); ++ind) {
            if (links_[ind].first < path2[ind].first) { return true; }
            if (links_[ind].first > path2[ind].first) { return false; }
            if (links_[ind].second.deparr_mode_ < path2[ind].second.deparr_mode_) { return true; }
            if (links_[ind].second.deparr_mode_ > path2[ind].second.deparr_mode_) { return false; }
            if (links_[ind].second.trip_id_     < path2[ind].second.trip_id_    ) { return true; }
            if (links_[ind].second.trip_id_     > path2[ind].second.trip_id_    ) { return false; }
        }
        return false;
    }

    // Add link to the path, modifying if necessary
    // Return feasibility (infeasible if two out of order trips)
    bool Path::addLink(int stop_id,
                       const StopState& link,
                       std::ostream& trace_file,
                       const PathSpecification& path_spec,
                       const PathFinder& pf)
    {
        // We'll likely modify this
        StopState new_link      = link;
        // for simplicity, don't need link to low cost path here.
        new_link.low_cost_path_ = NULL;
        bool feasible           = true;

        // if we already have liks
        if (links_.size() > 0)
        {
            bool chrono_order    = (!outbound_ && !enumerating_) || (outbound_ && enumerating_);
            StopState& prev_link = links_.back().second;

            if (path_spec.trace_)
            {
                trace_file << (outbound_ ? "outbound, " : "inbound, ") << (enumerating_ ? "enumerating, " : "labeling, ");
                trace_file << (chrono_order ? "chrono, " : "not chrono, ") << "size " << links_.size() << ", prev mode ";
                pf.printMode(trace_file, prev_link.deparr_mode_, prev_link.trip_id_);
                trace_file << std::endl;
                trace_file << "path_req ";
                Hyperlink::printStopState(trace_file, stop_id, link, path_spec, pf);
                trace_file << std::endl;

                // delete this later
                trace_file << "--------------- path_before ---- (cost " << cost_ << ", fare " << fare_ << ")" << std::endl;
                print(trace_file, path_spec, pf);
                trace_file << "--------------------------------" << std::endl;
            }

            // UPDATES to links
            // Hyperpaths have some uncertainty built in which we need to rectify as we go through and choose
            // concrete path states.

            // this is confusing
            double& new_dep_time  = (outbound_ ? new_link.deparr_time_  : new_link.arrdep_time_ );
            double& new_arr_time  = (outbound_ ? new_link.arrdep_time_  : new_link.deparr_time_ );
            double& prev_dep_time = (outbound_ ? prev_link.deparr_time_ : prev_link.arrdep_time_);
            double& prev_arr_time = (outbound_ ? prev_link.arrdep_time_ : prev_link.deparr_time_);

            // chronological order.
            if (chrono_order)
            {
                // Leave origin as late as possible
                if (prev_link.deparr_mode_ == MODE_ACCESS) {
                    int first_stop_id  = (outbound_ ? stop_id       : new_link.stop_succpred_);
                    int first_stop_seq = (outbound_ ? new_link.seq_ : new_link.seq_succpred_ );
                    double dep_time = pf.getScheduledDeparture(new_link.trip_id_, first_stop_id, first_stop_seq);
                    // set departure time for the access link to perfectly catch the vehicle
                    // todo: what if there is a wait queue?
                    prev_arr_time = dep_time;
                    prev_dep_time = dep_time - prev_link.link_time_;
                    // no wait time for the trip
                    new_link.link_time_ = new_arr_time - new_dep_time;
                }
                // *Fix trip time*
                else if (isTrip(new_link.deparr_mode_)) {
                    // link time is arrival time - previous arrival time
                    new_link.link_time_ = new_arr_time - prev_arr_time;
                    if (new_link.link_time_ < 0)      { feasible = false; }
                    if (new_dep_time < prev_arr_time) { feasible = false; }
                }
                // *Fix transfer times*
                else if (new_link.deparr_mode_ == MODE_TRANSFER) {
                    new_dep_time = prev_arr_time;   // start transferring immediately
                    new_arr_time = new_dep_time + new_link.link_time_;
                }
                // Egress: don't wait, just walk. Get to destination as early as possible
                else if (new_link.deparr_mode_ == MODE_EGRESS) {
                    new_dep_time = prev_arr_time;
                    new_arr_time = new_dep_time + new_link.link_time_;
                }
            }

            // REVERSE chronological order: egress, trip, [transfer, trip]*, access
            else
            {
                // Leave origin as late as possible
                if (new_link.deparr_mode_ == MODE_ACCESS) {
                    int first_stop_id  = (outbound_ ? new_link.stop_succpred_ : prev_link.stop_succpred_ );
                    int first_stop_seq = (outbound_ ? prev_link.seq_          : prev_link.seq_succpred_  );
                    double dep_time = pf.getScheduledDeparture(prev_link.trip_id_, first_stop_id, first_stop_seq);
                    // set arrival time for the access link to perfectly catch the vehicle
                    // todo: what if there is a wait queue?
                    new_arr_time = dep_time;
                    new_dep_time = new_arr_time - new_link.link_time_;
                    // no wait time for the trip
                    prev_link.link_time_ = prev_arr_time - prev_dep_time;
                }
                // *Fix trip time*: we are choosing in reverse so pretend the wait time is zero for now to
                // accurately evaluate possible transfers in next choice.
                else if (isTrip(new_link.deparr_mode_)) {
                    // no wait time yet
                    new_link.link_time_ = new_arr_time - new_dep_time;
                    // If we just picked this trip and the previous (next in time) is transfer then we know the wait now
                    // and we can update the transfer and the trip with the real wait
                    if (prev_link.deparr_mode_ == MODE_TRANSFER) {
                        // move transfer time so we do it right after arriving
                        prev_dep_time = new_arr_time;                        // depart right away for the transfer
                        prev_arr_time = new_arr_time + prev_link.link_time_; // arrive after walk

                        // if the previous trip departure time is before the transfer arrival time, we have a feasibility issue
                        double prev_trip_dep_time = (outbound_ ? links_[links_.size()-2].second.deparr_time_ : links_[links_.size()-2].second.arrdep_time_);
                        if (prev_trip_dep_time < prev_arr_time) { feasible = false; }

                        // give the wait time to the previous trip
                        double prev_trip_arr_time = (outbound_ ? links_[links_.size()-2].second.arrdep_time_ : links_[links_.size()-2].second.deparr_time_);
                        links_[links_.size()-2].second.link_time_ = prev_trip_arr_time - prev_arr_time;
                        // negative wait time means infeasible
                        if (links_[links_.size()-2].second.link_time_ < 0) { feasible = false; }
                    }
                }
                // *Fix transfer depart/arrive times*: transfer as late as possible to preserve options for earlier trip
                else if (new_link.deparr_mode_ == MODE_TRANSFER) {
                    new_arr_time = prev_dep_time; // arrive for subsequent trip (prev_link)
                    new_dep_time = new_arr_time - new_link.link_time_;
                }
                // Egress: don't wait, just walk. Get to destination as early as possible
                if (prev_link.deparr_mode_ == MODE_EGRESS) {
                    prev_dep_time = new_arr_time;
                    prev_arr_time = prev_dep_time + prev_link.link_time_;
                }
            }
        }
        cost_          += new_link.link_cost_;
        fare_          += new_link.link_fare_;
        new_link.cost_  = cost_;
        links_.push_back( std::make_pair(stop_id, new_link) );

        // update boards_per_fareperiod_
        if (link.fare_period_) {
            boards_per_fareperiod_[link.fare_period_->fare_period_] += 1;
        }

        if (path_spec.trace_)
        {
            trace_file << "path_add ";
            Hyperlink::printStopState(trace_file, stop_id, links_.back().second, path_spec, pf);
            trace_file << std::endl;

            // this is excessive but oh well
            if (links_.size() > 1) {
                trace_file << "--------------- path so far ----" << (feasible ? " (feasible)" : " (infeasible)") << " (cost " << cost_ << ", fare " << fare_ << ")" << std::endl;
                print(trace_file, path_spec, pf);
                trace_file << "--------------------------------" << std::endl;
            }
        }
        return feasible;
    }

    // Returns the fare given the relevant fare period, adjusting for transfer from last fare period as applicable
    double Path::getFareWithTransfer(const PathFinder&  pf,
                                     const std::string& last_fare_period,
                                     const FarePeriod*  fare_period) const
    {
        // no fare period --> no fare
        if (fare_period == 0) {
            return 0.0;
        }

        double fare = fare_period->price_;
        // no previous fare period -> no adjustment
        if (last_fare_period == "") {
            return fare;
        }

        // get the transfer info
        const FareTransfer* ft = pf.getFareTransfer(last_fare_period, fare_period->fare_period_);
        if (ft == (const FareTransfer*)0) {
            return fare;
        }

        if (ft->type_ == TRANSFER_FREE) {
            fare = 0.0;
        } else if (ft->type_ == TRANSFER_DISCOUNT) {
            fare = fare - ft->amount_;
        } else if (ft->type_ == TRANSFER_COST) {
            fare = ft->amount_;
        }

        // must be non-negative
        if (fare < 0) { fare = 0; }

        return fare;
    }

    /**
     * Calculate the path cost now that we know all the links.  This may result in different
     * costs than the original costs.  This updates the path's StopState.cost_ attributes
     * and returns the cost
     */
    void Path::calculateCost(
        std::ostream& trace_file,
        const PathSpecification& path_spec,
        const PathFinder& pf,
        bool hush)
    {
        // no stops - nothing to do
        if (links_.size()==0) { return; }

        // save aside the fare and cost
        initial_fare_ = fare_;
        initial_cost_ = cost_;

        bool chrono_order   = (!outbound_ && !enumerating_) || (outbound_ && enumerating_);
        if (path_spec.trace_ && !hush) {
            trace_file << "Path::calculateCost() (chrono? " << (chrono_order ? "yes, " : "no,");
            trace_file << " cost: " << initial_cost_ << ", fare: " << initial_fare_ << ")" << std::endl;
            print(trace_file, path_spec, pf);
            trace_file << std::endl;
        }

        bool   first_trip           = true;
        double dir_factor           = path_spec.outbound_ ? 1.0 : -1.0;

        // iterate through the states in chronological order
        int start_ind       = chrono_order ? 0 : links_.size()-1;
        int end_ind         = chrono_order ? links_.size() : -1;
        int inc             = chrono_order ? 1 : -1;

        cost_               = 0;
        fare_               = 0;
        std::string last_fare_period;

        // for free transfer calculations -- fare_period -> (first board time, board count)
        typedef std::map< std::string, std::pair<double,int> > FarePeriodForFreeTransfers;
        FarePeriodForFreeTransfers fp_for_freexfers;

        for (int index = start_ind; index != end_ind; index += inc)
        {
            int stop_id             = links_[index].first;
            StopState& stop_state   = links_[index].second;

            int orig_stop           = (path_spec.outbound_? stop_id : stop_state.stop_succpred_);
            int dest_stop           = (path_spec.outbound_? stop_state.stop_succpred_ : stop_id);

            // ============= access =============
            if (stop_state.deparr_mode_ == MODE_ACCESS)
            {
                // inbound: preferred time is origin departure time
                double orig_departure_time        = (path_spec.outbound_ ? stop_state.deparr_time_ : stop_state.deparr_time_ - stop_state.link_time_);

                int transit_stop                  = (path_spec.outbound_ ? stop_state.stop_succpred_ : stop_id);
                const NamedWeights* named_weights = pf.getNamedWeights( path_spec.user_class_, path_spec.purpose_, MODE_ACCESS, path_spec.access_mode_, stop_state.trip_id_);
                Attributes          attributes    = *(pf.getAccessAttributes( path_spec.origin_taz_id_, stop_state.trip_id_, transit_stop, orig_departure_time ));

                attributes["arrive_early_min"]     = 0;
                attributes["arrive_late_min"]      = 0;
                attributes["depart_early_min"]     = 0;
                attributes["depart_late_min"]      = 0;

                if (!path_spec.outbound_) {
                  // early -- use early function
                  if (orig_departure_time < path_spec.preferred_time_) {
                    attributes["depart_early_min"] = path_spec.preferred_time_ - orig_departure_time;
                  }
                  else {
                    attributes["depart_late_min"]  = orig_departure_time - path_spec.preferred_time_;
                  }
                }



                stop_state.link_cost_             = pf.tallyLinkCost(stop_state.trip_id_, path_spec, trace_file, *named_weights, attributes, hush);
            }
            // ============= egress =============
            else if (stop_state.deparr_mode_ == MODE_EGRESS)
            {
                // outbound: preferred time is destination arrival time
                double dest_arrival_time          = (path_spec.outbound_ ? stop_state.deparr_time_ + stop_state.link_time_ : stop_state.deparr_time_);

                int transit_stop                  = (path_spec.outbound_ ? stop_id : stop_state.stop_succpred_);
                const NamedWeights* named_weights = pf.getNamedWeights(  path_spec.user_class_, path_spec.purpose_, MODE_EGRESS, path_spec.egress_mode_, stop_state.trip_id_);
                Attributes          attributes    = *(pf.getAccessAttributes( path_spec.destination_taz_id_, stop_state.trip_id_, transit_stop, fmod(dest_arrival_time,24.0*60.0)));

                attributes["arrive_early_min"]    = 0;
                attributes["arrive_late_min"]     = 0;
                attributes["depart_early_min"]    = 0;
                attributes["depart_late_min"]     = 0;

                if (path_spec.outbound_) {
                  // late -- use late function
                  if (dest_arrival_time > path_spec.preferred_time_) {
                    attributes["arrive_late_min"] = dest_arrival_time - path_spec.preferred_time_;
                  }
                  else {
                    attributes["arrive_early_min"]= path_spec.preferred_time_ - dest_arrival_time;
                  }
                }



                stop_state.link_cost_             = pf.tallyLinkCost(stop_state.trip_id_, path_spec, trace_file, *named_weights, attributes, hush);

            }
            // ============= transfer =============
            else if (stop_state.deparr_mode_ == MODE_TRANSFER)
            {
                const Attributes* link_attr       = pf.getTransferAttributes(orig_stop, dest_stop);
                const NamedWeights* named_weights = pf.getNamedWeights( path_spec.user_class_, path_spec.purpose_, MODE_TRANSFER, "transfer", pf.transferSupplyMode());
                stop_state.link_cost_             = pf.tallyLinkCost(pf.transferSupplyMode(), path_spec, trace_file, *named_weights, *link_attr, hush);
            }
            // ============= trip =============
            else
            {
                double trip_ivt_min               = (stop_state.arrdep_time_ - stop_state.deparr_time_)*dir_factor;
                double trip_depart_time           = path_spec.outbound_ ? stop_state.deparr_time_ : stop_state.arrdep_time_;
                double wait_min                   = stop_state.link_time_ - trip_ivt_min;

                const TripInfo& trip_info         = *(pf.getTripInfo(stop_state.trip_id_));
                int supply_mode_num               = trip_info.supply_mode_num_;
                const NamedWeights* named_weights = pf.getNamedWeights( path_spec.user_class_, path_spec.purpose_, MODE_TRANSIT, path_spec.transit_mode_, supply_mode_num);
                Attributes link_attr              = trip_info.trip_attr_;
                link_attr["in_vehicle_time_min"]  = trip_ivt_min;
                link_attr["wait_time_min"]        = wait_min;
                link_attr["overcap"]              = pf.getTripStopTime(stop_state.trip_id_, stop_state.seq_).overcap_;
                link_attr["at_capacity"]          = (link_attr["overcap"] >= 0 ? 1.0 : 0.0);  // binary, 0 means at capacity
                // overcap should be non-negative
                if (link_attr["overcap"] < 0) { link_attr["overcap"] = 0; }

                const FarePeriod* fp              = stop_state.fare_period_;
                if (fp) {
                    // adjust fare
                    stop_state.link_fare_         = getFareWithTransfer(pf, last_fare_period, fp);

                    // check if free transfer based on fare attributes
                    FarePeriodForFreeTransfers::iterator fpft_iter = fp_for_freexfers.find(fp->fare_period_);
                    if (fpft_iter == fp_for_freexfers.end()) {
                        // initialize
                        fp_for_freexfers[fp->fare_period_] = std::make_pair(trip_depart_time, 1);
                    } else {
                        // time since first board, in seconds
                        double transfer_time_sec = (trip_depart_time - fp_for_freexfers[fp->fare_period_].first)*60.0;

                        // check if free transfer
                        if ((fp->transfers_ > 0) &&                                          // free transfer allowed
                            (fp_for_freexfers[fp->fare_period_].second <= fp->transfers_) && // this one qualifies
                            ((fp->transfer_duration_ < 0) ||                                 // no max transfer duration or
                             (transfer_time_sec <= fp->transfer_duration_)))                 //   transfer time <= transfer duration
                        {
                            stop_state.link_fare_ = 0.0;
                        }

                        // bump the count
                        fp_for_freexfers[fp->fare_period_].second += 1;
                    }

                    link_attr["fare"]             = stop_state.link_fare_;
                    // store last fare period
                    last_fare_period              = fp->fare_period_;
                } else {
                    last_fare_period              = "";
                }

                stop_state.link_cost_             = pf.tallyLinkCost(supply_mode_num, path_spec, trace_file, *named_weights, link_attr, hush);

                first_trip = false;
            }
            cost_                            += stop_state.link_cost_;
            fare_                            += stop_state.link_fare_;
            stop_state.cost_                  = cost_;
        }

        if (path_spec.trace_ && !hush) {
            trace_file << " ==================================================> cost: " << cost_ << ", fare: " << fare_ << std::endl;
            print(trace_file, path_spec, pf);
            trace_file << std::endl;
        }
    }

    void Path::print(
        std::ostream& ostr,
        const PathSpecification& path_spec,
        const PathFinder& pf) const
    {
        Hyperlink::printStopStateHeader(ostr, path_spec);
        ostr << std::endl;
        for (int index = 0; index < links_.size(); ++index)
        {
            Hyperlink::printStopState(ostr, links_[index].first, links_[index].second, path_spec, pf);
            ostr << std::endl;
        }
    }

    void Path::printCompat(
        std::ostream& ostr,
        const PathSpecification& path_spec,
        const PathFinder& pf) const
    {
        if (links_.size() == 0)
        {
            ostr << "no_path";
            return;
        }
        // board stops, trips, alight stops
        std::string board_stops, trips, alight_stops;
        int start_ind = path_spec.outbound_ ? 0 : links_.size()-1;
        int end_ind   = path_spec.outbound_ ? links_.size() : -1;
        int inc       = path_spec.outbound_ ? 1 : -1;
        for (int index = start_ind; index != end_ind; index += inc)
        {
            int stop_id = links_[index].first;
            // only want trips
            if (links_[index].second.deparr_mode_ == MODE_ACCESS  ) { continue; }
            if (links_[index].second.deparr_mode_ == MODE_EGRESS  ) { continue; }
            if (links_[index].second.deparr_mode_ == MODE_TRANSFER) { continue; }
            if ( board_stops.length() > 0) {  board_stops += ","; }
            if (       trips.length() > 0) {        trips += ","; }
            if (alight_stops.length() > 0) { alight_stops += ","; }
            board_stops  += (path_spec.outbound_ ? pf.stopStringForId(stop_id) : pf.stopStringForId(links_[index].second.stop_succpred_));
            trips        += pf.tripStringForId(links_[index].second.trip_id_);
            alight_stops += (path_spec.outbound_ ? pf.stopStringForId(links_[index].second.stop_succpred_) : pf.stopStringForId(stop_id));
        }
        ostr << " " << board_stops << " " << trips << " " << alight_stops;

    }

}
