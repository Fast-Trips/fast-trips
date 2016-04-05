/**
 * \file path.cpp
 *
 * Path implementation
 **/

#include "path.h"
#include "pathfinder.h"
#include "hyperlink.h"

namespace fasttrips {

    Path::Path(bool outbound, bool enumerating) :
        chrono_order_( (outbound && enumerating) || (!outbound && !enumerating) ),
        cost_(0),
        capacity_problem_(false)
    {}

    Path::~Path()
    {}

    // How many links are in this path?
    size_t Path::size() const
    {
        return links_.size();
    }

    // What's the cost of this path?
    double Path::cost() const
    {
        return cost_;
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

    /// Comparison
    bool Path::operator<(const Path& path2) const
    {
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
    void Path::addLink(int stop_id,
                       const StopState& link,
                       std::ostream& trace_file,
                       const PathSpecification& path_spec,
                       const PathFinder& pf)
    {
        // We'll likely modify this
        StopState new_link = link;

        // if we already have liks
        if (links_.size() > 0)
        {
            if (path_spec.trace_)
            {
                trace_file << "path_req ";
                Hyperlink::printStopState(trace_file, stop_id, link, path_spec, pf);
                trace_file << std::endl;
            }

            // UPDATES to links
            // Hyperpaths have some uncertainty built in which we need to rectify as we go through and choose
            // concrete path states.
            StopState& prev_link = links_.back().second;

            // chronological order -- access to egress.
            // OUTBOUND: We are choosing links in chronological order.
            if (chrono_order_)
            {
                // Leave origin as late as possible
                if (prev_link.deparr_mode_ == MODE_ACCESS) {
                    double dep_time = pf.getScheduledDeparture(new_link.trip_id_, stop_id, new_link.seq_);
                    // set departure time for the access link to perfectly catch the vehicle
                    // todo: what if there is a wait queue?
                    prev_link.arrdep_time_ = dep_time;
                    prev_link.deparr_time_ = dep_time - links_.front().second.link_time_;
                    // no wait time for the trip
                    new_link.link_time_ = new_link.arrdep_time_ - new_link.deparr_time_;
                }
                // *Fix trip time*
                else if (isTrip(new_link.deparr_mode_)) {
                    // link time is arrival time - previous arrival time
                    new_link.link_time_ = new_link.arrdep_time_ - prev_link.arrdep_time_;
                }
                // *Fix transfer times*
                else if (new_link.deparr_mode_ == MODE_TRANSFER) {
                    new_link.deparr_time_ = prev_link.arrdep_time_;   // start transferring immediately
                    new_link.arrdep_time_ = new_link.deparr_time_ + new_link.link_time_;
                }
                // Egress: don't wait, just walk. Get to destination as early as possible
                else if (new_link.deparr_mode_ == MODE_EGRESS) {
                    new_link.deparr_time_ = prev_link.arrdep_time_;
                    new_link.arrdep_time_ = new_link.deparr_time_ + new_link.link_time_;
                }
            }

            // INBOUND: We are choosing links in REVERSE chronological order
            else
            {
                // Leave origin as late as possible
                if (new_link.deparr_mode_ == MODE_ACCESS) {
                    double dep_time = pf.getScheduledDeparture(prev_link.trip_id_, stop_id, prev_link.seq_succpred_);
                    // set arrival time for the access link to perfectly catch the vehicle
                    // todo: what if there is a wait queue?
                    new_link.deparr_time_ = dep_time;
                    new_link.arrdep_time_ = new_link.deparr_time_ - new_link.link_time_;
                    // no wait time for the trip
                    prev_link.link_time_ = prev_link.deparr_time_ - prev_link.arrdep_time_;
                }
                // *Fix trip time*: we are choosing in reverse so pretend the wait time is zero for now to
                // accurately evaluate possible transfers in next choice.
                else if (isTrip(new_link.deparr_mode_)) {
                    new_link.link_time_ = new_link.deparr_time_ - new_link.arrdep_time_;
                    // If we just picked this trip and the previous (next in time) is transfer then we know the wait now
                    // and we can update the transfer and the trip with the real wait
                    if (prev_link.deparr_mode_ == MODE_TRANSFER) {
                        // move transfer time so we do it right after arriving
                        prev_link.arrdep_time_ = new_link.deparr_time_; // depart right away
                        prev_link.deparr_time_ = new_link.deparr_time_ + prev_link.link_time_; // arrive after walk
                        // give the wait time to the previous trip
                        links_[links_.size()-2].second.link_time_ = links_[links_.size()-2].second.deparr_time_ - prev_link.deparr_time_;
                    }
                    // If the previous (next in time) is another trip (so zero-walk transfer) give it wait time
                    else if (isTrip(new_link.deparr_mode_)) {
                        prev_link.link_time_ = prev_link.deparr_time_ - new_link.deparr_time_;
                    }
                }
                // *Fix transfer depart/arrive times*: transfer as late as possible to preserve options for earlier trip
                else if (new_link.deparr_mode_ == MODE_TRANSFER) {
                    new_link.deparr_time_ = prev_link.arrdep_time_;
                    new_link.arrdep_time_ = new_link.deparr_time_ - new_link.link_time_;
                }
                // Egress: don't wait, just walk. Get to destination as early as possible
                if (prev_link.deparr_mode_ == MODE_EGRESS) {
                    prev_link.arrdep_time_ = new_link.deparr_time_;
                    prev_link.deparr_time_ = prev_link.arrdep_time_ + prev_link.link_time_;
                }
            }
        }
        links_.push_back( std::make_pair(stop_id, new_link) );

        if (path_spec.trace_)
        {
            trace_file << "path_add ";
            Hyperlink::printStopState(trace_file, stop_id, links_.back().second, path_spec, pf);
            trace_file << std::endl;
        }
    }

    /**
     * Calculate the path cost now that we know all the links.  This may result in different
     * costs than the original costs.  This updates the path's StopState.cost_ attributes
     * and returns the cost
     */
    void Path::calculateCost(
        std::ostream& trace_file,
        const PathSpecification& path_spec,
        const PathFinder& pf)
    {
        // no stops - nothing to do
        if (links_.size()==0) { return; }

        if (path_spec.trace_) {
            trace_file << "calculatePathCost:" << std::endl;
            print(trace_file, path_spec, pf);
            trace_file << std::endl;
        }

        bool   first_trip           = true;
        double dir_factor           = path_spec.outbound_ ? 1.0 : -1.0;

        // iterate through the states in chronological order
        int start_ind       = chrono_order_ ? 0 : links_.size()-1;
        int end_ind         = chrono_order_ ? links_.size() : -1;
        int inc             = chrono_order_ ? 1 : -1;

        cost_               = 0;
        for (int index = start_ind; index != end_ind; index += inc)
        {
            int stop_id             = links_[index].first;
            StopState& stop_state   = links_[index].second;

            // ============= access =============
            if (stop_state.deparr_mode_ == MODE_ACCESS)
            {
                // inbound: preferred time is origin departure time
                double orig_departure_time        = (path_spec.outbound_ ? stop_state.deparr_time_ : stop_state.deparr_time_ - stop_state.link_time_);
                double preference_delay           = (path_spec.outbound_ ? 0 : orig_departure_time - path_spec.preferred_time_);

                int transit_stop                  = (path_spec.outbound_ ? stop_state.stop_succpred_ : stop_id);
                const NamedWeights* named_weights = pf.getNamedWeights( path_spec.user_class_, MODE_ACCESS, path_spec.access_mode_, stop_state.trip_id_);
                Attributes          attributes    = *(pf.getAccessAttributes( path_spec.origin_taz_id_, stop_state.trip_id_, transit_stop ));
                attributes["preferred_delay_min"] = preference_delay;

                stop_state.cost_                  = pf.tallyLinkCost(stop_state.trip_id_, path_spec, trace_file, *named_weights, attributes);
                cost_                            += stop_state.cost_;
            }
            // ============= egress =============
            else if (stop_state.deparr_mode_ == MODE_EGRESS)
            {
                // outbound: preferred time is destination arrival time
                double dest_arrival_time          = (path_spec.outbound_ ? stop_state.deparr_time_ + stop_state.link_time_ : stop_state.deparr_time_);
                double preference_delay           = (path_spec.outbound_ ? path_spec.preferred_time_ - dest_arrival_time : 0);

                int transit_stop                  = (path_spec.outbound_ ? stop_id : stop_state.stop_succpred_);
                const NamedWeights* named_weights = pf.getNamedWeights(  path_spec.user_class_, MODE_EGRESS, path_spec.egress_mode_, stop_state.trip_id_);
                Attributes          attributes    = *(pf.getAccessAttributes( path_spec.destination_taz_id_, stop_state.trip_id_, transit_stop ));
                attributes["preferred_delay_min"] = preference_delay;

                stop_state.cost_                  = pf.tallyLinkCost(stop_state.trip_id_, path_spec, trace_file, *named_weights, attributes);
                cost_                            += stop_state.cost_;

            }
            // ============= transfer =============
            else if (stop_state.deparr_mode_ == MODE_TRANSFER)
            {
                int orig_stop                     = (path_spec.outbound_? stop_id : stop_state.stop_succpred_);
                int dest_stop                     = (path_spec.outbound_? stop_state.stop_succpred_ : stop_id);

                const Attributes* link_attr       = pf.getTransferAttributes(orig_stop, dest_stop);
                const NamedWeights* named_weights = pf.getNamedWeights( path_spec.user_class_, MODE_TRANSFER, "transfer", pf.transferSupplyMode());
                stop_state.cost_                  = pf.tallyLinkCost(pf.transferSupplyMode(), path_spec, trace_file, *named_weights, *link_attr);
                cost_                            += stop_state.cost_;
            }
            // ============= trip =============
            else
            {
                double trip_ivt_min               = (stop_state.arrdep_time_ - stop_state.deparr_time_)*dir_factor;
                double wait_min                   = stop_state.link_time_ - trip_ivt_min;

                const TripInfo& trip_info         = *(pf.getTripInfo(stop_state.trip_id_));
                int supply_mode_num               = trip_info.supply_mode_num_;
                const NamedWeights* named_weights = pf.getNamedWeights( path_spec.user_class_, MODE_TRANSIT, path_spec.transit_mode_, supply_mode_num);
                Attributes link_attr              = trip_info.trip_attr_;
                link_attr["in_vehicle_time_min"]  = trip_ivt_min;
                link_attr["wait_time_min"]        = wait_min;


                stop_state.cost_                  = pf.tallyLinkCost(supply_mode_num, path_spec, trace_file, *named_weights, link_attr);
                cost_                            += stop_state.cost_;

                first_trip = false;
            }
        }

        if (path_spec.trace_) {
            trace_file << " ==================================================> cost: " << cost_ << std::endl;
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