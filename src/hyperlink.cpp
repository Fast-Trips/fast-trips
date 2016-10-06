/**
 * \file hyperlink.cpp
 *
 * Hyperlink implementation
 **/

#include "hyperlink.h"
#include "pathfinder.h"

#include <Python.h>
#include <math.h>
#include <ios>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <stack>

namespace fasttrips {

    bool isTrip(const int& mode)
    {
        return (mode == MODE_TRANSIT);
    }

    double Hyperlink::TIME_WINDOW_      = 0.0;
    double Hyperlink::STOCH_DISPERSION_ = 0.0;

    // Default constructor
    Hyperlink::Hyperlink() :
        stop_id_(0), linkset_trip_(false), linkset_nontrip_(false)
    {}

    // Constructor we should call
    Hyperlink::Hyperlink(int stop_id, bool outbound) :
        stop_id_(stop_id), linkset_trip_(outbound), linkset_nontrip_(outbound)
    {}

    // Destructor
    Hyperlink::~Hyperlink()
    {
        this->clear(true);
        this->clear(false);
    }

    // Remove the given stop state from cost_map_
    void Hyperlink::removeFromCostMap(const StopStateKey& ssk, const StopState& ss)
    {
        LinkSet& linkset = (isTrip(ssk.deparr_mode_) ? linkset_trip_ : linkset_nontrip_);

        // todo: switch this to cost_
        std::pair<CostToStopState::iterator, CostToStopState::iterator> iter_range = linkset.cost_map_.equal_range(ss.cost_);
        CostToStopState::iterator cm_iter = iter_range.first;
        while (cm_iter != iter_range.second) {
            if (cm_iter->second == ssk) {
                break;
            }
            ++cm_iter;
        }
        if (cm_iter->second != ssk) {
            std::cerr << "Hyperlink::removeFromCostMap() This shouldn't happen" << std::endl;
        }
        linkset.cost_map_.erase(cm_iter);
    }


    // Reset latest departure/earliest arrival
    void Hyperlink::resetLatestDepartureEarliestArrival(bool of_trip_links, const PathSpecification& path_spec)
    {
        LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);
        // reset
        linkset.latest_dep_earliest_arr_ = 0;
        linkset.lder_ssk_.deparr_mode_   = MODE_UNSET;
        linkset.lder_ssk_.trip_id_       = 0;
        linkset.lder_ssk_.stop_succpred_ = 0;
        linkset.lder_ssk_.seq_           = 0;
        linkset.lder_ssk_.seq_succpred_  = 0;

        for (StopStateMap::const_iterator it = linkset.stop_state_map_.begin(); it != linkset.stop_state_map_.end(); ++it)
        {
            const StopStateKey& ssk = it->first;
            const StopState&    ss  = it->second;

            if (linkset.lder_ssk_.deparr_mode_ == MODE_UNSET)
            {
                linkset.latest_dep_earliest_arr_ = ss.deparr_time_;
                linkset.lder_ssk_                = ssk;
            } else if (( path_spec.outbound_ && (linkset.latest_dep_earliest_arr_ > ss.deparr_time_)) ||
                       (!path_spec.outbound_ && (linkset.latest_dep_earliest_arr_ < ss.deparr_time_)))
            {
                linkset.latest_dep_earliest_arr_ = ss.deparr_time_;
                linkset.lder_ssk_                = ssk;
            }
        }

    }

    // Update the low cost path for this stop state
    void Hyperlink::updateLowCostPath(
        const StopStateKey& ssk,
        const Hyperlink* prev_link,
        std::ostream& trace_file,
        const PathSpecification& path_spec,
        const PathFinder& pf)
    {
        LinkSet& linkset = (isTrip(ssk.deparr_mode_) ? linkset_trip_ : linkset_nontrip_);
        // get the link
        StopState& ss = linkset.stop_state_map_[ssk];

        // if it's a start link, it's a new path
        if (( path_spec.outbound_ && ssk.deparr_mode_ == MODE_EGRESS) ||
            (!path_spec.outbound_ && ssk.deparr_mode_ == MODE_ACCESS))
        {
            if (ss.low_cost_path_ != NULL) { std::cerr << "updateLowCostPath error1" << std::endl; return; }
            // initialize it
            ss.low_cost_path_ = new Path(path_spec.outbound_, false); // labeling, not enumerating
            ss.low_cost_path_->addLink(stop_id_, ss, trace_file, path_spec, pf);
            return;
        }

        // otherwise prev_link better exist for us to pull trips
        if (prev_link == NULL) { std::cerr << "updateLowCostPath error2" << std::endl; return; }

        // pull trips (for non-trip links) or non-trips for trip links
        const StopStateMap& prev_link_map = prev_link->getStopStateMap(!isTrip(ssk.deparr_mode_));
        for (StopStateMap::const_iterator it = prev_link_map.begin(); it != prev_link_map.end(); ++it)
        {
            const StopStateKey& prev_ssk = it->first;
            const StopState&    prev_ss  = it->second;

            if (prev_ss.low_cost_path_ == NULL) { continue; }

            Path path_candidate = *(prev_ss.low_cost_path_);
            // we shouldn't add a non-start state onto an empty path
            if (path_candidate.size() == 0) { continue; }
            bool feasible = path_candidate.addLink(stop_id_, ss, trace_file, path_spec, pf);
            if (!feasible) { continue; }

            // todo -- make this unnecessary?  if additive then the path.addLink() could take care of costs properly
            path_candidate.calculateCost(trace_file, path_spec, pf, true);

            if (path_spec.trace_) {
                trace_file << "Path candidate cost " << path_candidate.cost();
                trace_file << " compared to current cost " << (ss.low_cost_path_ ? ss.low_cost_path_->cost() : -999) << std::endl;
                path_candidate.print(trace_file, path_spec, pf);
            }

            // keep it?
            if (ss.low_cost_path_ == NULL) {
                ss.low_cost_path_ = new Path(path_candidate);
            } else if (ss.low_cost_path_->cost() > path_candidate.cost()) {
                *ss.low_cost_path_ = path_candidate;
            }
        }
    }

    // How many links make up the hyperlink?
    size_t Hyperlink::size() const
    {
        return linkset_trip_.stop_state_map_.size() + linkset_nontrip_.stop_state_map_.size();
    }

    // How many links make up the trip/nontrip hyperlink
    size_t Hyperlink::size(bool of_trip_links) const
    {
        const LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);
        return linkset.stop_state_map_.size();
    }

    const StopStateMap& Hyperlink::getStopStateMap(bool of_trip_links) const
    {
        const LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);
        return linkset.stop_state_map_;
    }

    // Accessor for the low cost path
    const Path* Hyperlink::getLowCostPath(bool of_trip_links) const
    {
        const Path* low_cost_path = NULL;
        double low_cost = 0;

        const LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);
        for (StopStateMap::const_iterator it = linkset.stop_state_map_.begin(); it != linkset.stop_state_map_.end(); ++it)
        {
            const StopState& ss = it->second;

            if (ss.low_cost_path_ == NULL) { continue; }

            if (low_cost_path == NULL) {
                low_cost_path = ss.low_cost_path_;
                low_cost      = ss.low_cost_path_->cost();
                continue;
            }

            if (low_cost_path->cost() > ss.low_cost_path_->cost()) {
                low_cost_path = ss.low_cost_path_;
                low_cost      = ss.low_cost_path_->cost();
            }
        }

        return low_cost_path;
    }

    bool Hyperlink::addLink(const StopState& ss, const Hyperlink* prev_link, bool& rejected,
                            std::ostream& trace_file, const PathSpecification& path_spec, const PathFinder& pf)
    {
        rejected = false;
        const StopStateKey ssk = { ss.deparr_mode_, ss.trip_id_, ss.stop_succpred_, ss.seq_, ss.seq_succpred_ };

        // add to the linkset based on the mode
        LinkSet& linkset = (isTrip(ssk.deparr_mode_) ? linkset_trip_ : linkset_nontrip_);

        // deterministic -- we only keep one, the low cost link
        if (path_spec.hyperpath_ == false)
        {
            // if the cost isn't better, reject
            if ((linkset.cost_map_.size() > 0) && (ss.cost_ >= linkset.cost_map_.begin()->first))
            {
                rejected = true;

                // log it
                if (path_spec.trace_) {
                    trace_file << "  + new ";
                    Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
                    trace_file << " (rejected)" << std::endl;
                }

                return false;
            }
            // otherwise, accept by clearing out
            clear(isTrip(ssk.deparr_mode_));
            // fall through to add it below
        }
        // simplest case -- we have no stop states/links, so just add it
        if (linkset.stop_state_map_.size() == 0)
        {
            linkset.latest_dep_earliest_arr_ = ss.deparr_time_;
            linkset.lder_ssk_                = ssk;
            linkset.sum_exp_cost_            = exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            linkset.hyperpath_cost_          = std::max(ss.cost_, MIN_COST);

            // add to the map
            linkset.stop_state_map_[ssk] = ss;

            // assume success
            linkset.cost_map_.insert (std::pair<double, StopStateKey>(ss.cost_,ssk));

            // log it
            if (path_spec.trace_) {
                trace_file << "  + new ";
                Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
                trace_file << std::endl;
            }

            // update low-cost path
            // updateLowCostPath(ssk, prev_link, trace_file, path_spec, pf);
            return true;
        }
        // ========= now we have links in the hyperlink =========

        // don't apply window stuff to the last labeling link (access for outbound, egress for inbound)
        bool is_last_link = false;
        if ( path_spec.outbound_ && (ss.deparr_mode_ == MODE_ACCESS)) { is_last_link = true; }
        if (!path_spec.outbound_ && (ss.deparr_mode_ == MODE_EGRESS)) { is_last_link = true; }

        // is it too early (outbound) or too late (inbound)?
        if ((!is_last_link) &&
            (( path_spec.outbound_ && (ss.deparr_time_ < linkset.latest_dep_earliest_arr_ - TIME_WINDOW_)) ||
             (!path_spec.outbound_ && (ss.deparr_time_ > linkset.latest_dep_earliest_arr_ + TIME_WINDOW_)))) {
            rejected = true;

            // log it
            if (path_spec.trace_) {
                trace_file << "  + new ";
                Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
                trace_file << " (rejected)" << std::endl;
            }

            return false;
        }

        // ========= now it's definitely going in =========

        bool update_state = false;
        // we have some stop states/links, so try to insert but we may fail
        std::pair<StopStateMap::iterator, bool> result_l = linkset.stop_state_map_.insert(std::pair<StopStateKey,StopState>(ssk,ss));

        // if we succeeded, the key isn't in here already
        if (result_l.second == true) {
            std::string notes;

            linkset.cost_map_.insert (std::pair<double, StopStateKey>(ss.cost_,ssk));

            // check if the window is updated -- this is a state update
            if ((!is_last_link) &&
                (( path_spec.outbound_ && (ss.deparr_time_ > linkset.latest_dep_earliest_arr_)) ||
                 (!path_spec.outbound_ && (ss.deparr_time_ < linkset.latest_dep_earliest_arr_))))
            {
                linkset.latest_dep_earliest_arr_  = ss.deparr_time_;
                linkset.lder_ssk_                 = ssk;
                update_state                      = true;
                notes                            += " (window)";
                // if the window changes, we need to prune states out of bounds -- this recalculates sum_exp_cost_
                pruneWindow(trace_file, path_spec, pf, isTrip(ssk.deparr_mode_));
            } else {
                linkset.sum_exp_cost_         += exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            }

            // check if the hyperpath cost is affected -- this would be a state update
            double hyperpath_cost  = std::max((-1.0/STOCH_DISPERSION_)*log(linkset.sum_exp_cost_), MIN_COST);
            if (abs(hyperpath_cost - linkset.hyperpath_cost_) > 0.0001)
            {
                std::ostringstream oss;
                oss << " (hp cost " << std::setprecision(6) << std::fixed << linkset.hyperpath_cost_ << "->" << hyperpath_cost << ")";
                notes                   += oss.str();
                update_state             = true;
                linkset.hyperpath_cost_  = hyperpath_cost;
            }

            // log it
            if (path_spec.trace_) {
                trace_file << "  + new ";
                Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
                trace_file << notes << std::endl;
            }

            // updateLowCostPath(ssk, prev_link, trace_file, path_spec, pf);
            return update_state;
        }

        // ========= the key is in already in here so replace the values =========
        std::string notes(" (sub)");

        // update the cost map
        removeFromCostMap(ssk, linkset.stop_state_map_[ssk]);
        linkset.cost_map_.insert (std::pair<double, StopStateKey>(ss.cost_,ssk));

        // update the cost
        linkset.sum_exp_cost_ -= exp(-1.0*STOCH_DISPERSION_*linkset.stop_state_map_[ssk].cost_);

        // we're replacing the stopstate so delete the old path
        if (linkset.stop_state_map_[ssk].low_cost_path_) {
            delete linkset.stop_state_map_[ssk].low_cost_path_;
            linkset.stop_state_map_[ssk].low_cost_path_ = NULL;
        }
        // and the other state elements
        linkset.stop_state_map_[ssk] = ss;
        // stop_state_map_[ssk].iteration_ = old_iteration; // remove this
        linkset.sum_exp_cost_ += exp(-1.0*STOCH_DISPERSION_*ss.cost_);

        // if the the latest_dep_earliest_arr_ were set to the previous value, we need to check
        if (linkset.lder_ssk_ == ssk)
        {
            if (path_spec.trace_) { trace_file << "Resetting lder" << std::endl; }
            resetLatestDepartureEarliestArrival(isTrip(ssk.deparr_mode_), path_spec);
        }

        // check if the window is updated -- this is a state update
        if ((!is_last_link) &&
            (( path_spec.outbound_ && (ss.deparr_time_ > linkset.latest_dep_earliest_arr_)) ||
             (!path_spec.outbound_ && (ss.deparr_time_ < linkset.latest_dep_earliest_arr_))))
        {
            linkset.latest_dep_earliest_arr_  = ss.deparr_time_;
            linkset.lder_ssk_                 = ssk;
            update_state                      = true;
            notes                            += " (window)";
            // if the window changes, we need to prune states out of bounds -- this recalculates sum_exp_cost_
            pruneWindow(trace_file, path_spec, pf, isTrip(ssk.deparr_mode_));
        }

        double hyperpath_cost  = std::max((-1.0/STOCH_DISPERSION_)*log(linkset.sum_exp_cost_),MIN_COST);
        if (abs(hyperpath_cost - linkset.hyperpath_cost_) > 0.0001)
        {
            std::ostringstream oss;
            oss << " (hp cost " << std::setprecision(6) << std::fixed << linkset.hyperpath_cost_ << "->" << hyperpath_cost << ")";
            notes                   += oss.str();
            update_state             = true;
            linkset.hyperpath_cost_  = hyperpath_cost;
        }

        // log it
        if (path_spec.trace_) {
            trace_file << "  + new ";
            Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
            trace_file << notes << std::endl;
        }

        // updateLowCostPath(ssk, prev_link, trace_file, path_spec, pf);
        return update_state;
    }

    void Hyperlink::clear(bool of_trip_links)
    {
        const StopStateKey zero_ssk = { 0.0, 0, 0, 0, 0.0 };

        LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);

        // this memory needs to be freed
        for (StopStateMap::iterator it = linkset.stop_state_map_.begin(); it != linkset.stop_state_map_.end(); ++it)
        {
            StopState& ss = it->second;
            if (ss.low_cost_path_) {
                delete ss.low_cost_path_;
                ss.low_cost_path_ = NULL;
            }
        }

        linkset.stop_state_map_.clear();
        linkset.cost_map_.clear();
        linkset.sum_exp_cost_               = 0;
        linkset.hyperpath_cost_             = 0;
        linkset.latest_dep_earliest_arr_    = 0;
        linkset.lder_ssk_                   = zero_ssk;

        // don't reset process counts
    }

    const StopState& Hyperlink::lowestCostStopState(bool of_trip_links) const
    {
        const LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);

        const StopStateKey& ssk = linkset.cost_map_.begin()->second;
        return linkset.stop_state_map_.find(ssk)->second;
    }

    // Given an arrival time into this hyperlink (outbound) or a departure time out of this hyperlink (inbound),
    // returns the best guess link
    // arrdep time is for a trip so looks at nontrip
    const StopState& Hyperlink::bestGuessLink(bool outbound, double arrdep_time) const
    {
        for (CostToStopState::const_iterator iter = linkset_nontrip_.cost_map_.begin(); iter != linkset_nontrip_.cost_map_.end(); ++iter)
        {
            const StopStateKey& ssk = iter->second;
            const StopState&    ss  = linkset_nontrip_.stop_state_map_.find(ssk)->second;
            if (outbound && (ss.deparr_time_ >= arrdep_time)) {
                return ss;
            }

            if (!outbound && (arrdep_time >= ss.deparr_time_)) {
                return ss;
            }
        }
        return linkset_nontrip_.stop_state_map_.find(linkset_nontrip_.cost_map_.begin()->second)->second;
    }

    // Given an arrival link into this hyperlink (outbound) or a departure time out of this hyperlink (inbound),
    // returns the best guess cost.  Time consuming but more accurate.  Make it an option?
    double Hyperlink::bestGuessCost(bool outbound, double arrdep_time) const
    {
        double sum_exp = 0.0;
        for (StopStateMap::const_iterator it = linkset_nontrip_.stop_state_map_.begin(); it != linkset_nontrip_.stop_state_map_.end(); ++it)
        {
            const StopState& ss = it->second;
            if (outbound && (ss.deparr_time_ >= arrdep_time)) {
                sum_exp += exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            } else if (!outbound && (arrdep_time >= ss.deparr_time_)) {
                sum_exp += exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            }
        }
        if (sum_exp == 0) {
            return MAX_COST;
        }
        return (-1.0/STOCH_DISPERSION_)*log(sum_exp);
    }


    // Returns the earliest departure (outbound) or latest arrival (inbound) of the links that make up this hyperlink
    double Hyperlink::earliestDepartureLatestArrival(bool outbound, bool of_trip_links) const
    {
        const LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);

        double earliest_dep_latest_arr = lowestCostStopState(of_trip_links).deparr_time_;
        for (StopStateMap::const_iterator it = linkset.stop_state_map_.begin(); it != linkset.stop_state_map_.end(); ++it)
        {
            if (outbound) {
                earliest_dep_latest_arr = std::min(earliest_dep_latest_arr, it->second.deparr_time_);
            } else {
                earliest_dep_latest_arr = std::max(earliest_dep_latest_arr, it->second.deparr_time_);
            }
        }
        return earliest_dep_latest_arr;
    }

    // Returns the trip id for the latest departure (outbound) or earliest arrival (inbound) trip
    double Hyperlink::latestDepartureEarliestArrival(bool of_trip_links) const
    {
        const LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);
        return linkset.latest_dep_earliest_arr_;
    }

    // Calculate the cost of just the non-walk links that make up this hyperlink
    double Hyperlink::calculateNonwalkLabel() const
    {
        return linkset_trip_.hyperpath_cost_;
    }

    // Accessor for the process count
    int Hyperlink::processCount(bool of_trip_links) const
    {
        const LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);
        return linkset.process_count_;
    }

    // Increment process count
    void Hyperlink::incrementProcessCount(bool of_trip_links)
    {
        LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);
        linkset.process_count_ += 1;
    }

    // Accessor for the hyperlink cost
    double Hyperlink::hyperpathCost(bool of_trip_links) const
    {
        const LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);
        return linkset.hyperpath_cost_;
    }

    void Hyperlink::printStopStateHeader(std::ostream& ostr, const PathSpecification& path_spec)
    {
        ostr << std::setw( 8) << std::setfill(' ') << std::right << "stop" << ": ";
        ostr << std::setw(10) << (path_spec.outbound_ ? "dep_time" : "arr_time");
        ostr << std::setw(12) << (path_spec.outbound_ ? "dep_mode" : "arr_mode");
        ostr << std::setw(22) << "trip_id";
        ostr << std::setw(12) << (path_spec.outbound_ ? "successor" : "predecessor");
        ostr << std::setw( 5) << "seq";
        ostr << std::setw( 5) << (path_spec.outbound_ ? "suc" : "pred");
        ostr << std::setw(12) << "linktime";
        ostr << std::setw(14) << "linkcost";
        ostr << std::setw(12) << "linkdist";
        ostr << std::setw(13) << "cost";
        ostr << std::setw( 9) << "iter";
        ostr << std::setw(10) << (path_spec.outbound_ ? "arr_time" : "dep_time");
    }

    void Hyperlink::printStopState(std::ostream& ostr, int stop_id, const StopState& ss, const PathSpecification& path_spec, const PathFinder& pf)
    {
        ostr << std::setw( 8) << std::setfill(' ') << std::right << pf.stopStringForId(stop_id) << ":   ";
        pf.printTime(ostr, ss.deparr_time_);
        ostr << "  ";
        pf.printMode(ostr, ss.deparr_mode_, ss.trip_id_);
        ostr << "  ";
        if (ss.deparr_mode_ == MODE_TRANSIT) {
            ostr << std::setw(20) << std::setfill(' ') << pf.tripStringForId(ss.trip_id_);
        } else if (ss.deparr_mode_ == MODE_ACCESS || ss.deparr_mode_ == MODE_EGRESS) {
            ostr << std::setw(20) << std::setfill(' ') << pf.modeStringForNum(ss.trip_id_);
        } else {
            ostr << std::setw(20) << std::setfill(' ') << ss.trip_id_;
        }
        ostr << "  ";
        ostr << std::setw(10) << std::setfill(' ') << pf.stopStringForId(ss.stop_succpred_);
        ostr << "  ";
        ostr << std::setw(3) << std::setfill(' ') << ss.seq_;
        ostr << "  ";
        ostr << std::setw(3) << std::setfill(' ') << ss.seq_succpred_;
        ostr << "  ";
        pf.printTimeDuration(ostr, ss.link_time_);
        ostr << "  ";
        if (path_spec.hyperpath_) {
            ostr << std::setw(12) << std::setprecision(4) << std::fixed << std::setfill(' ') << ss.link_cost_;
            ostr << std::setw(12) << std::setprecision(4) << std::fixed << std::setfill(' ') << ss.link_dist_;
            ostr << std::setw(13) << std::setprecision(4) << std::fixed << std::setfill(' ') << ss.cost_;
        } else {
            // cost is a time duration
            ostr << "  ";
            pf.printTimeDuration(ostr, ss.link_cost_);
            ostr << std::setw(12) << std::setprecision(4) << std::fixed << std::setfill(' ') << ss.link_dist_;
            ostr << "  ";
            pf.printTimeDuration(ostr, ss.cost_);
        }
        ostr << "  ";
        ostr << std::setw(7) << std::setfill(' ') << ss.iteration_;
        ostr << "  ";
        pf.printTime(ostr, ss.arrdep_time_);
    }

    void Hyperlink::printLinkSet(std::ostream& ostr, int stop_id, bool is_trip, const LinkSet& linkset, const PathSpecification& path_spec, const PathFinder& pf)
    {
        ostr << " (size " << linkset.cost_map_.size();
        ostr << "; count " << linkset.process_count_;
        ostr << "; lder ";
        pf.printTime(ostr, linkset.latest_dep_earliest_arr_);
        ostr << " @ trip ";
        if (is_trip) {
            ostr << pf.tripStringForId(linkset.lder_ssk_.trip_id_) << ", stop " << pf.stopStringForId(linkset.lder_ssk_.stop_succpred_);
        } else {
            ostr << pf.modeStringForNum(linkset.lder_ssk_.trip_id_) << ", stop " << pf.stopStringForId(linkset.lder_ssk_.stop_succpred_);
        }
        ostr << "; cost ";
        if (path_spec.hyperpath_) {
            ostr << linkset.hyperpath_cost_;
        }
        else {
            pf.printTimeDuration(ostr, linkset.hyperpath_cost_);
        }
        ostr << ")" << std::endl << "  ";
        Hyperlink::printStopStateHeader(ostr, path_spec);
        ostr << std::endl;
        for (CostToStopState::const_iterator iter = linkset.cost_map_.begin(); iter != linkset.cost_map_.end(); ++iter) {
            ostr << "  ";
            const StopStateKey& ssk = iter->second;
            Hyperlink::printStopState(ostr, stop_id, linkset.stop_state_map_.find(ssk)->second, path_spec, pf);
            ostr << std::endl;
        }
    }

    void Hyperlink::print(std::ostream& ostr, const PathSpecification& path_spec, const PathFinder& pf) const
    {
        if (linkset_trip_.cost_map_.size() == 0) {
            ostr << "   No trip links" << std::endl;
        } else {
            ostr << " Trip links";
            Hyperlink::printLinkSet(ostr, stop_id_, true, linkset_trip_, path_spec, pf);
        }

        if (linkset_nontrip_.cost_map_.size() == 0) {
            ostr << "   No non-trip links" << std::endl;
        } else {
            ostr << " Non-Trip links";
            Hyperlink::printLinkSet(ostr, stop_id_, false, linkset_nontrip_, path_spec, pf);
        }
    }

    // Go through stop states (links) and remove any outside the time window
    // Recalculates sum_exp_cost_ but not hyperpath_cost_
    void Hyperlink::pruneWindow(std::ostream& trace_file, const PathSpecification& path_spec, const PathFinder& pf, bool of_trip_links)
    {

        LinkSet& linkset = (of_trip_links ? linkset_trip_ : linkset_nontrip_);

        std::stack<StopStateKey> prune_keys;

        // recalculate this
        linkset.sum_exp_cost_ = 0;

        for (StopStateMap::const_iterator ssm_iter = linkset.stop_state_map_.begin(); ssm_iter != linkset.stop_state_map_.end(); ++ssm_iter)
        {
            const StopStateKey& ssk = ssm_iter->first;
            const StopState&    ss  = ssm_iter->second;

            if (( path_spec.outbound_ && (ss.deparr_time_ < linkset.latest_dep_earliest_arr_ - TIME_WINDOW_)) ||
                (!path_spec.outbound_ && (ss.deparr_time_ > linkset.latest_dep_earliest_arr_ + TIME_WINDOW_))) {
                prune_keys.push(ssk);
            } else {
                linkset.sum_exp_cost_ += exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            }
        }

        if (prune_keys.size() == 0) { return; }

        // window-pruning
        while (!prune_keys.empty()) {
            const StopStateKey& ssk = prune_keys.top();

            if (path_spec.trace_) {
                trace_file << "  + del ";
                printStopState(trace_file, stop_id_, linkset.stop_state_map_[ssk], path_spec, pf);
                trace_file << " (prune-window)" << std::endl;
            }

            removeFromCostMap(ssk, linkset.stop_state_map_[ssk]);
            if (linkset.stop_state_map_[ssk].low_cost_path_) {
                delete linkset.stop_state_map_[ssk].low_cost_path_;
                linkset.stop_state_map_[ssk].low_cost_path_ = NULL;
            }
            linkset.stop_state_map_.erase( ssk );
            prune_keys.pop();
        }

    }

    // Choose a link from this hyperlink based on the probabilities
    void Hyperlink::setupProbabilities(const PathSpecification& path_spec, std::ostream& trace_file,
                                       const PathFinder& pf, std::vector<ProbabilityStopState>& probabilities,
                                       const StopState* prev_link, const int last_trip_id) const
    {
        const LinkSet& linkset = (prev_link && !isTrip(prev_link->deparr_mode_) ? linkset_trip_ : linkset_nontrip_);

        static int COST_CUTOFF = 1;

        // Build a vector of probabilities in order of the costmap iteration
        if (path_spec.trace_) { Hyperlink::printStopStateHeader(trace_file, path_spec);  trace_file << std::endl; }

        double sum_exp     = 0;

        // Setup the probabilities
        for (CostToStopState::const_iterator iter = linkset.cost_map_.begin(); iter != linkset.cost_map_.end(); ++iter)
        {
            const StopStateKey& ssk   = iter->second;
            const StopState&     ss   = linkset.stop_state_map_.find(ssk)->second;
            ProbabilityStopState pss  = { 0.0, 0, ssk };

            // some checks if we have a previous link -- this will be a two-pass :p
            if (prev_link != NULL)
            {
                // these are ready to log
                if (path_spec.trace_) {
                    trace_file << "checking: ";
                    Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
                    trace_file << std::endl;
                }

                // outbound: we cannot depart before we arrive
                if ( path_spec.outbound_ && ss.deparr_time_ < prev_link->arrdep_time_) { continue; }
                // inbound: we cannot arrive after we depart
                if (!path_spec.outbound_ && ss.deparr_time_ > prev_link->arrdep_time_) { continue; }

                // don't repeat the same trip
                if (isTrip(ss.deparr_mode_) && (ss.trip_id_ == last_trip_id)) { continue; }

                // calculating denominator
                sum_exp += exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            }
            else
            {
                // we have no additional information so we trust the hyperpath cost and can go ahead
                pss.probability_ = exp(-1.0*STOCH_DISPERSION_*ss.cost_) /
                                   exp(-1.0*STOCH_DISPERSION_*linkset.hyperpath_cost_);
                pss.prob_i_      = static_cast<int>(RAND_MAX*pss.probability_);

                // too small to consider
                if (pss.prob_i_ < COST_CUTOFF) { continue; }

                // make prob_i_ cumulative
                if (probabilities.size() != 0) { pss.prob_i_ += probabilities.back().prob_i_; }

                // these are ready to log
                if (path_spec.trace_) {
                    Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
                    trace_file << " prob "     << std::setw(6) << pss.probability_;
                    trace_file << " cum_prob " << std::setw(6) << pss.prob_i_;
                    trace_file << std::endl;
                }
            }

            probabilities.push_back(pss);
        }
        // fail
        if (probabilities.size() == 0) { return; }

        // this set is ready
        if (prev_link == NULL) { return; }

        // the two-pass version -- this could fail too
        if (sum_exp == 0) { probabilities.clear(); return; }

        // fix up the probabilities
        for (int idx = 0; idx < probabilities.size(); ++idx)
        {
            const StopState& ss = linkset.stop_state_map_.find(probabilities[idx].ssk_)->second;
            probabilities[idx].probability_ = exp(-1.0*STOCH_DISPERSION_*ss.cost_) / sum_exp;
            probabilities[idx].prob_i_      = static_cast<int>(RAND_MAX*probabilities[idx].probability_);

            // make it cumulative
            if (idx > 0) { probabilities[idx].prob_i_ += probabilities[idx-1].prob_i_; }

            // ready to log
            if (path_spec.trace_) {
                Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
                trace_file << " prob "     << std::setw(6) << probabilities[idx].probability_;
                trace_file << " cum_prob " << std::setw(6) << probabilities[idx].prob_i_;
                trace_file << std::endl;
            }
        } // finish second pass
    }

    const StopState& Hyperlink::chooseState(
        const PathSpecification& path_spec,
        std::ostream& trace_file,
        const std::vector<ProbabilityStopState>& prob_stops,
        const StopState* prev_link) const
    {
        const LinkSet& linkset = (prev_link && !isTrip(prev_link->deparr_mode_) ? linkset_trip_ : linkset_nontrip_);

        int random_num = rand();
        if (path_spec.trace_) { trace_file << "random_num " << random_num << " -> "; }

        // mod it by max prob
        random_num = random_num % (prob_stops.back().prob_i_);
        if (path_spec.trace_) { trace_file << random_num << std::endl; }

        for (size_t ind = 0; ind < prob_stops.size(); ++ind)
        {
            if (prob_stops[ind].prob_i_==0) { continue; }
            if (random_num <= prob_stops[ind].prob_i_) { return linkset.stop_state_map_.find(prob_stops[ind].ssk_)->second; }
        }
        // shouldn't get here
        printf("PathFinder::chooseState() This should never happen!\n");
        return linkset.stop_state_map_.begin()->second;
    }
}
