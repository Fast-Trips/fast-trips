/**
 * \file StopStates.cpp
 *
 * StopStates implementation
 **/

#include "hyperlink.h"
#include "pathfinder.h"

#include <math.h>
#include <ios>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <stack>

namespace fasttrips {

    double Hyperlink::TIME_WINDOW_      = 0.0;
    double Hyperlink::STOCH_DISPERSION_ = 0.0;

    // Default constructor
    Hyperlink::Hyperlink() :
        stop_id_(0),
        sum_exp_cost_(0.0),
        hyperpath_cost_(0.0),
        latest_dep_earliest_arr_(0.0),
        lder_trip_id_(0),
        process_count_(0)
    {}

    // Constructor we should call
    Hyperlink::Hyperlink(int stop_id) :
        stop_id_(stop_id),
        sum_exp_cost_(0.0),
        hyperpath_cost_(0.0),
        latest_dep_earliest_arr_(0.0),
        lder_trip_id_(0),
        process_count_(0)
    {}

    // Remove the given stop state from cost_map_
    void Hyperlink::removeFromCostMap(const StopStateKey& ssk, const StopState& ss)
    {
        // todo: switch this to cost_
        std::pair<CostToStopState::iterator, CostToStopState::iterator> iter_range = cost_map_.equal_range(ss.cost_);
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
        cost_map_.erase(cm_iter);
    }

    bool Hyperlink::addLink(const StopState& ss, bool& rejected,
                            std::ostream& trace_file, const PathSpecification& path_spec, const PathFinder& pf)
    {
        rejected = false;
        const StopStateKey ssk = { ss.deparr_mode_, ss.trip_id_, ss.stop_succpred_, ss.seq_, ss.seq_succpred_ };

        // deterministic -- we only keep one, the low cost link
        if (path_spec.hyperpath_ == false && stop_state_map_.size() > 0)
        {
            // if the cost isn't better, reject
            if (ss.cost_ >= lowestCostStopState().cost_) {
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
            clear();
            // fall through to add it below
        }
        // simplest case -- we have no stop states/links, so just add it
        if (stop_state_map_.size() == 0)
        {
            latest_dep_earliest_arr_ = ss.deparr_time_;
            lder_trip_id_            = ss.trip_id_;
            sum_exp_cost_            = exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            hyperpath_cost_          = ss.cost_;

            // add to the map
            stop_state_map_[ssk] = ss;

            // assume success
            cost_map_.insert (std::pair<double, StopStateKey>(ss.cost_,ssk));

            // log it
            if (path_spec.trace_) {
                trace_file << "  + new ";
                Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
                trace_file << std::endl;
            }

            return true;
        }
        // ========= now we have links in the hyperlink =========

        // is it too early (outbound) or too late (inbound)?
        // don't worry about the last labeling (access for outbound, egress for inbound) -- that one is special
        if (( path_spec.outbound_ && (ss.deparr_mode_ != MODE_ACCESS) && (ss.deparr_time_ < latest_dep_earliest_arr_ - TIME_WINDOW_)) ||
            (!path_spec.outbound_ && (ss.deparr_mode_ != MODE_EGRESS) && (ss.deparr_time_ > latest_dep_earliest_arr_ + TIME_WINDOW_))) {
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
        std::pair<StopStateMap::iterator, bool> result_l = stop_state_map_.insert(std::pair<StopStateKey,StopState>(ssk,ss));

        // if we succeeded, the key isn't in here already
        if (result_l.second == true) {
            std::string notes;

            cost_map_.insert (std::pair<double, StopStateKey>(ss.cost_,ssk));

            // check if the window is updated -- this is a state update
            if (( path_spec.outbound_ && (ss.deparr_time_ > latest_dep_earliest_arr_)) ||
                (!path_spec.outbound_ && (ss.deparr_time_ < latest_dep_earliest_arr_)))
            {
                latest_dep_earliest_arr_ = ss.deparr_time_;
                lder_trip_id_            = ss.trip_id_;
                update_state             = true;
                notes                   += " (window)";
                // if the window changes, we need to prune states out of bounds -- this recalculates sum_exp_cost_
                pruneWindow(trace_file, path_spec, pf);
            } else {
                sum_exp_cost_         += exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            }

            // check if the hyperpath cost is affected -- this would be a state update
            double hyperpath_cost  = (-1.0/STOCH_DISPERSION_)*log(sum_exp_cost_);
            if (abs(hyperpath_cost - hyperpath_cost_) > 0.0001)
            {
                std::ostringstream oss;
                oss << " (hp cost " << std::setprecision(6) << std::fixed << hyperpath_cost_ << "->" << hyperpath_cost << ")";
                notes                   += oss.str();
                update_state             = true;
                hyperpath_cost_          = hyperpath_cost;
            }

            // log it
            if (path_spec.trace_) {
                trace_file << "  + new ";
                Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
                trace_file << notes << std::endl;
            }

            return update_state;
        }

        // ========= the key is in already in here so replace the values =========
        std::string notes(" (sub)");

        // todo: what if the the latest_dep_earliest_arr_/lder_trip_id_ were set to this before?  update?

        // update the cost map
        // todo: when we use cost, do this.  But we don't actually change the iteration order
        // int old_iteration = stop_state_map_[ssk].iteration_;
        removeFromCostMap(ssk, stop_state_map_[ssk]);
        cost_map_.insert (std::pair<double, StopStateKey>(ss.cost_,ssk));

        // update the cost
        sum_exp_cost_ -= exp(-1.0*STOCH_DISPERSION_*stop_state_map_[ssk].cost_);

        // and the other state elements
        stop_state_map_[ssk] = ss;
        // stop_state_map_[ssk].iteration_ = old_iteration; // remove this
        sum_exp_cost_ += exp(-1.0*STOCH_DISPERSION_*ss.cost_);

        // check if the window is updated -- this is a state update
        if (( path_spec.outbound_ && (ss.deparr_time_ > latest_dep_earliest_arr_)) ||
            (!path_spec.outbound_ && (ss.deparr_time_ < latest_dep_earliest_arr_)))
        {
            latest_dep_earliest_arr_ = ss.deparr_time_;
            lder_trip_id_            = ss.trip_id_;
            update_state             = true;
            notes                   += " (window)";
            // if the window changes, we need to prune states out of bounds -- this recalculates sum_exp_cost_
            pruneWindow(trace_file, path_spec, pf);
        }

        double hyperpath_cost  = (-1.0/STOCH_DISPERSION_)*log(sum_exp_cost_);
        if (abs(hyperpath_cost - hyperpath_cost_) > 0.0001)
        {
            std::ostringstream oss;
            oss << " (hp cost " << std::setprecision(6) << std::fixed << hyperpath_cost_ << "->" << hyperpath_cost << ")";
            notes                   += oss.str();
            update_state             = true;
            hyperpath_cost_          = hyperpath_cost;
        }

        // log it
        if (path_spec.trace_) {
            trace_file << "  + new ";
            Hyperlink::printStopState(trace_file, stop_id_, ss, path_spec, pf);
            trace_file << notes << std::endl;
        }
        return update_state;
    }

    void Hyperlink::clear()
    {
        cost_map_.clear();
        stop_state_map_.clear();
        sum_exp_cost_            = 0;
        hyperpath_cost_          = 0;
        latest_dep_earliest_arr_ = 0;
        lder_trip_id_            = 0;
        process_count_           = 0;
    }

    const StopState& Hyperlink::lowestCostStopState() const
    {
        const StopStateKey& ssk = cost_map_.begin()->second;
        return stop_state_map_.find(ssk)->second;
    }

    // Returns the earliest departure (outbound) or latest arrival (inbound) of the links that make up this hyperlink
    double Hyperlink::earliestDepartureLatestArrival(bool outbound) const
    {
        double earliest_dep_latest_arr = lowestCostStopState().deparr_time_;
        for (StopStateMap::const_iterator it = stop_state_map_.begin(); it != stop_state_map_.end(); ++it)
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
    double Hyperlink::latestDepartureEarliestArrival() const
    {
        return latest_dep_earliest_arr_;
    }

    // Returns the trip id for the latest departure (outbound) or earliest arrival (inbound) trip
    int Hyperlink::latestDepartingEarliestArrivingTripID() const
    {
        return lder_trip_id_;
    }

    // Calculate the cost of just the non-walk links that make up this hyperlink
    double Hyperlink::calculateNonwalkLabel() const
    {
        double nonwalk_label = 0.0;
        for (StopStateMap::const_iterator it = stop_state_map_.begin(); it != stop_state_map_.end(); ++it)
        {
            if ((it->first.deparr_mode_ != MODE_EGRESS  ) &&
                (it->first.deparr_mode_ != MODE_TRANSFER) &&
                (it->first.deparr_mode_ != MODE_ACCESS  ))
            {
                nonwalk_label += exp(-1.0*STOCH_DISPERSION_*it->second.cost_);
            }
        }

        if (nonwalk_label == 0.0) {
            return PathFinder::MAX_COST;
        }
        return -1.0/STOCH_DISPERSION_*log(nonwalk_label);
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
            ostr << std::setw(13) << std::setprecision(4) << std::fixed << std::setfill(' ') << ss.cost_;
        } else {
            // cost is a time duration
            ostr << "  ";
            pf.printTimeDuration(ostr, ss.link_cost_);
            ostr << "  ";
            pf.printTimeDuration(ostr, ss.cost_);
        }
        ostr << "  ";
        ostr << std::setw(7) << std::setfill(' ') << ss.iteration_;
        ostr << "  ";
        pf.printTime(ostr, ss.arrdep_time_);
    }

    void Hyperlink::print(std::ostream& ostr, const PathSpecification& path_spec, const PathFinder& pf) const
    {
        ostr << "  ";
        Hyperlink::printStopStateHeader(ostr, path_spec);
        ostr << std::endl;
        for (CostToStopState::const_iterator iter = cost_map_.begin(); iter != cost_map_.end(); ++iter) {
            ostr << "  ";
            const StopStateKey& ssk = iter->second;
            Hyperlink::printStopState(ostr, stop_id_, stop_state_map_.find(ssk)->second, path_spec, pf);
            ostr << std::endl;
        }
    }

    // Go through stop states (links) and remove any outside the time window
    // Recalculates sum_exp_cost_ but not hyperpath_cost_
    void Hyperlink::pruneWindow(std::ostream& trace_file, const PathSpecification& path_spec, const PathFinder& pf)
    {
        std::stack<StopStateKey> prune_keys;

        // recalculate this
        sum_exp_cost_ = 0;

        for (StopStateMap::const_iterator ssm_iter = stop_state_map_.begin(); ssm_iter != stop_state_map_.end(); ++ssm_iter)
        {
            const StopStateKey& ssk = ssm_iter->first;
            const StopState&    ss  = ssm_iter->second;

            if (( path_spec.outbound_ && (ss.deparr_time_ < latest_dep_earliest_arr_ - TIME_WINDOW_)) ||
                (!path_spec.outbound_ && (ss.deparr_time_ > latest_dep_earliest_arr_ + TIME_WINDOW_))) {
                prune_keys.push(ssk);
            } else {
                sum_exp_cost_ += exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            }
        }

        if (prune_keys.size() == 0) { return; }

        // window-pruning
        while (!prune_keys.empty()) {
            const StopStateKey& ssk = prune_keys.top();

            if (path_spec.trace_) {
                trace_file << "  + del ";
                printStopState(trace_file, stop_id_, stop_state_map_[ssk], path_spec, pf);
                trace_file << " (prune-window)" << std::endl;
            }

            removeFromCostMap(ssk, stop_state_map_[ssk]);
            stop_state_map_.erase( ssk );
            prune_keys.pop();
        }

    }

    // Choose a link from this hyperlink based on the probabilities
    void Hyperlink::setupProbabilities(const PathSpecification& path_spec, std::ostream& trace_file,
                                       const PathFinder& pf, std::vector<ProbabilityStopState>& probabilities,
                                       const StopState* prev_link) const
    {
        static int COST_CUTOFF = 1;

        // Build a vector of probabilities in order of the costmap iteration
        if (path_spec.trace_) { Hyperlink::printStopStateHeader(trace_file, path_spec);  trace_file << std::endl; }

        double sum_exp     = 0;

        // Setup the probabilities
        for (CostToStopState::const_iterator iter = cost_map_.begin(); iter != cost_map_.end(); ++iter)
        {
            const StopStateKey& ssk   = iter->second;
            const StopState&     ss   = stop_state_map_.find(ssk)->second;
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

                // no repeat of access/egress
                if ( path_spec.outbound_ && ssk.deparr_mode_ == MODE_ACCESS) { continue; }
                if (!path_spec.outbound_ && ssk.deparr_mode_ == MODE_EGRESS) { continue; }
                // no double walk
                if (path_spec.outbound_ &&
                    ((       ssk.deparr_mode_ == MODE_EGRESS) || (       ssk.deparr_mode_ == MODE_TRANSFER)) &&
                    ((prev_link->deparr_mode_ == MODE_ACCESS) || (prev_link->deparr_mode_ == MODE_TRANSFER))) { continue; }
                if (!path_spec.outbound_ &&
                    ((       ssk.deparr_mode_ == MODE_ACCESS) || (       ssk.deparr_mode_ == MODE_TRANSFER)) &&
                    ((prev_link->deparr_mode_ == MODE_EGRESS) || (prev_link->deparr_mode_ == MODE_TRANSFER))) { continue; }
                // don't double on the same trip ID - that's already covered by a single trip
                if (ssk.deparr_mode_ == MODE_TRANSIT && ssk.trip_id_ == prev_link->trip_id_) { continue; }

                // outbound: we cannot depart before we arrive
                if ( path_spec.outbound_ && ss.deparr_time_ < prev_link->arrdep_time_) { continue; }
                // inbound: we cannot arrive after we depart
                if (!path_spec.outbound_ && ss.deparr_time_ > prev_link->arrdep_time_) { continue; }

                // calculating denominator
                sum_exp += exp(-1.0*STOCH_DISPERSION_*ss.cost_);
            }
            else
            {
                // we have no additional information so we trust the hyperpath cost and can go ahead
                pss.probability_ = exp(-1.0*STOCH_DISPERSION_*ss.cost_) /
                                   exp(-1.0*STOCH_DISPERSION_*hyperpath_cost_);
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
            const StopState& ss = stop_state_map_.find(probabilities[idx].ssk_)->second;
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
        std::ofstream& trace_file,
        const std::vector<ProbabilityStopState>& prob_stops) const
    {
        int random_num = rand();
        if (path_spec.trace_) { trace_file << "random_num " << random_num << " -> "; }

        // mod it by max prob
        random_num = random_num % (prob_stops.back().prob_i_);
        if (path_spec.trace_) { trace_file << random_num << std::endl; }

        for (size_t ind = 0; ind < prob_stops.size(); ++ind)
        {
            if (prob_stops[ind].prob_i_==0) { continue; }
            if (random_num <= prob_stops[ind].prob_i_) { return stop_state_map_.find(prob_stops[ind].ssk_)->second; }
        }
        // shouldn't get here
        printf("PathFinder::chooseState() This should never happen!\n");
        return stop_state_map_.begin()->second;
    }
}