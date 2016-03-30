/**
 * \file StopStates.h
 *
 * Defines the structure to hold the stop states for a stop.
 */
#include <iostream>
#include <map>
#include <set>
#include <vector>

#include "pathspec.h"

#ifndef HYPERLINK_H
#define HYPERLINK_H

namespace fasttrips {

    enum DemandModeType {
        MODE_ACCESS   = -100,
        MODE_EGRESS   = -101,
        MODE_TRANSFER = -102,
        MODE_TRANSIT  = -103,
    };

    /**
     * The pathfinding algorithm is a labeling algorithm which associates each stop with a state, encapsulated
     * here.  See StopStates for more.  If the sought path is outbound, then the preferred time is an arrival time
     * at the destination, so the labeling algorithm starts at the destination and works backwards.  If the sought
     * path is inbound, then the preferred time is a departure time from the origin, so the labeling algorithm starts
     * at the origin and works forwards.  Thus, the attributes here have different meanings if the path sought is
     * outbound versus inbound, and the convention is outbound/inbound fo the variable names.
     *
     * The StopState is basically the state at this stop with details of the link after (for outbound) or before
     * (inbound) the stop in the found path.
     *
     * NOTE: for trip states, deparr_time_ and arrdep_time_ are both for the *vehicle* because the passenger
     * times can be inferred from the surrounding states.
     *
     * In particular, for outbound trips, the deparr_time_ for trip states is not necessarily the person departure time.
     * For inbound trips, the arrdep_time_ for trip states is not necessarily the person departure time.
     */
    struct StopStateKey {
        int     deparr_mode_;           ///< Departure mode for outbound, arrival mode for inbound.
                                        ///< One of fasttrips::MODE_ACCESS, fasttrips::MODE_EGRESS,
                                        ///< fasttrips::MODE_TRANSFER, or fasttrips::MODE_TRANSIT
        int     trip_id_;               ///< Trip ID if deparr_mode_ is fasttrips::MODE_TRANSIT,
                                        ///< or the supply_mode_num for access, egress
        int     stop_succpred_;         ///< Successor stop for outbound, predecessor stop for inbound
        int     seq_;                   ///< The sequence number of this stop on this trip. (-1 if not trip)
        int     seq_succpred_;          ///< The sequence number of the successor/predecessor stop

        bool operator ==(const StopStateKey& rhs) const
        {
            return (rhs.deparr_mode_    == deparr_mode_   ) &&
                   (rhs.trip_id_        == trip_id_       ) &&
                   (rhs.stop_succpred_  == stop_succpred_ ) &&
                   (rhs.seq_            == seq_           ) &&
                   (rhs.seq_succpred_   == seq_succpred_  );
        }
        bool operator !=(const StopStateKey& rhs) const
        {
            return (rhs.deparr_mode_    != deparr_mode_   ) ||
                   (rhs.trip_id_        != trip_id_       ) ||
                   (rhs.stop_succpred_  != stop_succpred_ ) ||
                   (rhs.seq_            != seq_           ) ||
                   (rhs.seq_succpred_   != seq_succpred_  );
        }
        bool operator<(const StopStateKey& rhs) const
        {
            if (deparr_mode_   < rhs.deparr_mode_  ) { return true;  }
            if (deparr_mode_   > rhs.deparr_mode_  ) { return false; }
            if (trip_id_       < rhs.trip_id_      ) { return true;  }
            if (trip_id_       > rhs.trip_id_      ) { return false; }
            if (stop_succpred_ < rhs.stop_succpred_) { return true;  }
            if (stop_succpred_ > rhs.stop_succpred_) { return false; }
            if (seq_           < rhs.seq_          ) { return true;  }
            if (seq_           > rhs.seq_          ) { return false; }
            if (seq_succpred_  < rhs.seq_succpred_ ) { return true;  }
            if (seq_succpred_  > rhs.seq_succpred_ ) { return false; }
            return false;
        }
    };

    typedef struct {
        double  deparr_time_;           ///< Departure time for outbound, arrival time for inbound
        int     deparr_mode_;           ///< Departure mode for outbound, arrival mode for inbound.
                                        ///< One of fasttrips::MODE_ACCESS, fasttrips::MODE_EGRESS,
                                        ///< fasttrips::MODE_TRANSFER, or fasttrips::MODE_TRANSIT
        int     trip_id_;               ///< Trip ID if deparr_mode_ is fasttrips::MODE_TRANSIT,
                                        ///< or the supply_mode_num for access, egress
        int     stop_succpred_;         ///< Successor stop for outbound, predecessor stop for inbound
        int     seq_;                   ///< The sequence number of this stop on this trip. (-1 if not trip)
        int     seq_succpred_;          ///< The sequence number of the successor/predecessor stop
        double  link_time_;             ///< Link time.  For trips, includes wait time. Just walk time for others.
        double  link_cost_;             ///< Link cost.
        double  cost_;                  ///< Cost from previous link(s) and this link together.
        int     iteration_;             ///< Labeling iteration that generated this stop state.
        double  arrdep_time_;           ///< Arrival time for outbound, departure time for inbound
    } StopState;

    /// Structure used in PathFinder::hyperpathChoosePath
    typedef struct {
        double         probability_;   ///< Probability of this stop
        int            prob_i_;        ///< Cumulative probability * 1000
        StopStateKey   ssk_;           ///< Pointer to relevant stop state
    } ProbabilityStopState;

    typedef std::map<StopStateKey, StopState> StopStateMap;
    // cost to stop state key
    typedef std::multimap< double, StopStateKey> CostToStopState;

    struct LinkSet {
        double latest_dep_earliest_arr_;   ///< latest departure time from this stop for outbound trips, earliest arrival time to this stop for inbound trips
        int    lder_trip_id_;              ///< trip for the latest departure/earliest arrival
        double sum_exp_cost_;              ///< sum of the exponentiated cost
        double hyperpath_cost_;            ///< hyperpath cost for this stop state

        StopStateMap stop_state_map_;      ///< set of stop states where compare means the key is unique
        CostToStopState cost_map_;         ///< multimap of cost -> stop state pointers into the stop_state_set_ above

        LinkSet() : latest_dep_earliest_arr_(0), lder_trip_id_(0), sum_exp_cost_(0), hyperpath_cost_(0) {}
    } ;

    class PathFinder;

    /**
     * Class that encapulates the link (for deterministic) or hyperlink (for stochastic)
     * to (for inbound) or from (for outbound) a stop.
     *
     * For deterministic path-finding, a hyperlink is just one link.
     * For stochastic path-finding, a hyperlink is comprised of a set of links
     * that depart (for outbound) or arrive (for inbound) from a single stop
     * within a time window.
     *
     **/
    class Hyperlink {

    private:
        /// For outbound, originating stop; for inbound, destination stop.
        int stop_id_;
        /// increment this every time the stop is processed
        int process_count_;

        LinkSet linkset_;

        /// Remove the given stop state from cost_map_
        void removeFromCostMap(const StopStateKey& ssk, const StopState& ss);

    public:
        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.TIME_WINDOW">fasttrips.Assignment.TIME_WINDOW</a>
        /// This could be configured per stop in the future.
        static double TIME_WINDOW_;
        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.STOCH_DISPERSION">fasttrips.Assignment.STOCH_DISPERSION</a>
        static double STOCH_DISPERSION_;

        /// Default constructor
        Hyperlink();
        /// Constructor we should call
        Hyperlink(int stop_id);
        /// Destructor
        ~Hyperlink() {}

        /// How many links make up the hyperlink?
        size_t size() const { return linkset_.stop_state_map_.size(); }

        /// Add this link to the hyperlink.
        /// For deterministic: we only keep one link.  Accept it iff the cost is lower.
        /// For stochastic:
        /// - If it's outside the time window, reject it.
        /// - If it's already here according to the key, then replace the state.
        /// - Return true iff the hyperlink state was affected (e.g. the stop needs to be re-processed)
        bool addLink(const StopState& ss, bool& rejected,
                     std::ostream& trace_file, const PathSpecification& path_spec, const PathFinder& pf);

        /// Clears data
        void clear();

        /// Returns the lowest cost stop state (link) in this hyperlink
        const StopState& lowestCostStopState() const;

        /// Returns the earliest departure (outbound) or latest arrival (inbound) of the links that make up this hyperlink
        double earliestDepartureLatestArrival(bool outbound) const;

        /// Returns the trip id for the latest departure (outbound) or earliest arrival (inbound) trip
        double latestDepartureEarliestArrival() const;
        /// Returns the trip id for the latest departure (outbound) or earliest arrival (inbound) trip
        int latestDepartingEarliestArrivingTripID() const;
        /// Calculate the cost of just the non-walk links that make up this hyperlink
        double calculateNonwalkLabel() const;

        /// Accessor for the process count
        int processCount() const { return process_count_; }
        /// Increment process count
        void incrementProcessCount() { process_count_ += 1; }
        /// Accessor for the hyperlink cost
        double hyperpathCost() const { return linkset_.hyperpath_cost_; }

        /// Print the stop state header.  For printing stop states in table form.
        static void printStopStateHeader(std::ostream& ostr, const PathSpecification& path_spec);
        /// Print the given stop state.
        static void printStopState(std::ostream& ostr, int stop_id, const StopState& ss, const PathSpecification& path_spec, const PathFinder& pf);
        /// Print the hyperlink, including a header and the stop states (links) that make it up.
        void print(std::ostream& ostr, const PathSpecification& path_spec, const PathFinder& pf) const;

        /// Go through stop states (links) and remove any outside the time window
        void pruneWindow(std::ostream& trace_file, const PathSpecification& path_spec, const PathFinder& pf);

        /// Setup probabilities for hyperlink's stop states (links)
        void setupProbabilities(const PathSpecification& path_spec, std::ostream& trace_file,
                                const PathFinder& pf, std::vector<ProbabilityStopState>& probabilities,
                                const StopState* prev_link = NULL) const;

        /**
         * Given a vector of fasttrips::ProbabilityStop instances,
         * randomly selects one based on the cumulative probability
         * (fasttrips::ProbabilityStop.prob_i_)
         *
         * @return the index_ from chosen ProbabilityStop.
         */
        const StopState& chooseState(const PathSpecification& path_spec,
                                     std::ofstream& trace_file,
                                     const std::vector<ProbabilityStopState>& prob_stops) const;
    };

    /**
     * The path finding algorithm stores StopState data in this structure.
     * For the stochastic algorithm, a stop ID maps to a vector of StopState instances.
     * For the deterministic algorithm, the vector only has a single instance of StopState.
     */
    typedef std::map<int, Hyperlink> StopStates;

}

#endif