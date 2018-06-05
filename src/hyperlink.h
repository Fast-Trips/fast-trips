/**
 * \file hyperlink.h
 *
 * Defines the Hyperlink class that holds the links (stop states) for a stop.
 */
#include <iostream>
#include <map>
#include <set>
#include <vector>

#include "pathspec.h"
#include "path.h"

#ifndef HYPERLINK_H
#define HYPERLINK_H

namespace fasttrips {

    enum DemandModeType {
        MODE_ACCESS   = -100,
        MODE_EGRESS   = -101,
        MODE_TRANSFER = -102,
        MODE_TRANSIT  = -103,
        MODE_UNSET    =    0,
    };

    /// Hyperpath cost when no links are there
    const double MAX_COST = 999999;

    /// What we multiply the fractional cumulative probabilities by to get an integer to compare with random numbers
    /// Must be less than, but close to RAND_MAX for the system, which is 32767 on windows
    const double INT_MULT = 10000;

    // Commenting MIN_COST out for now. If gencost is calculated in terms of IVT mins, negative costs unlikely.
    // This also creates an issue with probabilities not adding up to 1.
    /// Hyperpath minimum cost (zero and negative costs are problematic)
    //const double MIN_COST = 0.001;

    bool isTrip(const int& mode);

    typedef std::map<StopStateKey, StopState> StopStateMap;
    // cost to stop state key
    typedef std::multimap< double, StopStateKey> CostToStopState;

    struct LinkSet {
        double          latest_dep_earliest_arr_;  ///< latest departure time from this stop for outbound trips, earliest arrival time to this stop for inbound trips
        StopStateKey    lder_ssk_;                 ///< trip for the latest departure/earliest arrival
        double          sum_exp_cost_;             ///< sum of the exponentiated cost
        double          hyperpath_cost_;           ///< hyperpath cost for this stop state
        int             process_count_;            ///< increment this every time the stop is processed
        int             max_cum_prob_i_;           ///< Set by setupProbabilities()

        StopStateMap    stop_state_map_;           ///< the links.  (or a set of stop states where compare means the key is unique)
        CostToStopState cost_map_;                 ///< multimap of cost -> stop state pointers into the stop_state_set_ above

        LinkSet(bool outbound) : latest_dep_earliest_arr_(0), sum_exp_cost_(0), hyperpath_cost_(MAX_COST), process_count_(0) {}
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

        /// link set with trip links
        LinkSet linkset_trip_;
        /// link set with non-trip link
        LinkSet linkset_nontrip_;

        /// Remove the given stop state from cost_map_
        void removeFromCostMap(const StopStateKey& ssk, const StopState& ss);

        /// Reset latest departure/earliest arrival
        void resetLatestDepartureEarliestArrival(bool of_trip_links, const PathSpecification& path_spec);

        /// Update the low cost path for this stop state
        void updateLowCostPath(const StopStateKey& ssk, const Hyperlink* prev_link,
                               std::ostream& trace_file, const PathSpecification& path_spec, const PathFinder& pf);

    public:

        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.TIME_WINDOW">fasttrips.Assignment.TIME_WINDOW</a>
        /// This could be configured per stop in the future.
        static double TIME_WINDOW_;
        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.UTILS_CONVERSION">fasttrips.Assignment.UTILS_CONVERSION</a>
        static double UTILS_CONVERSION_;
        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.STOCH_DISPERSION">fasttrips.Assignment.STOCH_DISPERSION</a>
        static double STOCH_DISPERSION_;
        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.TRANSFER_FARE_IGNORE_PATHFINDING">fasttrips.Assignment.TRANSFER_FARE_IGNORE_PATHFINDING</a>
        static bool TRANSFER_FARE_IGNORE_PATHFINDING_;
        /// See <a href="_generated/fasttrips.Assignment.html#fasttrips.Assignment.TRANSFER_FARE_IGNORE_PATHENUM">fasttrips.Assignment.TRANSFER_FARE_IGNORE_PATHENUM</a>
        static bool TRANSFER_FARE_IGNORE_PATHENUM_;

        /// Default constructor
        Hyperlink();
        /// Constructor we should call
        Hyperlink(int stop_id, bool outbound);
        /// Destructor
        ~Hyperlink();

        /// How many links make up the hyperlink?
        size_t size() const;
        /// How many links make up the trip/nontrip hyperlink
        size_t size(bool of_trip_links) const;

        /// Accessor for stop state map
        const StopStateMap& getStopStateMap(bool of_trip_links) const;
        /// Accessor for the low cost path
        const Path* getLowCostPath(bool of_trip_links) const;

        /// Add this link to the hyperlink.
        /// For deterministic: we only keep one link.  Accept it iff the cost is lower.
        /// For stochastic:
        /// - If it's outside the time window, reject it.
        /// - If it's already here according to the key, then replace the state.
        /// - Return true iff the hyperlink state was affected (e.g. the stop needs to be re-processed)
        bool addLink(const StopState& ss, const Hyperlink* prev_link, bool& rejected,
                     std::ostream& trace_file, const PathSpecification& path_spec, const PathFinder& pf);

        /// Clears data
        void clear(bool of_trip_links);

        /// Returns the lowest cost stop state (link) in this hyperlink
        /// If for_trip_link, lowest trip link. Otherwise, lowest non-trip link.
        const StopState& lowestCostStopState(bool of_trip_links) const;
        /// Given an arrival time into this hyperlink (outbound) or a departure time out of this hyperlink (inbound),
        /// returns the best guess link
        const StopState& bestGuessLink(bool outbound, double arrdep_time) const;
        /// Given an arrival link into this hyperlink (outbound) or a departure time out of this hyperlink (inbound),
        /// returns the best guess cost.  Time consuming but more accurate.  Make it an option?
        /// This isn't currently being used.  Initial tests didn't show it helping things.  TODO: remove?
        double bestGuessCost(bool outbound, double arrdep_time) const;

        /// Returns the earliest departure (outbound) or latest arrival (inbound) of the links that make up this hyperlink
        double earliestDepartureLatestArrival(bool outbound, bool of_trip_links=true) const;

        /// Returns the trip id for the latest departure (outbound) or earliest arrival (inbound) trip
        double latestDepartureEarliestArrival(bool of_trip_links) const;
        /// Calculate the cost of just the non-walk links that make up this hyperlink
        double calculateNonwalkLabel() const;

        /// Accessor for the process count
        int processCount(bool of_trip_links) const;
        /// Increment process count
        void incrementProcessCount(bool of_trip_links);
        /// Accessor for the hyperlink cost
        double hyperpathCost(bool of_trip_links) const;

        /// Print the stop state header.  For printing stop states in table form.
        static void printStopStateHeader(std::ostream& ostr, const PathSpecification& path_spec);
        /// Print the given stop state.
        static void printStopState(std::ostream& ostr, int stop_id, const StopState& ss, const PathSpecification& path_spec, const PathFinder& pf);
        /// Print the given Link Set
        static void printLinkSet(std::ostream& ostr, int stop_id, bool is_trip, const LinkSet& linkset, const PathSpecification& path_spec, const PathFinder& pf);

        /// Print the hyperlink, including a header and the stop states (links) that make it up.
        void print(std::ostream& ostr, const PathSpecification& path_spec, const PathFinder& pf) const;

        /// Go through stop states (links) and remove any outside the time window
        void pruneWindow(std::ostream& trace_file, const PathSpecification& path_spec, const PathFinder& pf, bool of_trip_links);

        /**
         * Setup probabilities for hyperlink's stop states (links)
         * Return the max cum probability for the linkset
         */
        int setupProbabilities(const PathSpecification& path_spec, std::ostream& trace_file,
                                 const PathFinder& pf, bool trip_linkset,
                                 const Path* path_so_far = NULL);

        /**
         * Randomly selects one of the links in this hyperlink based on the cumulative probability
         * set by Hyperlink::setupProbabilities()
         *
         * @return a const reference to the chosen StopState.
         */
        const StopState& chooseState(const PathSpecification& path_spec,
                                     std::ostream& trace_file,
                                     const StopState* prev_link = NULL) const;

        /**
         * Iterates through the trip links and multiplies transer_probability x probability and sums
         * to the fare period in fp_probs.
         */
        void collectFarePeriodProbabilities(const PathSpecification& path_spec,
                                            std::ostream& trace_file,
                                            const PathFinder& pf,
                                            double transfer_probability,
                                            std::map<const FarePeriod*, double>& fp_probs) const;

        void updateFare(const PathSpecification& path_spec,
                        std::ostream& trace_file,
                        const PathFinder& pf,
                        const FarePeriod* last_trip_fp,
                        bool  last_is_prev,                 ///< is last_trip_fp the previous trip, chronologically?
                        const Path& path_so_far,
                        StopState& ss,
                        std::string& trace_xfer_type) const;

        /**
         * Estimate the fare during pathfinding given transfer possibilities.
         * This is called on the previous (transfer) link by PathFinder::updateStopStatesForTrips().
         */
        double getFareWithTransfer(const PathSpecification& path_spec,
                                   std::ostream& trace_file,
                                   const PathFinder& pf,
                                   const FarePeriod& fare_period,
                                   const std::map<int, Hyperlink>& stop_states) const;

    };

    /**
     * The path finding algorithm stores StopState data in this structure.
     */
    typedef std::map<int, Hyperlink> StopStates;

}

#endif
