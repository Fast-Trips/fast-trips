/**
 * \file StopStates.h
 *
 * Defines the structure to hold the stop states for a stop.
 */

#include <map>

namespace fasttrips {

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

    /**
     * The path finding algorithm stores StopState data in this structure.
     * For the stochastic algorithm, a stop ID maps to a vector of StopState instances.
     * For the deterministic algorithm, the vector only has a single instance of StopState.
     */
    typedef std::map<int, std::vector<StopState> > StopStates;

}