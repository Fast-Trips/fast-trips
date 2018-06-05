/**
 * \file pathspec.h
 *
 * Defines the specification for a path.
 */

#ifndef PATHSPEC_H
#define PATHSPEC_H

namespace fasttrips {

    /**
     * The definition of the path we're trying to find.
     */
    typedef struct {
        int     iteration_;             ///< Iteration
        int     pathfinding_iteration_; ///< Pathfinding iteration
        bool    hyperpath_;             ///< If true, find path using stochastic algorithm
        int     origin_taz_id_;         ///< Origin of path
        int     destination_taz_id_;    ///< Destination of path
        bool    outbound_;              ///< If true, the preferred time is for arrival, otherwise it's departure
        double  preferred_time_;        ///< Preferred time of arrival or departure, minutes after midnight
        double  value_of_time_;         ///< Value of time, in currency_type/hour
        bool    trace_;                 ///< If true, log copious details of the pathfinding into a trace log
        std::string person_id_;         ///< Person ID
        std::string person_trip_id_;    ///< Person Trip ID
        std::string user_class_;        ///< User class string
        std::string purpose_;           ///< Purpose string
        std::string access_mode_;       ///< Access demand mode
        std::string transit_mode_;      ///< Transit demand mode
        std::string egress_mode_;       ///< Egress demand mode
    } PathSpecification;

    /**
     * The pathfinding algorithm is a labeling algorithm which associates each stop with a state (or link), encapsulated
     * here.  If the sought path is outbound, then the preferred time is an arrival time
     * at the destination, so the labeling algorithm starts at the destination and works backwards.  If the sought
     * path is inbound, then the preferred time is a departure time from the origin, so the labeling algorithm starts
     * at the origin and works forwards.  Thus, the attributes here have different meanings if the path sought is
     * outbound versus inbound, and the convention is outbound/inbound for the variable names.
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

    // forward dec
    class Path;
    struct FarePeriod;

    /** Stop states are basically links in a hyperpath.
      * Note that the time fields (deparr_time_ and arrdep_time_) are based around the preferred arrival or departure time
      * and can be negative or over 24*60 if the travel crosses the midnight boundary.
      * For example, if the preferred arrival time is 12:10a then links before it might have negative values.
      **/
    struct StopState {
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
        double  link_fare_;             ///< Link fare. Financial cost of the link.
        double  link_cost_;             ///< Link generalized cost.
        double  link_ivtwt_;            ///< Link in-vehicle time path weight.
        double  link_dist_;             ///< Link distance, in units of shape_dist_traveled.
        double  cost_;                  ///< Cost from previous link(s) and this link together.
        int     iteration_;             ///< Labeling iteration that generated this stop state.
        double  arrdep_time_;           ///< Arrival time for outbound, departure time for inbound

        const FarePeriod* fare_period_; ///< Trip links may have a FarePeriod

        // previously in ProbabilityStopState
        double  probability_;           ///< The probability of this link
        int     cum_prob_i_;            ///< Cumulative integer version of probability


        Path*   low_cost_path_;         ///< Lowest cost path that includes this link.  Only set in labeling.

        StopState() :
            deparr_time_  (0),
            deparr_mode_  (0),
            trip_id_      (0),
            stop_succpred_(0),
            seq_          (0),
            seq_succpred_ (0),
            link_time_    (0),
            link_fare_    (0),
            link_cost_    (0),
			link_ivtwt_   (0),
            link_dist_    (0),
            cost_         (0),
            iteration_    (-1),
            arrdep_time_  (0),
            fare_period_  (NULL),
            probability_  (0),
            cum_prob_i_   (0),
            low_cost_path_(NULL) {}

        StopState(
            double deparr_time,
            int    deparr_mode,
            int    trip_id,
            int    stop_succpred,
            int    seq,
            int    seq_succpred,
            double link_time,
            double link_fare,
            double link_cost,
            double link_dist,
            double cost,
            int    iteration,
            double arrdep_time,
			double link_ivtwt,
            const FarePeriod* fp=NULL) :
            deparr_time_  (deparr_time),
            deparr_mode_  (deparr_mode),
            trip_id_      (trip_id),
            stop_succpred_(stop_succpred),
            seq_          (seq),
            seq_succpred_ (seq_succpred),
            link_time_    (link_time),
            link_fare_    (link_fare),
            link_cost_    (link_cost),
			link_ivtwt_   (link_ivtwt),
            link_dist_    (link_dist),
            cost_         (cost),
            iteration_    (iteration),
            arrdep_time_  (arrdep_time),
            fare_period_  (fp),
            probability_  (0),
            cum_prob_i_   (0),
            low_cost_path_(NULL) {}
    };
}

#endif
