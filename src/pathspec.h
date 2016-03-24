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
        int     passenger_id_;          ///< The passenger ID
        int     path_id_;               ///< The path ID - uniquely identifies a passenger+path
        bool    hyperpath_;             ///< If true, find path using stochastic algorithm
        int     origin_taz_id_;         ///< Origin of path
        int     destination_taz_id_;    ///< Destination of path
        bool    outbound_;              ///< If true, the preferred time is for arrival, otherwise it's departure
        double  preferred_time_;        ///< Preferred time of arrival or departure, minutes after midnight
        bool    trace_;                 ///< If true, log copious details of the pathfinding into a trace log
        std::string user_class_;        ///< User class string
        std::string access_mode_;       ///< Access demand mode
        std::string transit_mode_;      ///< Transit demand mode
        std::string egress_mode_;       ///< Egress demand mode
    } PathSpecification;
}

#endif