/**
 * \file path.h
 *
 * Defines Path class.
 */
#include <iostream>
#include <vector>

#include "hyperlink.h"

#ifndef PATH_H
#define PATH_H

namespace fasttrips {


    /** In stochastic path finding, this is the information we'll collect about the path. */
    typedef struct {
        int     count_;             ///< Number of times this path was generated (for stochastic)
        double  cost_;              ///< Cost of this path.                       TODO: move to Path?
        bool    capacity_problem_;  ///< Does this path have a capacity problem?  TODO: move to Path?
        double  probability_;       ///< Probability of this stop          (for stochastic)
        int     prob_i_;            ///< Cumulative probability * RAND_MAX (for stochastic)
    } PathInfo;

    // Forward declaration
    class PathFinder;

    /**
     * This class represents a concrete path.
     **/
    class Path
    {
    private:
        /// is this path in chronological order?
        bool chrono_order_;

        /// The links that make up this path (stop id, stop states)
        /// They are in origin to destination order for outbound trips,
        /// and destination to origin order for inbound trips.
        std::vector< std::pair<int, StopState> > links_;

    public:
        /// Constructor
        Path(bool outbound, bool enumerating);
        /// Desctructor
        ~Path();

        /// How many links are in this path?
        size_t size() const;

        /// Accessors
        const std::pair<int, StopState>& operator[](size_t n) const;
              std::pair<int, StopState>& operator[](size_t n);
        const std::pair<int, StopState>& back() const;

        /// Comparison
        bool operator<(const Path& other) const;

        /// Add link to the path, modifying if necessary
        void addLink(int stop_id,
                     const StopState& link,
                     std::ostream& trace_file,
                     const PathSpecification& path_spec,
                     const PathFinder& pf);

    };

    /** A set of paths consists of paths mapping to information about them (for choosing one)
     */
    typedef std::map<Path, PathInfo> PathSet;

}

#endif