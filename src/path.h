/**
 * \file path.h
 *
 * Defines Path class.
 */
#include <iostream>
#include <map>
#include <vector>

#include "pathspec.h"

#ifndef PATH_H
#define PATH_H

namespace fasttrips {


    /** In stochastic path finding, this is the information we'll collect about the path. */
    typedef struct {
        int     count_;             ///< Number of times this path was generated (for stochastic)
        double  probability_;       ///< Probability of this stop          (for stochastic)
        int     prob_i_;            ///< Cumulative probability * RAND_MAX (for stochastic)
    } PathInfo;

    // Forward declarations
    class PathFinder;

    /**
     * This class represents a concrete path.
     **/
    class Path
    {
    private:
        bool    chrono_order_;      ///< is this path in chronological order?
        double  cost_;              ///< Cost of this path.
        bool    capacity_problem_;  ///< Does this path have a capacity problem?

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
        /// What's the cost of this path?
        double cost() const;

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

        /** Calculates the cost for the entire given path, and checks for capacity issues.
         *  Sets the resulting cost and also updates link costs
         **/
        void calculateCost(std::ostream& trace_file,
            const PathSpecification& path_spec,
            const PathFinder& pf);

        void print(std::ostream& ostr,
            const PathSpecification& path_spec,
            const PathFinder& pf) const;

        void printCompat(
            std::ostream& ostr,
            const PathSpecification& path_spec,
            const PathFinder& pf) const;
    };

    /** A set of paths consists of paths mapping to information about them (for choosing one)
     */
    typedef std::map<Path, PathInfo> PathSet;

}

#endif