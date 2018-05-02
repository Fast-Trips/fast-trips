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

    /// utility function to make sure time is in [0, 24*60) = [0, 1440)
    double fix_time_range(double time);

    /** In stochastic path finding, this is the information we'll collect about the path. */
    typedef struct {
        int     count_;             ///< Number of times this path was generated (for stochastic)
        double  probability_;       ///< Probability of this stop          (for stochastic)
        int     prob_i_;            ///< Cumulative probability * INT_MULT (for stochastic)
    } PathInfo;

    // Forward declarations
    class PathFinder;
    struct FarePeriod;

    /**
     * This class represents a concrete path.
     **/
    class Path
    {
    private:
        bool    outbound_;          ///< is this path outbound (preferred arrival) or inbound (preferred departure)?
        bool    enumerating_;       ///< are we enumarating paths?  or labeling?
        double  fare_;              ///< Total fare of this path.
        double  cost_;              ///< Cost of this path.
        bool    capacity_problem_;  ///< Does this path have a capacity problem?

        /// For debugging/investigation.  When we first enumerate this path by adding links
        /// via Path::addLink(), we're guessing at fares and costs.  When we finally complete the
        /// path and call Path::calculateCost(), we will know cost and fare with full information.
        /// These variables are for saving our initial understanding to see how far off we were.
        /// These are set in calculateCost().
        double initial_fare_;       ///< Initial total fare
        double initial_cost_;       ///< Initial cost

        /// The links that make up this path (stop id, stop states)
        /// They are in origin to destination order for outbound trips,
        /// and destination to origin order for inbound trips.
        std::vector< std::pair<int, StopState> > links_;

        /// Boards per fare period.  Added by addLink()
        std::map< std::string, int > boards_per_fareperiod_;

    public:
        /// Default constructor
        Path();
        /// Constructor
        Path(bool outbound, bool enumerating);
        /// Desctructor
        ~Path();

        /// How many links are in this path?
        size_t size() const;
        double cost() const;
        double fare() const;
        double initialCost() const; ///< initial understanding of the cost before path was finalized
        double initialFare() const; ///< initial understanding of the fare before path was finalized

        void clear();

        /// Accessors
        const std::pair<int, StopState>& operator[](size_t n) const;
              std::pair<int, StopState>& operator[](size_t n);
        const std::pair<int, StopState>& back() const;
              std::pair<int, StopState>& back();
        const std::pair<int, StopState>* lastAddedTrip() const;
        int boardsForFarePeriod(const std::string& fare_period) const;

        /// Comparison operator; determines ordering in PathSet
        bool operator<(const Path& other) const;

        /// Returns the fare given the relevant fare period, adjusting for transfer from last fare period as applicable
        double getFareWithTransfer(const PathFinder&  pf,
                                   const std::string& last_fare_period,
                                   const FarePeriod*  fare_period) const;

        /// Add link to the path, modifying if necessary
        /// Return feasibility (infeasible if two out of order trips)
        bool addLink(int stop_id,
            const StopState& link,
            std::ostream& trace_file,
            const PathSpecification& path_spec,
            const PathFinder& pf);

        /** Calculates the cost for the entire given path, and checks for capacity issues.
         *  Sets the resulting cost and also updates link costs
         **/
        void calculateCost(std::ostream& trace_file,
            const PathSpecification& path_spec,
            const PathFinder& pf,
            bool hush = false);

        void print(std::ostream& ostr,
            const PathSpecification& path_spec,
            const PathFinder& pf) const;

        void printCompat(
            std::ostream& ostr,
            const PathSpecification& path_spec,
            const PathFinder& pf) const;
    };

    /** Path -> count of times it was generated
     */
    typedef std::map<Path, PathInfo> PathSet;

}

#endif
