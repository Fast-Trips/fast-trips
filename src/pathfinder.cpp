#include "pathfinder.h"

#include <sstream>
#include <iomanip>

namespace fasttrips {

    PathFinder::PathFinder() : process_num_(-1)
    {
        std::cout << "PathFinder constructor" << std::endl;
    }

    void PathFinder::initializeSupply(
        int    process_num,
        int*   taz_access_index,
        float* taz_access_cost,
        int    num_links)
    {
        process_num_ = process_num;

        // std::ostringstream ss;
        // ss << "fasttrips_ext_" << std::setw (3) << std::setfill ('0') << process_num_ << ".log";
        // logfile_.open(ss.str().c_str());
        // logfile_ << "PathFinder InitializeSupply" << std::endl;

        for(int i=0; i<num_links; ++i) {
            taz_access_links_[taz_access_index[2*i]][taz_access_index[2*i+1]] = taz_access_cost[i];
            if ((process_num_<= 1) && ((i<5) || (i>num_links-5))) {
                printf("access_links[%d][%d]=%f\n", taz_access_index[2*i], taz_access_index[2*i+1], taz_access_cost[i]);
            }
        }
    }

    PathFinder::~PathFinder()
    {
        std::cout << "PathFinder destructor" << std::endl;
    }

    // Find the path from the origin TAZ to the destination TAZ
    void PathFinder::findPath(int origin_id, int destination_id) const
    {
        std::cout << "PathFinder findPath with origin " << origin_id << " and destination " << destination_id << std::endl;
    }
}