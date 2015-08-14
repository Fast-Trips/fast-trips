#include <map>
#include <iostream>
#include <fstream>

namespace fasttrips {

    class PathFinder
    {
    private:
        // for multi-processing
        int process_num_;

        // taz id -> stop id -> cost
        std::map<int, std::map<int,float>> taz_access_links_;

    public:
        // Constructor
        PathFinder();

        void initializeSupply(int    process_num,
                              int*   taz_access_index,
                              float* taz_access_cost,
                              int    num_links);

        // Destructor
        ~PathFinder();

        // Find the path from the origin TAZ to the destination TAZ
        // TODO: the return won't be a const of course
        void findPath(int origin_id, int destination_id) const;
    };
}