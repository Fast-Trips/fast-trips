/**
 * \file access_egress.h
 *
 * Defines the access/egress link lookup structure
 */
#include <ios>
#include <iostream>
#include <map>
#include <ostream>

namespace fasttrips {
    /// Generic attributes
    typedef std::map<std::string, double> Attributes;

    /// Key for access egress links map
    struct AccessEgressLinkKey {
        int taz_id_;
        int supply_mode_num_;
        int stop_id_;
        double start_time_;
        double end_time_;

        AccessEgressLinkKey() :
            taz_id_(-1),
            supply_mode_num_(-1),
            stop_id_(-1),
            start_time_(-1),
            end_time_(-1)
        {}

        AccessEgressLinkKey(
            int    taz_id,
            int    supply_mode_num,
            int    stop_id,
            double start_time,
            double end_time) :
            taz_id_         (taz_id),
            supply_mode_num_(supply_mode_num),
            stop_id_        (stop_id),
            start_time_     (start_time),
            end_time_       (end_time)
        {}
    };

    /// method to print the AccessEgressLinkKey
    std::ostream& operator<<(std::ostream& os, const AccessEgressLinkKey& aelk);

    struct AccessEgressLinkCompare {
        // less than
        bool operator()(const AccessEgressLinkKey& ael1, const AccessEgressLinkKey& ael2) const {
            if (ael1.taz_id_          < ael2.taz_id_            ) { return true;  }
            if (ael1.taz_id_          > ael2.taz_id_            ) { return false; }
            if (ael1.supply_mode_num_ < ael2.supply_mode_num_   ) { return true;  }
            if (ael1.supply_mode_num_ > ael2.supply_mode_num_   ) { return false; }
            if (ael1.stop_id_         < ael2.stop_id_           ) { return true;  }
            if (ael1.stop_id_         > ael2.stop_id_           ) { return false; }
            if (ael1.start_time_      < ael2.start_time_        ) { return true;  }
            if (ael1.start_time_      > ael2.start_time_        ) { return false; }
            if (ael1.end_time_        < ael2.end_time_          ) { return true;  }
            if (ael1.end_time_        > ael2.end_time_          ) { return false; }
            return false;
        }
    };

    typedef std::map<AccessEgressLinkKey, Attributes, struct AccessEgressLinkCompare > AccessEgressLinkAttr;

    class AccessEgressLinks {
    private:
        int supply_mode_num_min_;
        int supply_mode_num_max_;
        int stop_id_min_;
        int stop_id_max_;

        // The primary data store
        AccessEgressLinkAttr map_;

    public:
        /// Constructor
        AccessEgressLinks();
        /// Destructor
        ~AccessEgressLinks() {}

        void clear() { map_.clear(); }

        void readLinks(std::ifstream& accegr_file, bool debug_out);

        /// Are there access or egress links for the given taz?
        bool hasLinksForTaz(int taz_id) const;

        /// Iterate through the links for the taz id and supply mode
        AccessEgressLinkAttr::const_iterator lower_bound(int taz_id, int supply_mode_num) const;
        AccessEgressLinkAttr::const_iterator upper_bound(int taz_id, int supply_mode_num) const;

        /// Iterate through the links for the taz id, supply mode and stop
        AccessEgressLinkAttr::const_iterator lower_bound(int taz_id, int supply_mode_num, int stop_id) const;
        AccessEgressLinkAttr::const_iterator upper_bound(int taz_id, int supply_mode_num, int stop_id) const;

        /// accessor
        const Attributes* getAccessAttributes(int taz_id, int supply_mode_num, int stop_id, double tp_time) const;
    };

}
