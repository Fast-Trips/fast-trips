#include "access_egress.h"
#include "path.h"

#include <fstream>
#include <limits.h>
#include <ios>
#include <iomanip>
#include <string>

namespace fasttrips {

    std::ostream& operator<<(std::ostream& os, const AccessEgressLinkKey& aelk)
    {
        return os << aelk.taz_id_ << " " << aelk.supply_mode_num_ << " " << aelk.stop_id_ << " " << aelk.start_time_ << " " << aelk.end_time_;
    }

    AccessEgressLinks::AccessEgressLinks() :
        supply_mode_num_min_(INT_MAX),
        supply_mode_num_max_(INT_MIN),
        stop_id_min_(INT_MAX),
        stop_id_max_(INT_MIN)
    {}

    void AccessEgressLinks::readLinks(std::ifstream& accegr_file, bool debug_out) {
        // reset
        accegr_file.clear();
        accegr_file.seekg(0, std::ios::beg);

        std::string string_taz_num, string_supply_mode_num, string_stop_id_num, string_start_time, string_end_time, attr_name, string_attr_value;
        double attr_value;

        accegr_file >> string_taz_num >> string_supply_mode_num >> string_stop_id_num
                    >> string_start_time >> string_end_time >> attr_name >> string_attr_value;
        if (debug_out) {
            std::cout << "[" << string_taz_num         << "] ";
            std::cout << "[" << string_supply_mode_num << "] ";
            std::cout << "[" << string_stop_id_num     << "] ";
            std::cout << "[" << string_start_time      << "] ";
            std::cout << "[" << string_end_time        << "] ";
            std::cout << "[" << attr_name              << "] ";
            std::cout << "[" << string_attr_value      << "] ";
        }
        int attrs_read = 0;
        AccessEgressLinkKey aelk;
        while (accegr_file >> aelk.taz_id_ >> aelk.supply_mode_num_ >> aelk.stop_id_ >> aelk.start_time_ >> aelk.end_time_ >> attr_name >> attr_value) {
            map_[aelk][attr_name] = attr_value;
            attrs_read++;

            supply_mode_num_min_ = std::min(supply_mode_num_min_, aelk.supply_mode_num_);
            supply_mode_num_max_ = std::max(supply_mode_num_max_, aelk.supply_mode_num_);
            stop_id_min_         = std::min(stop_id_min_,         aelk.stop_id_        );
            stop_id_max_         = std::max(stop_id_max_,         aelk.stop_id_        );
        }
        if (debug_out) {
            std::cout << " => Read " << attrs_read << " attributes for " << map_.size() << " links" << std::endl;
            // std::cout << " supply_mode_num_ in [" << supply_mode_num_min_ << ", " << supply_mode_num_max_ << "]" << std::endl;
            // std::cout << " stop_id_         in [" << stop_id_min_         << ", " << stop_id_max_         << "]" << std::endl;

            // for (AccessEgressLinkAttr::const_iterator it=map_.begin(); it != map_.end(); ++it) {
            //     const AccessEgressLinkKey& aelk = it->first;
            //     std::cout << aelk << " => " << it->second.size() << std::endl;
            // }
        }
    }

    bool AccessEgressLinks::hasLinksForTaz(int taz_id) const {

        // Returns an iterator pointing to the first element in the container whose key is not considered to go before k (i.e., either it is equivalent or goes after).
        AccessEgressLinkAttr::const_iterator iter_low = map_.lower_bound( AccessEgressLinkKey(taz_id, supply_mode_num_min_-1, 0, 0, 0) );
        // no keys >= given key
        if (iter_low == map_.end()) {
            return false;
        }
        // Returns an iterator pointing to the first element in the container whose key is considered to go after k.
        AccessEgressLinkAttr::const_iterator iter_hi  = map_.upper_bound( AccessEgressLinkKey(taz_id, supply_mode_num_max_+1, 0, 0, 0) );
        if (iter_hi == map_.end()) {
            // this is ok if they're at the end
        }

        /** Suppose the links are the following and the taz_id is 13, then iter_lo and iter_hi would point to the following.
         *  11 101 5 0 1440 => 2
         *  11 104 1 0 1440 => 10
         *  12 101 1 0 1440 => 2
         *  12 101 4 0 1440 => 2
         *  14 101 3 0 1440 => 2    <- iter_lo, iter_hi
         *  14 104 8 0 1440 => 10
        **/
        if (iter_low == iter_hi) {
            return false;
        }
        return true;
    }

    AccessEgressLinkAttr::const_iterator AccessEgressLinks::lower_bound(int taz_id, int supply_mode_num) const
    {
        return map_.lower_bound( AccessEgressLinkKey(taz_id, supply_mode_num, stop_id_min_-1, 0, 0));
    }

    AccessEgressLinkAttr::const_iterator AccessEgressLinks::upper_bound(int taz_id, int supply_mode_num) const
    {
        return map_.upper_bound( AccessEgressLinkKey(taz_id, supply_mode_num, stop_id_max_+1, 0, 0));
    }

    AccessEgressLinkAttr::const_iterator AccessEgressLinks::lower_bound(int taz_id, int supply_mode_num, int stop_id) const
    {
        return map_.lower_bound( AccessEgressLinkKey(taz_id, supply_mode_num, stop_id, -100*24, -100*24));
    }

    AccessEgressLinkAttr::const_iterator AccessEgressLinks::upper_bound(int taz_id, int supply_mode_num, int stop_id) const
    {
        return map_.upper_bound( AccessEgressLinkKey(taz_id, supply_mode_num, stop_id, 100*24,  100*24));
    }

    /// Accessor
    const Attributes* AccessEgressLinks::getAccessAttributes(int taz_id, int supply_mode_num, int stop_id, double tp_time) const
    {

        AccessEgressLinkAttr::const_iterator iter_lo = map_.lower_bound(AccessEgressLinkKey(taz_id, supply_mode_num, stop_id, -100*24, -100*24));

        AccessEgressLinkAttr::const_iterator iter_hi = map_.lower_bound(AccessEgressLinkKey(taz_id, supply_mode_num, stop_id,  100*24,  100*24));

        double tp_time_024 = fix_time_range(tp_time);
        for (AccessEgressLinkAttr::const_iterator iter = iter_lo; iter != iter_hi; ++iter) {
            if ((iter->first.start_time_ <= tp_time_024) && (tp_time_024 < iter->first.end_time_)) {
                return &(iter->second);
            }
        }
        return NULL;
    }

}