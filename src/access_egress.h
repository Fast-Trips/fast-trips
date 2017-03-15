/**
 * \file access_egress.h
 *
 * Defines the access/egress link lookup structure
 */
#include <map>

namespace fasttrips {
    /// Generic attributes
    typedef std::map<std::string, double> Attributes;

    /// Duration struct.  Goes from [start_time_,end_time_), so end_time is not included
    typedef struct {
        double start_time_;     /// minutes past midnight, included
        double end_time_;       /// minutes past midnight, not included
    } TimePeriod;

    struct TimePeriodCompare {
        // less than
        bool operator()(const TimePeriod& tp1, const TimePeriod& tp2) const {
            if (tp1.start_time_ < tp2.start_time_) { return true;  }
            if (tp1.start_time_ > tp2.start_time_) { return false; }
            if (tp1.end_time_   < tp2.end_time_  ) { return true;  }
            if (tp1.end_time_   > tp2.end_time_  ) { return false; }
            return false;
        }
    };


    typedef std::map<TimePeriod, Attributes, struct fasttrips::TimePeriodCompare > TimePeriodToAttr;
    typedef std::map<int, TimePeriodToAttr> StopTpToAttr;
    typedef std::map<int, StopTpToAttr> SupStopTpToAttr;
    /// Access/Egress information: taz id -> supply_mode -> stop id -> (start time, end time) -> attribute map
    typedef std::map<int, SupStopTpToAttr> TAZSupStopTpToAttr;

}