#include <cassert>
#include <exception>
#include <stdexcept>

// Uncomment for debug detail for LabelStopQueue
// #define DEBUG_LSQ

// Debug macros. If debug is not on, the if (trace) isn't even executed.
#ifdef DEBUG_LSQ
#  define D_LSQ(x) do { if (trace) { x } } while (0)
#else
#  define D_LSQ(x) do {} while (0)
#endif

namespace fasttrips {

    /**
     * Struct containing just a label and a stop id, this is stored in the fasttrips::LabelStopQueue
     * (a priority queue) to find the lowest label stops.
     */
    typedef struct {
        double  label_;                 ///< The label during path finding
        int     stop_id_;               ///< Stop ID corresponding to this label
        bool    is_trip_;               ///< Two labels: a trip-label and a non-trip-label
    } LabelStop;

    /// Supply data: Stops
    typedef struct {
        std::string stop_str_;          ///< stop id string
        int         zone_num_;          ///< stop zone number for fare lookup
    } Stop;

    /// Comparator to enable the fasttrips::LabelStopQueue to return the lowest labeled stop.
    struct LabelStopCompare {
        bool operator()(const LabelStop &cs1, const LabelStop &cs2) const {
            if (cs1.label_ > cs2.label_) { return true;  }
            if (cs1.label_ < cs2.label_) { return false; }

            if (cs1.stop_id_ > cs2.stop_id_) { return true;  }
            if (cs1.stop_id_ < cs2.stop_id_) { return false; }

            if (cs1.is_trip_ && !cs2.is_trip_) { return true; }
            return false;
        }
    };

    class LabelStopQueueError : public std::runtime_error {
    public:
        LabelStopQueueError(const std::string& what_arg): std::runtime_error(what_arg) {}
        LabelStopQueueError(const char* what_arg): std::runtime_error(what_arg) {}
        virtual ~LabelStopQueueError() throw() {}
    };

    /**
     * This is just like a priority queue but with the additonal constraint that each (stop ID, is trip bool)
     * can only be in the queue once.
     *
     * This is to save work; if we mark a stop for processing by adding it onto the queue, and then do that again shortly
     * after, we don't actually want to process twice.  We only want to process it once, for the lowest label.
     *
     **/
    class LabelStopQueue
    {

    private:
        // underlying priority queue, contains (label, stop id, is trip bool)
        std::priority_queue<LabelStop, std::vector<LabelStop>, struct LabelStopCompare> labelstop_priority_queue_;

        typedef struct {
            double label_;  ///< lowest label for this stop in the labelstop_priority_queue_ (e.g. the only valid one)
            bool   valid_;  ///< is this stop valid in the queue?
            int    count_;  ///< number of instances of this stop in the labelstop_priority_queue_ (valid and invalid)
        } LabelCount;

        /** Keep track of the lowest label and the count for each (stop, is_trip bool) */
        std::map< std::pair<int, bool>, LabelCount> labelstop_map_;

        int valid_count_;

    public:
        LabelStopQueue() : valid_count_(0) {}
        ~LabelStopQueue() {}

        void push(const LabelStop& val) {
            // if the stop is not in here, no problem!
            std::pair<int,bool> full_stop_id = std::make_pair(val.stop_id_, val.is_trip_);

            if (labelstop_map_.find(full_stop_id) == labelstop_map_.end()) {
                labelstop_priority_queue_.push(val);
                LabelCount lc = { val.label_, true, 1 };
                labelstop_map_[full_stop_id] = lc;
                valid_count_++;
                return;
            }

            // if not valid in the queue, then we've popped out all valid instances from the priority queue so it's like it's not here
            if (!labelstop_map_[full_stop_id].valid_) {
                labelstop_priority_queue_.push(val);
                labelstop_map_[full_stop_id].label_     = val.label_;
                labelstop_map_[full_stop_id].valid_     = true;
                labelstop_map_[full_stop_id].count_    += 1;
                valid_count_++;
                return;
            }

            // The stop is in the queue, valid.  Look at the label.
            // If the label is smaller, add this one and invalidate the other
            if (val.label_ < labelstop_map_[full_stop_id].label_) {
                labelstop_priority_queue_.push(val);
                labelstop_map_[full_stop_id].label_ = val.label_;
                labelstop_map_[full_stop_id].count_ += 1;
                // no additional valid counts
            }
            // otherwise the label is bigger -- don't add it since the smaller one will cause reprocessing
            else {
                // we're ok
            }
        }

        /** Pop the top *valid* LabelStop */
        LabelStop pop_top(const std::map<int, Stop>& stop_num_to_stop, bool trace, std::ofstream& trace_file) {
            // this will crash if labelstop_priority_queue_ is empty.  I'm terrible.

            while (true) {
                // get the lowest cost stop
                const LabelStop& ls = labelstop_priority_queue_.top();
                std::pair<int, bool> full_stop_id = std::make_pair(ls.stop_id_, ls.is_trip_);

                std::map< std::pair<int, bool>, LabelCount>::iterator ls_iter = labelstop_map_.find(full_stop_id);

                // assert we have the count info
                if (ls_iter == labelstop_map_.end()) {
                    std::cerr << "LabelStopQueueError FATAL ERROR 1" << std::endl;
                    throw LabelStopQueueError("FATAL ERROR 1");
                }

                // assert that the count is positive
                if (ls_iter->second.count_ <= 0) {
                    std::cerr << "LabelStopQueueError FATAL ERROR 2" << std::endl;
                    throw LabelStopQueueError("FATAL ERROR 2");
                }

                // if it's not valid then continue
                if (!ls_iter->second.valid_) {
                    D_LSQ(
                        trace_file << "Skipping stop A (" << stop_num_to_stop.find(ls.stop_id_)->second.stop_str_ << "," << ls.is_trip_ << ")";
                        trace_file << "; valid " << ls_iter->second.valid_;
                        trace_file << "; count " << ls_iter->second.count_;
                        trace_file << "; map label " << ls_iter->second.label_;
                        trace_file << "; priq label " << ls.label_;
                        trace_file << "; priq size " << labelstop_priority_queue_.size() << std::endl;
                    );
                    ls_iter->second.count_ -= 1;
                    labelstop_priority_queue_.pop();
                    continue;
                }

                // this stop id is in the queue as valid
                // but only the matching label is valid
                if (ls_iter->second.label_ != ls.label_) {
                    D_LSQ(
                        trace_file << "Skipping stop B (" << stop_num_to_stop.find(ls.stop_id_)->second.stop_str_ << "," << ls.is_trip_ << ")";
                        trace_file << "; valid " << ls_iter->second.valid_;
                        trace_file << "; count " << ls_iter->second.count_;
                        trace_file << "; map label " << ls_iter->second.label_;
                        trace_file << "; priq label " << ls.label_;
                        trace_file << "; priq size " << labelstop_priority_queue_.size() << std::endl;
                    );
                    ls_iter->second.count_ -= 1;
                    labelstop_priority_queue_.pop();
                    continue;
                }

                D_LSQ(
                    trace_file << "LabelStopQueue returning (" << stop_num_to_stop.find(ls.stop_id_)->second.stop_str_ << "," << ls.is_trip_ << ")";
                    trace_file << "; valid " << ls_iter->second.valid_;
                    trace_file << "; count " << ls_iter->second.count_;
                    trace_file << "; map label " << ls_iter->second.label_;
                    trace_file << "; priq label " << ls.label_;
                    trace_file << "; priq size " << labelstop_priority_queue_.size();
                    trace_file << "; valid count " << valid_count_ << std::endl;
                );
                // else the map label is equal to the priority queue one, it's the valid one -- return it
                LabelStop to_ret = ls;
                labelstop_priority_queue_.pop();
                ls_iter->second.valid_  = false; // not valid any longer
                ls_iter->second.count_ -= 1;     // decrement count
                valid_count_ -= 1;
                return to_ret;

            }
        }

        size_t size() const {
            return valid_count_;
        }

        bool empty() const {
            return (valid_count_ == 0);
        }
    };

};
