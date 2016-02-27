
namespace fasttrips {

    /**
     * Struct containing just a label and a stop id, this is stored in the fasttrips::LabelStopQueue
     * (a priority queue) to find the lowest label stops.
     */
    typedef struct {
        double  label_;                 ///< The label during path finding
        int     stop_id_;               ///< Stop ID corresponding to this label
    } LabelStop;


    /// Comparator to enable the fasttrips::LabelStopQueue to return the lowest labeled stop.
    struct LabelStopCompare {
        bool operator()(const LabelStop &cs1, const LabelStop &cs2) const {
            if (cs1.label_ > cs2.label_) { return true;  }
            if (cs1.label_ < cs2.label_) { return false; }
            // if they're equal go by the stop id
            return (cs1.stop_id_ > cs2.stop_id_);
        }
    };

    /**
     * This is just like a priority queue but with the additonal constraint that each stop can only be in the queue once.
     **/
    class LabelStopQueue
    {

    private:
        /**
         * The pathfinding algorithm uses this to find the lowest label stops. The LabelStopQueue will have the
         *     latest departure time from a stop (outbound) and we want max, or
         *   earliest arrival   time to   a stop (inbound ) and we want min
         * Since it's a min priority queue, for outbound, we'll want to make it negative to get min
         */
        std::priority_queue<LabelStop, std::vector<LabelStop>, struct LabelStopCompare> labelstop_priority_queue_;


    public:
        LabelStopQueue() {}
        ~LabelStopQueue() {}

        void push(const LabelStop& val) {
            labelstop_priority_queue_.push(val);
        }

        const LabelStop& top() const {
            return labelstop_priority_queue_.top();
        }

        void pop() {
            labelstop_priority_queue_.pop();
        }

        size_t size() const {
            return labelstop_priority_queue_.size();
        }

        bool empty() const {
            return labelstop_priority_queue_.empty();
        }
    };

};