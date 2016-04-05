/**
 * \file path.cpp
 *
 * Path implementation
 **/

#include "path.h"
#include "pathfinder.h"
#include "hyperlink.h"

namespace fasttrips {

    Path::Path(const PathSpecification& path_spec) :
        path_spec_(path_spec)
    {}

    Path::~Path()
    {}

    // Assignment operator
    Path& Path::operator=(const Path& other)
    {
        // check for self-assignment
        if (&other == this) {
            return *this;
        }
        links_ = other.links_;
        return *this;
    }

    // How many links are in this path?
    size_t Path::size() const
    {
        return links_.size();
    }

    // Accessor
    const std::pair<int, StopState>& Path::operator[](size_t n) const
    {
        return links_[n];
    }

    std::pair<int, StopState>& Path::operator[](size_t n)
    {
        return links_[n];
    }

    const std::pair<int, StopState>& Path::back() const
    {
        return links_.back();
    }

    /// Comparison
    bool Path::operator<(const Path& path2) const
    {
        if (size() < path2.size()) { return true; }
        if (size() > path2.size()) { return false; }
        // if number of stops matches, check the stop ids and deparr_mode_
        for (int ind=0; ind<size(); ++ind) {
            if (links_[ind].first < path2[ind].first) { return true; }
            if (links_[ind].first > path2[ind].first) { return false; }
            if (links_[ind].second.deparr_mode_ < path2[ind].second.deparr_mode_) { return true; }
            if (links_[ind].second.deparr_mode_ > path2[ind].second.deparr_mode_) { return false; }
            if (links_[ind].second.trip_id_     < path2[ind].second.trip_id_    ) { return true; }
            if (links_[ind].second.trip_id_     > path2[ind].second.trip_id_    ) { return false; }
        }
        return false;
    }

    // Add link to the path, modifying if necessary
    void Path::addLink(int stop_id,
                       const StopState& link,
                       std::ostream& trace_file,
                       const PathFinder& pf)
    {
        // We'll likely modify this
        StopState new_link = link;

        // if we already have liks
        if (links_.size() > 0)
        {
            if (path_spec_.trace_)
            {
                trace_file << "path_req ";
                Hyperlink::printStopState(trace_file, stop_id, link, path_spec_, pf);
                trace_file << std::endl;
            }

            // UPDATES to links
            // Hyperpaths have some uncertainty built in which we need to rectify as we go through and choose
            // concrete path states.
            StopState& prev_link = links_.back().second;

            // OUTBOUND: We are choosing links in chronological order.
            if (path_spec_.outbound_)
            {
                // Leave origin as late as possible
                if (prev_link.deparr_mode_ == MODE_ACCESS) {
                    double dep_time = pf.getScheduledDeparture(new_link.trip_id_, stop_id, new_link.seq_);
                    // set departure time for the access link to perfectly catch the vehicle
                    // todo: what if there is a wait queue?
                    prev_link.arrdep_time_ = dep_time;
                    prev_link.deparr_time_ = dep_time - links_.front().second.link_time_;
                    // no wait time for the trip
                    new_link.link_time_ = new_link.arrdep_time_ - new_link.deparr_time_;
                }
                // *Fix trip time*
                else if (isTrip(new_link.deparr_mode_)) {
                    // link time is arrival time - previous arrival time
                    new_link.link_time_ = new_link.arrdep_time_ - prev_link.arrdep_time_;
                }
                // *Fix transfer times*
                else if (new_link.deparr_mode_ == MODE_TRANSFER) {
                    new_link.deparr_time_ = prev_link.arrdep_time_;   // start transferring immediately
                    new_link.arrdep_time_ = new_link.deparr_time_ + new_link.link_time_;
                }
                // Egress: don't wait, just walk. Get to destination as early as possible
                else if (new_link.deparr_mode_ == MODE_EGRESS) {
                    new_link.deparr_time_ = prev_link.arrdep_time_;
                    new_link.arrdep_time_ = new_link.deparr_time_ + new_link.link_time_;
                }
            }

            // INBOUND: We are choosing links in REVERSE chronological order
            else
            {
                // Leave origin as late as possible
                if (new_link.deparr_mode_ == MODE_ACCESS) {
                    double dep_time = pf.getScheduledDeparture(prev_link.trip_id_, stop_id, prev_link.seq_succpred_);
                    // set arrival time for the access link to perfectly catch the vehicle
                    // todo: what if there is a wait queue?
                    new_link.deparr_time_ = dep_time;
                    new_link.arrdep_time_ = new_link.deparr_time_ - new_link.link_time_;
                    // no wait time for the trip
                    prev_link.link_time_ = prev_link.deparr_time_ - prev_link.arrdep_time_;
                }
                // *Fix trip time*: we are choosing in reverse so pretend the wait time is zero for now to
                // accurately evaluate possible transfers in next choice.
                else if (isTrip(new_link.deparr_mode_)) {
                    new_link.link_time_ = new_link.deparr_time_ - new_link.arrdep_time_;
                    // If we just picked this trip and the previous (next in time) is transfer then we know the wait now
                    // and we can update the transfer and the trip with the real wait
                    if (prev_link.deparr_mode_ == MODE_TRANSFER) {
                        // move transfer time so we do it right after arriving
                        prev_link.arrdep_time_ = new_link.deparr_time_; // depart right away
                        prev_link.deparr_time_ = new_link.deparr_time_ + prev_link.link_time_; // arrive after walk
                        // give the wait time to the previous trip
                        links_[links_.size()-2].second.link_time_ = links_[links_.size()-2].second.deparr_time_ - prev_link.deparr_time_;
                    }
                    // If the previous (next in time) is another trip (so zero-walk transfer) give it wait time
                    else if (isTrip(new_link.deparr_mode_)) {
                        prev_link.link_time_ = prev_link.deparr_time_ - new_link.deparr_time_;
                    }
                }
                // *Fix transfer depart/arrive times*: transfer as late as possible to preserve options for earlier trip
                else if (new_link.deparr_mode_ == MODE_TRANSFER) {
                    new_link.deparr_time_ = prev_link.arrdep_time_;
                    new_link.arrdep_time_ = new_link.deparr_time_ - new_link.link_time_;
                }
                // Egress: don't wait, just walk. Get to destination as early as possible
                if (prev_link.deparr_mode_ == MODE_EGRESS) {
                    prev_link.arrdep_time_ = new_link.deparr_time_;
                    prev_link.deparr_time_ = prev_link.arrdep_time_ + prev_link.link_time_;
                }
            }
        }
        links_.push_back( std::make_pair(stop_id, new_link) );

        if (path_spec_.trace_)
        {
            trace_file << "path_add ";
            Hyperlink::printStopState(trace_file, stop_id, links_.back().second, path_spec_, pf);
            trace_file << std::endl;
        }
    }
}