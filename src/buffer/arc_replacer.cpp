// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

namespace {

auto IsValidFrameId(frame_id_t frame_id, size_t replacer_size) -> bool {
    return frame_id >= 0 && static_cast<size_t>(frame_id) <= replacer_size;
}

}  // namespace

void ArcReplacer::RemoveGhost(page_id_t page_id) {
    auto it = ghost_map_.find(page_id);
    if(it == ghost_map_.end()) {
        return;
    }

    if(it->second.arc_status_ == ArcStatus::MRU_GHOST) {
        mru_ghost_.erase(it->second.iter_);
    } else {
        mfu_ghost_.erase(it->second.iter_);
    }
    ghost_map_.erase(it);
}

void ArcReplacer::PushGhostFront(page_id_t page_id, ArcStatus ghost_status) {
    RemoveGhost(page_id);

    if(ghost_status == ArcStatus::MRU_GHOST) {
        mru_ghost_.push_front(page_id);
        ghost_map_.emplace(page_id, GhostEntry{ArcStatus::MRU_GHOST, mru_ghost_.begin()});
    }  else {
        mfu_ghost_.push_front(page_id);
        ghost_map_.emplace(page_id, GhostEntry{ArcStatus::MFU_GHOST, mfu_ghost_.begin()});
    }

    TrimGhostIfNeeded();
}

void ArcReplacer::MoveAliveToMFU(frame_id_t frame_id) {
    auto &entry = alive_map_.at(frame_id);

    if(entry.arc_status_ == ArcStatus::MRU) {
        mru_.erase(entry.iter_);
    } else {
        mfu_.erase(entry.iter_);
    }
    
    mfu_.push_front(frame_id);
    entry.iter_ = mfu_.begin();
    entry.arc_status_ = ArcStatus::MFU;
}

void ArcReplacer::InsertAliveTOMRU(page_id_t page_id, frame_id_t frame_id) {
    mru_.push_front(frame_id);
    alive_map_.emplace(frame_id, LiveEntry{page_id, false, ArcStatus::MRU, mru_.begin()});
}

void ArcReplacer::InsertAliveToMFU(page_id_t page_id, frame_id_t frame_id) {
    mfu_.push_front(frame_id);
    alive_map_.emplace(frame_id, LiveEntry{page_id, false, ArcStatus::MFU, mfu_.begin()});
}

void ArcReplacer::TrimGhostIfNeeded() {
    while(mfu_ghost_.size() + mru_ghost_.size() > replacer_size_) {
        if(mru_ghost_.size() >= mfu_ghost_.size() && !mru_ghost_.empty()) {
            auto victim_page = mru_ghost_.back();
            mru_ghost_.pop_back();
            ghost_map_.erase(victim_page);
        } else if (!mfu_ghost_.empty()) {
            auto victim_page = mfu_ghost_.back();
            mfu_ghost_.pop_back();
            ghost_map_.erase(victim_page);
        } else {
            break;
        }
    }
}

auto ArcReplacer::EvictFromList(std::list<frame_id_t> &lst, ArcStatus ghost_status) -> std::optional<frame_id_t> {
    for(auto it = lst.end(); it != lst.begin();) {
        --it;
        frame_id_t frame_id = *it;
        auto map_it = alive_map_.find(frame_id);
        if(map_it == alive_map_.end()) {
            continue;
        }

        if(!map_it->second.evictable_) {
            continue;
        }

        page_id_t page_id = map_it->second.page_id_;
        lst.erase(it);
        alive_map_.erase(map_it);
        curr_size_--;
        PushGhostFront(page_id, ghost_status);
        return frame_id;
    }
    return std::nullopt;
}


/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> { 
    std::lock_guard<std::mutex> guard(latch_);

    if(curr_size_ == 0) {
        return std::nullopt;
    }

    bool perform_mru = mru_.size() >= mru_target_size_;

    if(perform_mru) {
        auto victim = EvictFromList(mru_, ArcStatus::MRU_GHOST);

        if(victim.has_value()) {
            return victim;
        }

        return EvictFromList(mfu_, ArcStatus::MFU_GHOST);
    }

    auto victim = EvictFromList(mfu_, ArcStatus::MFU_GHOST);

    if(victim.has_value()) {
        return victim;
    }

    return EvictFromList(mru_, ArcStatus::MRU_GHOST);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex> guard(latch_);
    BUSTUB_ASSERT(IsValidFrameId(frame_id, replacer_size_), "invalid frame id");

    auto alive_it = alive_map_.find(frame_id);

    if(alive_it != alive_map_.end()) {
        BUSTUB_ASSERT(alive_it->second.page_id_ == page_id, "frame_id is already bound to another page ");
        MoveAliveToMFU(frame_id);
        return;
    }

    auto ghost_it = ghost_map_.find(page_id);
    if(ghost_it != ghost_map_.end()) {
        if(ghost_it->second.arc_status_ == ArcStatus::MRU_GHOST) {
            size_t delta = std::max<size_t>(1, mfu_ghost_.size() / std::max<size_t>(1, mru_ghost_.size()));
            mru_target_size_ = std::min(replacer_size_, mru_target_size_ + delta);
        } else {
            size_t delta = std::max<size_t>(1, mru_ghost_.size() / std::max<size_t>(1, mfu_ghost_.size()));
            mru_target_size_ = (delta > mru_target_size_) ? 0 : (mru_target_size_ - delta);
        }
        RemoveGhost(page_id);
        InsertAliveToMFU(page_id, frame_id);
        return;
    }

    if(mru_.size() + mru_ghost_.size() == replacer_size_) {
        if(mru_.size() < replacer_size_) {
            BUSTUB_ASSERT(!mru_ghost_.empty(), "mru ghost must be non-empty when trimming");
            auto victim_page = mru_ghost_.back();
            mru_ghost_.pop_back();
            ghost_map_.erase(victim_page);
        }
    } else {
        size_t total_size = mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size();
        if(total_size >= 2 * replacer_size_ && !mfu_ghost_.empty()) {
            auto victim_page = mfu_ghost_.back();
            mfu_ghost_.pop_back();
            ghost_map_.erase(victim_page);
        }
    }

    InsertAliveTOMRU(page_id, frame_id);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> guard(latch_);
    BUSTUB_ASSERT(IsValidFrameId(frame_id, replacer_size_), "invalid frame id");

    auto it = alive_map_.find(frame_id);
    if(it == alive_map_.end()) {
        return;
    }

    if(it->second.evictable_ == set_evictable) {
        return;
    }

    it->second.evictable_ = set_evictable;
    if(set_evictable) {
        curr_size_++;
    } else {
        curr_size_--;
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    if(frame_id < 0) {
        BUSTUB_ASSERT(false, "invaild frame id");
    }

    auto it = alive_map_.find(frame_id);
    if(it == alive_map_.end()) {
        return;
    }

    BUSTUB_ASSERT(it->second.evictable_, "cannot remove a non-evictable frame");

    if(it->second.arc_status_ == ArcStatus::MRU) {
        mru_.erase(it->second.iter_);
    } else {
        mfu_.erase(it->second.iter_);
    }

    alive_map_.erase(it);
    curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex> guard(latch_);
    return curr_size_;
}

}  // namespace bustub
