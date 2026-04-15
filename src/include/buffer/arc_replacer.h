
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.h
//
// Identification: src/include/buffer/arc_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

enum class ArcStatus { MRU, MFU, MRU_GHOST, MFU_GHOST };

// TODO(student): You can modify or remove this struct as you like.
struct LiveEntry {
  page_id_t page_id_;
  bool evictable_;
  ArcStatus arc_status_;
  std::list<frame_id_t>::iterator iter_;

  LiveEntry(page_id_t page_id, bool evictable, ArcStatus arc_status_, std::list<frame_id_t>::iterator iter) : 
  page_id_(page_id), evictable_(evictable), arc_status_(arc_status_), iter_(iter) {}
};

struct GhostEntry {
  ArcStatus arc_status_;
  std::list<page_id_t>::iterator iter_;

  GhostEntry(ArcStatus arc_status_, std::list<page_id_t>::iterator iter) :
  arc_status_(arc_status_), iter_(iter) {}
};

/**
 * ArcReplacer implements the ARC replacement policy.
 */
class ArcReplacer {
 public:
  explicit ArcReplacer(size_t num_frames);

  DISALLOW_COPY_AND_MOVE(ArcReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~ArcReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;
  void RecordAccess(frame_id_t frame_id, page_id_t page_id, AccessType access_type = AccessType::Unknown);
  void SetEvictable(frame_id_t frame_id, bool set_evictable);
  void Remove(frame_id_t frame_id);
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace or remove these member variables as you like.
  void RemoveGhost(page_id_t page_id);
  void PushGhostFront(page_id_t page_id, ArcStatus ghost_status);
  void MoveAliveToMFU(frame_id_t frame_id);
  void InsertAliveToMFU(page_id_t page_id, frame_id_t frame_id);
  void InsertAliveTOMRU(page_id_t page_id, frame_id_t frame_id);
  auto EvictFromList(std::list<frame_id_t> &lst, ArcStatus ghost_status) -> std::optional<frame_id_t>;
  void TrimGhostIfNeeded();

  
  std::list<frame_id_t> mru_;
  std::list<frame_id_t> mfu_;
  std::list<page_id_t> mru_ghost_;
  std::list<page_id_t> mfu_ghost_;

  /* record entries in mru_ and mfu_
   * this uses frame_id_t to guarantee no duplicate records for the same
   * frame when they are alive */
  std::unordered_map<frame_id_t, LiveEntry> alive_map_;
  /* record entries in mru_ghost_ and mfu_ghost_
   * this uses page_id_t but not frame_id_t because page_id is the unique
   * identifier in ghost lists */
  std::unordered_map<page_id_t, GhostEntry> ghost_map_;

  /* alive, evictable entries count */
  [[maybe_unused]] size_t curr_size_{0};
  /* p as in original paper */
  [[maybe_unused]] size_t mru_target_size_{0};
  /* c as in original paper */
  [[maybe_unused]] size_t replacer_size_;
  std::mutex latch_;

  // TODO(student): You can add member variables / functions as you like.
};

}  // namespace bustub
