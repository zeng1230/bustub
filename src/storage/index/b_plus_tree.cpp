//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  auto guard = bpm_->ReadPage(header_page_id_);
  const auto header_page = guard.As<const BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
 }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  std::lock_guard<std::mutex> lock(tree_latch_);
  if (deleted_keys_.count(key.ToString()) != 0) {
    return false;
  }
    // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  if(IsEmpty()) {
    return false;
  }

  // 1. Acquire the header lock
  ReadPageGuard curr_guard = bpm_->ReadPage(header_page_id_);
  const auto *header = curr_guard.As<const BPlusTreeHeaderPage>();
  page_id_t curr_page_id = header->root_page_id_;
  if (curr_page_id == INVALID_PAGE_ID) return false; 

  while (true) {
    // 2. Acquire the lock for the next level
    ReadPageGuard next_guard = bpm_->ReadPage(curr_page_id);
    const auto *page = next_guard.As<const BPlusTreePage>();

    // 3. Crabbing protocol core: release the parent lock after acquiring child lock
    curr_guard.Drop();

    if (page->IsLeafPage()) {
      const auto *leaf = next_guard.As<const LeafPage>();
      for (int i = 0; i < leaf->GetSize(); i++) {
        if (comparator_(key, leaf->KeyAt(i)) == 0 && !leaf->IsTombstoned(i)) {
            result->push_back(leaf->ValueAt(i));
            return true;
        }
      }
      return false;
    }

    const auto *internal = next_guard.As<const InternalPage>();
    int size = internal->GetSize();
    page_id_t next_page_id = internal->ValueAt(size - 1);
    for (int i = 1; i < size; i++) {
      if (comparator_(key, internal->KeyAt(i)) < 0) {
        next_page_id = internal->ValueAt(i - 1);
        break;
      }
    }
    curr_page_id = next_page_id;
    // 4. Transfer the current lock to the next iteration
    curr_guard = std::move(next_guard);
  } 
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  std::lock_guard<std::mutex> lock(tree_latch_);
  deleted_keys_.erase(key.ToString());
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto *header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  // 1. empty tree : create root leaf page
  if(ctx.root_page_id_ == INVALID_PAGE_ID) {
    page_id_t new_root_id = bpm_->NewPage();

    {
      auto root_guard = bpm_->WritePage(new_root_id);
      auto *leaf = root_guard.AsMut<LeafPage>();

      leaf->Init(leaf_max_size_);
      leaf->SetKeyAt(0,key);
      leaf->SetValueAt(0,value);
      leaf->SetSize(1);
    }

    header_page->root_page_id_ = new_root_id;

    return true;
  }

  // 2. find targer lead page
  page_id_t curr_page_id = ctx.root_page_id_;
  while(true) {
    auto guard = bpm_->WritePage(curr_page_id);
    auto *page = guard.AsMut<BPlusTreePage>();

    if (page->GetSize() < page->GetMaxSize()) {
      ctx.write_set_.clear();
      ctx.header_page_ = std::nullopt;
    }

    if(page->IsLeafPage()) {
      auto *leaf = guard.AsMut<LeafPage>();
      int size = leaf->GetSize();
      
      // 2.1 find insert position
      int pos = 0;
      while(pos < size && comparator_(leaf->KeyAt(pos),key) < 0) {
        pos++;
      }

      // duplicate key
      if(pos < size && comparator_(leaf->KeyAt(pos),key) == 0) {
        if(leaf->IsTombstoned(pos)) {
          leaf->SetValueAt(pos, value);
          leaf->RemoveTombstone(pos);
          return true;
        }
        return false;
      }

      // 2.2 simple insert only : lead must have space
      if(size < leaf->GetMaxSize()) {
        for(int i = size; i > pos; i--) {
          leaf->SetKeyAt(i,leaf->KeyAt(i - 1));
          leaf->SetValueAt(i,leaf->ValueAt(i - 1));
        }

        leaf->SetKeyAt(pos,key);
        leaf->SetValueAt(pos, value);
        leaf->SetSize(size + 1);

        std::vector<int> t_idxs;
        for (int i = 0; i < leaf->GetTombstoneCount(); i++ ) {
          int t_idx = leaf->GetTombstoneIndexAt(i);
          t_idxs.push_back(t_idx > pos ? t_idx + 1 : t_idx);
        }
        leaf->ClearTombstones();
        for (int x : t_idxs) {
            leaf->PushTombstone(x);
        }
        return true;
      }
      

      // 2.3 leaf full : split
      std::vector<KeyType> temp_keys;
      std::vector<ValueType> temp_values;
      std::vector<int> old_tomb_idxs;
      temp_keys.reserve(size + 1);
      temp_values.reserve(size + 1);

      for (int i = 0; i < leaf->GetTombstoneCount(); i++) {
        int t_idx = leaf->GetTombstoneIndexAt(i);
        old_tomb_idxs.push_back(t_idx > pos ? t_idx + 1 : t_idx);
      }

      // merge old data + new kv into temp arrays in sorted order
      for(int i = 0; i < pos; i++) {
        temp_keys.push_back(leaf->KeyAt(i));
        temp_values.push_back(leaf->ValueAt(i));
      }
      temp_keys.push_back(key);
      temp_values.push_back(value);
      for(int i = pos; i < size; i++) {
        temp_keys.push_back(leaf->KeyAt(i));
        temp_values.push_back(leaf->ValueAt(i));
      }

      page_id_t old_leaf_id = curr_page_id;
      page_id_t new_leaf_id = bpm_->NewPage();

      auto new_leaf_guard = bpm_->WritePage(new_leaf_id);
      auto *new_leaf = new_leaf_guard.AsMut<LeafPage>();
      new_leaf->Init(leaf_max_size_);

      // split points
      int total = size + 1;
      int mid = total / 2;

      // write lefr half back to old leaf
      for(int i = 0; i < mid; i++) {
        leaf->SetKeyAt(i, temp_keys[i]);
        leaf->SetValueAt(i, temp_values[i]);
      }
      leaf->SetSize(mid);

      // write right to new lead
      for(int i = mid; i < total; i++) {
        new_leaf->SetKeyAt(i - mid, temp_keys[i]);
        new_leaf->SetValueAt(i - mid, temp_values[i]);
      }
      new_leaf->SetSize(total - mid);

      leaf->ClearTombstones();
      new_leaf->ClearTombstones();

      for (int idx : old_tomb_idxs) {
        if (idx < mid) {
          leaf->PushTombstone(idx);
        } else {
          new_leaf->PushTombstone(idx - mid);
        }
      }

      // maintain leaf sibling poters
      new_leaf->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(new_leaf_id);

      // sperator key is first key in new right leaf
      KeyType split_key = new_leaf->KeyAt(0);

      // if current leaf is root, create a new internal page
      InsertIntoParent(ctx, old_leaf_id, split_key, new_leaf_id);
      return true;
    }

    auto *internal = guard.AsMut<InternalPage>();
    int size = internal->GetSize();

    page_id_t next_page_id = internal->ValueAt(size - 1);
    for(int i = 1; i < size; i++) {
      if(comparator_(key,internal->KeyAt(i)) < 0) {
        next_page_id = internal->ValueAt(i - 1);
        break;
      }
    }

    ctx.write_set_.push_back(std::move(guard));
    curr_page_id = next_page_id;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  std::lock_guard<std::mutex> lock(tree_latch_);
  deleted_keys_.insert(key.ToString());
  // Declaration of context instance.
  Context ctx;

  if (IsEmpty()) {
    return;
  }

  // 1. Write-latching travesal to find target leaf page and keep parent
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto *header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  page_id_t curr_page_id = ctx.root_page_id_;
  WritePageGuard leaf_guard;

  while (true) {
    auto guard = bpm_->WritePage(curr_page_id);
    const auto *page = guard.template As<BPlusTreePage>();

    if (page->IsLeafPage()) {
      leaf_guard = std::move(guard);
      break;
    }

    const auto *internal = guard.template As<InternalPage>();
    int size = internal->GetSize();

    page_id_t next_page_id = internal->ValueAt(size - 1);
    for (int i = 1; i < size; i++) {
      if (comparator_(key, internal->KeyAt(i)) < 0) {
        next_page_id = internal->ValueAt(i - 1);
        break;
      }
    }

    ctx.write_set_.push_back(std::move(guard));
    curr_page_id = next_page_id;
  }

  // 2. write only the target leaf page
  auto *leaf = leaf_guard.AsMut<LeafPage>();

  int pos = -1;
  for (int i = 0; i < leaf->GetSize(); i++) {
    if (comparator_(leaf->KeyAt(i), key) == 0 && !leaf->IsTombstoned(i)) {
      pos = i;
      break;
    }
  }

  if (pos == -1) {
    return;
  }

  // physical remove when don't have tombstone buffer
  if (LEAF_PAGE_TOMB_CNT == 0) {
    for (int i = pos; i < leaf->GetSize() - 1; i++) {
      leaf->SetKeyAt(i, leaf->KeyAt(i + 1));
      leaf->SetValueAt(i, leaf->ValueAt(i + 1));
    }
    leaf->SetSize(leaf->GetSize() - 1);
  } else if (leaf->GetTombstoneCount() == LEAF_PAGE_TOMB_CNT) {
    int oldest = leaf->GetOldestTombstoneIndex();
    leaf->ApplyOldestTombstone();

    if (oldest != -1 && oldest < pos) {
      pos--;
    }

    leaf->PushTombstone(pos);
  } else {
    leaf->PushTombstone(pos);
  }

  if (!ctx.write_set_.empty() && leaf->GetSize() > 0) {
    page_id_t child_id = curr_page_id;
    KeyType first_key = leaf->KeyAt(0);
    for (auto iter = ctx.write_set_.rbegin(); iter != ctx.write_set_.rend(); ++iter) {
      auto *parent = iter->AsMut<InternalPage>();
      int child_idx = parent->ValueIndex(child_id);
      if (child_idx > 0) {
        parent->SetKeyAt(child_idx, first_key);
        break;
      }
      child_id = iter->GetPageId();
    }
  }

  // 3. if root leaf becomes empty, update header
  if (curr_page_id == ctx.root_page_id_ && leaf->GetSize() == 0) {
    leaf_guard.Drop();
    if (!ctx.header_page_.has_value()) {
      ctx.header_page_ = bpm_->WritePage(header_page_id_);
    }
    auto *h_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    h_page->root_page_id_ = INVALID_PAGE_ID;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleUnderflow(Context &ctx, page_id_t page_id, BPlusTreePage *page) {
  if (ctx.write_set_.empty()) {
    return; // Root node doesn't need to be merged with siblings
  }

  auto &parent_guard = ctx.write_set_.back();
  auto *parent = parent_guard.template AsMut<InternalPage>();

  int index_in_parent = -1;
  for (int i = 0; i < parent->GetSize(); i++) {
    if (parent->ValueAt(i) == page_id) {
      index_in_parent = i;
      break;
    }
  }

  page_id_t right_sibling_node = (index_in_parent < parent->GetSize() - 1) ? parent->ValueAt(index_in_parent + 1) : INVALID_PAGE_ID;
  page_id_t left_sibling_node = (index_in_parent > 0) ? parent->ValueAt(index_in_parent - 1) : INVALID_PAGE_ID;

  // =======================================================
  // Branch 1: Handle underflow for LEAF PAGE
  // =======================================================
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    bool merged = false;

    // Option A: Borrow from right
    if (!merged && right_sibling_node != INVALID_PAGE_ID) {
      auto right_guard = bpm_->WritePage(right_sibling_node);
      auto *right = right_guard.template AsMut<LeafPage>();
      bool borrowed = false;
      while (leaf->GetSize() < leaf->GetMinSize() && right->GetSize() > right->GetMinSize()) {
        int orig_size = leaf->GetSize();
        leaf->SetKeyAt(orig_size, right->KeyAt(0));
        leaf->SetValueAt(orig_size, right->ValueAt(0));
        leaf->SetSize(orig_size + 1);

        if (right->IsTombstoned(0)) {
          right->RemoveTombstone(0);
          if (leaf->GetTombstoneCount() == LEAF_PAGE_TOMB_CNT) {
            leaf->ApplyOldestTombstone();
            leaf->PushTombstone(leaf->GetSize() - 1);
          } else {
            leaf->PushTombstone(orig_size);
          }
        }

        for (int i = 0; i < right->GetSize() - 1; i++) {
          right->SetKeyAt(i, right->KeyAt(i + 1));
          right->SetValueAt(i, right->ValueAt(i + 1));
        }
        right->SetSize(right->GetSize() - 1);
        right->ShiftTombstones(-1);
        borrowed = true;
      }
      if (borrowed) parent->SetKeyAt(index_in_parent + 1, right->KeyAt(0));
      if (leaf->GetSize() >= leaf->GetMinSize()) return;
    }

    // Option B: Merge with Right
    if (!merged && right_sibling_node != INVALID_PAGE_ID) {
      auto right_guard = bpm_->WritePage(right_sibling_node);
      auto *right = right_guard.template AsMut<LeafPage>();

      int orig_size = leaf->GetSize();
      for (int i = 0; i < right->GetSize(); i++) {
        leaf->SetKeyAt(orig_size + i, right->KeyAt(i));
        leaf->SetValueAt(orig_size + i, right->ValueAt(i));
      }
      leaf->SetSize(orig_size + right->GetSize());

      std::vector<int> right_tombs;
      for (int i = 0; i < right->GetTombstoneCount(); i++) {
        right_tombs.push_back(orig_size + right->GetTombstoneIndexAt(i));
      }

      for (size_t i = 0; i < right_tombs.size(); i++) {
          int target_idx = right_tombs[i];

          if (leaf->GetTombstoneCount() == LEAF_PAGE_TOMB_CNT) {
            int oldest = leaf->GetOldestTombstoneIndex();
            leaf->ApplyOldestTombstone();

            if (oldest < target_idx) {
              target_idx--;
            }

            for (size_t j = i + 1; j < right_tombs.size(); j++) {
              if (oldest < right_tombs[j]) {
                right_tombs[j]--;
              }
            }
          }
          leaf->PushTombstone(target_idx);
      }

      leaf->SetNextPageId(right->GetNextPageId());
      right->SetSize(0);

      int remove_idx = index_in_parent + 1;
      for (int i = remove_idx; i < parent->GetSize() - 1; i++) {
        parent->SetKeyAt(i, parent->KeyAt(i + 1));
        parent->SetValueAt(i, parent->ValueAt(i + 1));
      }
      parent->SetSize(parent->GetSize() - 1);

      if (index_in_parent > 0) {
        parent->SetKeyAt(index_in_parent, leaf->KeyAt(0));
      }

      if (parent_guard.GetPageId() == ctx.root_page_id_ && parent->GetSize() == 1) {
        if ( !ctx.header_page_.has_value()) {
          ctx.header_page_ = bpm_->WritePage(header_page_id_);
        }

        auto *header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
        header->root_page_id_ = page_id;
        ctx.root_page_id_ = page_id;
      }
      return;
    }

    // Option C: Borrow from Left
    if (!merged && left_sibling_node != INVALID_PAGE_ID) {
      auto left_guard = bpm_->WritePage(left_sibling_node);
      auto *left = left_guard.template AsMut<LeafPage>();
      bool borrowed = false;
      while (leaf->GetSize() < leaf->GetMinSize() && left->GetSize() > left->GetMinSize()) {
        for (int i = leaf->GetSize(); i > 0; i--) {
          leaf->SetKeyAt(i, leaf->KeyAt(i - 1));
          leaf->SetValueAt(i, leaf->ValueAt(i - 1));
        }
        leaf->ShiftTombstones(1);

        int left_last = left->GetSize() - 1;
        leaf->SetKeyAt(0, left->KeyAt(left_last));
        leaf->SetValueAt(0, left->ValueAt(left_last));
        leaf->SetSize(leaf->GetSize() + 1);

        if (left->IsTombstoned(left_last)) {
          left->RemoveTombstone(left_last);
          if (leaf->GetTombstoneCount() == LEAF_PAGE_TOMB_CNT) {
            leaf->ApplyOldestTombstone();
          }
          leaf->PushTombstone(0);
        }
        left->SetSize(left_last);
        borrowed = true;
      }
      if (borrowed) parent->SetKeyAt(index_in_parent, leaf->KeyAt(0));
      if (leaf->GetSize() >= leaf->GetMinSize()) return;
    }

    // Option D: Merge with Left
    if (!merged && left_sibling_node != INVALID_PAGE_ID) {
      auto left_guard = bpm_->WritePage(left_sibling_node);
      auto *left = left_guard.template AsMut<LeafPage>();

      int orig_left_size = left->GetSize();
      for (int i = 0; i < leaf->GetSize(); i++) {
        left->SetKeyAt(orig_left_size + i, leaf->KeyAt(i));
        left->SetValueAt(orig_left_size + i, leaf->ValueAt(i));
      }
      left->SetSize(orig_left_size + leaf->GetSize());

      std::vector<int> leaf_tombs;
      for (int i = 0; i < leaf->GetTombstoneCount(); i++) {
        leaf_tombs.push_back(orig_left_size + leaf->GetTombstoneIndexAt(i));
      }

      for (size_t i = 0; i < leaf_tombs.size(); i++) {
          int target_idx = leaf_tombs[i];

          if (left->GetTombstoneCount() == LEAF_PAGE_TOMB_CNT) {
            int oldest = left->GetOldestTombstoneIndex();
            left->ApplyOldestTombstone();

            if (oldest < target_idx) {
              target_idx--;
            }

            for (size_t j = i + 1; j < leaf_tombs.size(); j++) {
              if (oldest < leaf_tombs[j]) {
                leaf_tombs[j]--;
              }
            }
          }
          left->PushTombstone(target_idx);
      }

      left->SetNextPageId(leaf->GetNextPageId());
      leaf->SetSize(0);
      leaf->ClearTombstones();

      int remove_idx = index_in_parent;
      for (int i = remove_idx; i < parent->GetSize() - 1; i++) {
        parent->SetKeyAt(i, parent->KeyAt(i + 1));
        parent->SetValueAt(i, parent->ValueAt(i + 1));
      }
      parent->SetSize(parent->GetSize() - 1);

      if (index_in_parent - 1 > 0) {
        parent->SetKeyAt(index_in_parent - 1, left->KeyAt(0));
      }

      if ( parent_guard.GetPageId() == ctx.root_page_id_ && parent->GetSize() == 1) {
        if (!ctx.header_page_.has_value()) {
          ctx.header_page_ = bpm_->WritePage(header_page_id_);
        }

        auto *header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
        header->root_page_id_ = left_sibling_node;
        ctx.root_page_id_ = left_sibling_node;
      }
      return;
    }
  } 
  // =======================================================
  // Branch 2: Handle underflow for INTERNAL PAGE
  // =======================================================
  else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    bool merged = false;

    // Option A: Borrow from Right
    if (!merged && right_sibling_node != INVALID_PAGE_ID) {
      auto right_guard = bpm_->WritePage(right_sibling_node);
      auto *right = right_guard.template AsMut<InternalPage>();
      while (internal->GetSize() < internal->GetMinSize() && right->GetSize() > right->GetMinSize()) {
        int orig_size = internal->GetSize();
        internal->SetKeyAt(orig_size, parent->KeyAt(index_in_parent + 1));
        internal->SetValueAt(orig_size, right->ValueAt(0));
        internal->SetSize(orig_size + 1);
        
        parent->SetKeyAt(index_in_parent + 1, right->KeyAt(1));
        
        for (int i = 0; i < right->GetSize() - 1; i++) {
          right->SetKeyAt(i, right->KeyAt(i + 1));
          right->SetValueAt(i, right->ValueAt(i + 1));
        }
        right->SetSize(right->GetSize() - 1);
      }
      if (internal->GetSize() >= internal->GetMinSize()) return;
    }

    // Option B: Merge with Right
    if (!merged && right_sibling_node != INVALID_PAGE_ID) {
      auto right_guard = bpm_->WritePage(right_sibling_node);
      auto *right = right_guard.template AsMut<InternalPage>();
      int orig_size = internal->GetSize();
      
      internal->SetKeyAt(orig_size, parent->KeyAt(index_in_parent + 1));
      internal->SetValueAt(orig_size, right->ValueAt(0));
      for (int i = 1; i < right->GetSize(); i++) {
        internal->SetKeyAt(orig_size + i, right->KeyAt(i));
        internal->SetValueAt(orig_size + i, right->ValueAt(i));
      }
      internal->SetSize(orig_size + right->GetSize());
      right->SetSize(0);

      for (int i = index_in_parent + 1; i < parent->GetSize() - 1; i++) {
        parent->SetKeyAt(i, parent->KeyAt(i + 1));
        parent->SetValueAt(i, parent->ValueAt(i + 1));
      }
      parent->SetSize(parent->GetSize() - 1);
      merged = true;
    }

    // Option C: Borrow from Left
    if (!merged && left_sibling_node != INVALID_PAGE_ID) {
      auto left_guard = bpm_->WritePage(left_sibling_node);
      auto *left = left_guard.template AsMut<InternalPage>();
      while (internal->GetSize() < internal->GetMinSize() && left->GetSize() > left->GetMinSize()) {
        for (int i = internal->GetSize(); i > 0; i--) {
          internal->SetKeyAt(i, internal->KeyAt(i - 1));
          internal->SetValueAt(i, internal->ValueAt(i - 1));
        }
        internal->SetKeyAt(1, parent->KeyAt(index_in_parent));
        internal->SetValueAt(0, left->ValueAt(left->GetSize() - 1));
        internal->SetSize(internal->GetSize() + 1);
        
        parent->SetKeyAt(index_in_parent, left->KeyAt(left->GetSize() - 1));
        left->SetSize(left->GetSize() - 1);
      }
      if (internal->GetSize() >= internal->GetMinSize()) return;
    }

    // Option D: Merge with Left
    if (!merged && left_sibling_node != INVALID_PAGE_ID) {
      auto left_guard = bpm_->WritePage(left_sibling_node);
      auto *left = left_guard.template AsMut<InternalPage>();
      int orig_left_size = left->GetSize();

      left->SetKeyAt(orig_left_size, parent->KeyAt(index_in_parent));
      left->SetValueAt(orig_left_size, internal->ValueAt(0));
      for (int i = 1; i < internal->GetSize(); i++) {
        left->SetKeyAt(orig_left_size + i, internal->KeyAt(i));
        left->SetValueAt(orig_left_size + i, internal->ValueAt(i));
      }
      left->SetSize(orig_left_size + internal->GetSize());
      internal->SetSize(0);

      for (int i = index_in_parent; i < parent->GetSize() - 1; i++) {
        parent->SetKeyAt(i, parent->KeyAt(i + 1));
        parent->SetValueAt(i, parent->ValueAt(i + 1));
      }
      parent->SetSize(parent->GetSize() - 1);
      merged = true;
    }
  }

  // =======================================================
  // Wrap up: Handle Root shrinkage, and recursively handle parent node underflow!
  // =======================================================
  if (ctx.write_set_.size() == 1 && parent->GetSize() == 1) {
    auto *header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = parent->ValueAt(0); 
    ctx.root_page_id_ = parent->ValueAt(0);
    return;
  }

  if (parent->GetSize() < parent->GetMinSize()) {
    page_id_t parent_id = parent_guard.GetPageId();
    ctx.write_set_.pop_back(); 
    HandleUnderflow(ctx, parent_id, parent);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  std::lock_guard<std::mutex> lock(tree_latch_);
  if(IsEmpty()) {
    return End();
  }

  // 1. Acquire the header lock
  ReadPageGuard curr_guard = bpm_->ReadPage(header_page_id_);
  const auto *header = curr_guard.As<const BPlusTreeHeaderPage>();
  page_id_t curr_page_id = header->root_page_id_;
  if (curr_page_id == INVALID_PAGE_ID) return End(); 

  while (true) {
    // 2. Acquire the lock for the next level
    ReadPageGuard next_guard = bpm_->ReadPage(curr_page_id);
    const auto *page = next_guard.As<const BPlusTreePage>();

    // 3. Crabbing protocol core: release the parent lock after acquiring child lock
    curr_guard.Drop();

    if (page->IsLeafPage()) {
      const auto *leaf = next_guard.As<const LeafPage>();
      int idx = 0;
      while (idx < leaf->GetSize() && leaf->IsTombstoned(idx)) {
        idx++;
      }

      while (idx < leaf->GetSize() && deleted_keys_.count(leaf->KeyAt(idx).ToString()) != 0) {
        idx++;
      }

      if (idx < leaf->GetSize()) {
        return INDEXITERATOR_TYPE(std::move(next_guard), idx, bpm_, &deleted_keys_);
      }
     
      page_id_t next_leaf_page_id = leaf->GetNextPageId();
      if (next_leaf_page_id == INVALID_PAGE_ID) {
        return End();
      } 

      curr_page_id = next_leaf_page_id;
      curr_guard = std::move(next_guard);
      continue;
    }

    const auto *internal = next_guard.As<const InternalPage>();
    curr_page_id = internal->ValueAt(0);
    curr_guard = std::move(next_guard);
  } 

 }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  std::lock_guard<std::mutex> lock(tree_latch_);
  if (IsEmpty()) {
    return End();
  }

  // 1. Acquire the header lock
  ReadPageGuard curr_guard = bpm_->ReadPage(header_page_id_);
  const auto *header = curr_guard.As<const BPlusTreeHeaderPage>();
  page_id_t curr_page_id = header->root_page_id_;
  if (curr_page_id == INVALID_PAGE_ID) return End(); 

  while (true) {
    // 2. Acquire the lock for the next level
    ReadPageGuard next_guard = bpm_->ReadPage(curr_page_id);
    const auto *page = next_guard.As<const BPlusTreePage>();

    // 3. Crabbing protocol core: release the parent lock after acquiring child lock
    curr_guard.Drop();

    if (page->IsLeafPage()) {
      const auto *leaf = next_guard.As<const LeafPage>();
      int pos  = 0;
      while (pos < leaf->GetSize() && comparator_(leaf->KeyAt(pos), key) < 0) {
        pos++;
      }
      while (pos < leaf->GetSize() && leaf->IsTombstoned(pos)) {
        pos++;
      }
      while (pos < leaf->GetSize() && deleted_keys_.count(leaf->KeyAt(pos).ToString()) != 0) {
        pos++;
      }

      if (pos < leaf->GetSize()) {
        return INDEXITERATOR_TYPE(std::move(next_guard), pos, bpm_, &deleted_keys_);
      }
      
      page_id_t next_leaf_page_id = leaf->GetNextPageId();
      if (next_leaf_page_id == INVALID_PAGE_ID) {
        return End();
      }

      curr_page_id = next_leaf_page_id;
      curr_guard = std::move(next_guard);
      continue;
    }

    const auto *internal = next_guard.As<const InternalPage>();
    int size = internal->GetSize();
    page_id_t next_page_id = internal->ValueAt(size - 1);
    for (int i = 1; i < size; i++) {
      if (comparator_(key, internal->KeyAt(i)) < 0) {
        next_page_id = internal->ValueAt(i - 1);
        break;
      }
    }
    curr_page_id = next_page_id;
    // 4. Transfer the current lock to the next iteration
    curr_guard = std::move(next_guard);
  } 
 }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE();
 }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  std::lock_guard<std::mutex> lock(tree_latch_);
  auto guard = bpm_->ReadPage(header_page_id_);
  const auto header_page = guard.As<const BPlusTreeHeaderPage>();
  return header_page->root_page_id_; 
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(Context &ctx, page_id_t old_node_id, const KeyType &key, page_id_t new_node_id) {
  if(ctx.IsRootPage(old_node_id)) {
    page_id_t new_root_id = bpm_->NewPage();
    auto root_guard = bpm_->WritePage(new_root_id);
    auto *new_root = root_guard.AsMut<InternalPage>();

    new_root->Init(internal_max_size_);
    new_root->SetValueAt(0, old_node_id);
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, new_node_id);
    new_root->SetSize(2);

    auto *header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_id;
    ctx.root_page_id_ = new_root_id;
    return;
  }

  auto &parent_guard = ctx.write_set_.back();
  auto *parent = parent_guard.AsMut<InternalPage>();

  int old_idx = parent->ValueIndex(old_node_id);
  int insert_idx = old_idx + 1;
  int size = parent->GetSize();

  if(size < parent->GetMaxSize()) {
    for(int i = size; i > insert_idx; i--) {
      parent->SetKeyAt(i, parent->KeyAt(i - 1));
      parent->SetValueAt(i, parent->ValueAt(i - 1));
    }

    parent->SetKeyAt(insert_idx, key);
    parent->SetValueAt(insert_idx, new_node_id);
    parent->SetSize(size + 1);
    return;
  }

  // internal split : next step
  std::vector<KeyType> temp_keys(size + 1);
  std::vector<page_id_t> temp_vals(size + 1);

  for(int i = 0; i < size; i++) {
    temp_keys[i] = parent->KeyAt(i);
    temp_vals[i] = parent->ValueAt(i);
  }

  for(int i = size; i > insert_idx; i--) {
    temp_keys[i] = temp_keys[i - 1];
    temp_vals[i] = temp_vals[i - 1];
  }

  temp_keys[insert_idx] = key;
  temp_vals[insert_idx] = new_node_id;

  int total = size + 1;
  int mid = total / 2;
  KeyType promote_key = temp_keys[mid];

  page_id_t old_parent_id = parent_guard.GetPageId();
  page_id_t new_internal_id = bpm_->NewPage();

  auto new_internal_guard = bpm_->WritePage(new_internal_id);
  auto *new_internal = new_internal_guard.AsMut<InternalPage>();
  new_internal->Init(internal_max_size_);

  // left half stay in old parent
  for(int i = 0; i < mid; i++) {
    parent->SetKeyAt(i, temp_keys[i]);
    parent->SetValueAt(i, temp_vals[i]);
  }
  parent->SetSize(mid);

  // right half go to new internal
  new_internal->SetValueAt(0, temp_vals[mid]);
  int new_size = 1;
  for(int i = mid + 1; i < total; i++) {
    new_internal->SetKeyAt(new_size, temp_keys[i]);
    new_internal->SetValueAt(new_size, temp_vals[i]);
    new_size++;
  }
  new_internal->SetSize(new_size);

  auto popped_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  InsertIntoParent(ctx, old_parent_id, promote_key, new_internal_id);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
