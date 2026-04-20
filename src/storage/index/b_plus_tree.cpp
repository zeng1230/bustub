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
    // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  if(IsEmpty()) {
    return false;
  }

  page_id_t page_id = GetRootPageId();

  while(true) {
    auto guard = bpm_->ReadPage(page_id);
    const auto *page = guard.As<const BPlusTreePage>();

    if(page->IsLeafPage()) {
      const auto *leaf = guard.As<const LeafPage>();
      for(int i = 0; i < leaf->GetSize(); i++) {
        if(comparator_(key,leaf->KeyAt(i)) == 0) {
          result->push_back(leaf->ValueAt(i));
          return true;
        }
      }
      return false;
    }

    const auto *internal = guard.As<const InternalPage>();
    int size = internal->GetSize();

    page_id_t next_page_id = internal->ValueAt(size - 1);
    for(int i = 1; i < size; i++) {
      if(comparator_(key,internal->KeyAt(i)) < 0) {
        next_page_id = internal->ValueAt(i - 1);
        break;
      }
    }

    page_id = next_page_id;
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

      leaf->Init();
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
        return true;
      }

      // 2.3 leaf full : split
      std::vector<KeyType> temp_keys;
      std::vector<ValueType> temp_values;
      temp_keys.reserve(size + 1);
      temp_values.reserve(size + 1);

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
      new_leaf->Init();

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
  // Declaration of context instance.
  Context ctx;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
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
  if(IsEmpty()) {
    return End();
  }

  page_id_t curr_page_id = GetRootPageId();

  while(true) {
    auto guard = bpm_->ReadPage(curr_page_id);
    const auto *page = guard.As<const BPlusTreePage>();

    if(page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(std::move(guard),0,bpm_);
    }

    const auto *internal = guard.As<const InternalPage>();
    curr_page_id = internal->ValueAt(0);
  }
 }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  if (IsEmpty()) {
    return End();
  }

  page_id_t curr_page_id = GetRootPageId();

  while (true) {
    auto guard = bpm_->ReadPage(curr_page_id);
    const auto *page = guard.As<const BPlusTreePage>();

    if (page->IsLeafPage()) {
      const auto *leaf = guard.As<const LeafPage>();

      int pos = 0;
      while (pos < leaf->GetSize() && comparator_(leaf->KeyAt(pos), key) < 0) {
        pos++;
      }

      if (pos < leaf->GetSize()) {
        return INDEXITERATOR_TYPE(std::move(guard), pos, bpm_);
      }

      page_id_t next_page_id = leaf->GetNextPageId();
      if (next_page_id == INVALID_PAGE_ID) {
        return End();
      }

      auto next_guard = bpm_->ReadPage(next_page_id);
      return INDEXITERATOR_TYPE(std::move(next_guard), 0, bpm_);
    }

    const auto *internal = guard.As<const InternalPage>();
    int size = internal->GetSize();

    page_id_t next_page_id = internal->ValueAt(size - 1);
    for (int i = 1; i < size; i++) {
      if (comparator_(key, internal->KeyAt(i)) < 0) {
        next_page_id = internal->ValueAt(i - 1);
        break;
      }
    }

    curr_page_id = next_page_id;
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
