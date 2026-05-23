//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(ReadPageGuard guard, int index, std::shared_ptr<TracedBufferPoolManager> bpm,
                                  const std::unordered_set<int64_t> *deleted_keys)
  : guard_(std::move(guard)), index_(index), bpm_(std::move(bpm)), deleted_keys_(deleted_keys) {}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool { 
  return bpm_ == nullptr;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  const auto *leaf = guard_.As<const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
  return {leaf->KeyAt(index_), leaf->ValueAt(index_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
  if (IsEnd()) {
    return *this;
  }

  // 步进到下一个元素
  index_++;

  while (true) {
    const auto *leaf = guard_.As<const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();

    // 1. 在当前页内不断跳过墓碑 (Tombstones)
    while (index_ < leaf->GetSize() &&
           (leaf->IsTombstoned(index_) ||
            (deleted_keys_ != nullptr && deleted_keys_->count(leaf->KeyAt(index_).ToString()) != 0))) {
      index_++;
    }

    // 2. 如果跳过墓碑后，索引还在当前页内，说明找到了有效的下一个元素，直接返回
    if (index_ < leaf->GetSize()) {
      return *this;
    }

    // 3. 当前页的有效元素耗尽，需要跳到下一页
    page_id_t next_page_id = leaf->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      // 走到树的尽头了，释放锁，标记为 End
      guard_.Drop();
      bpm_ = nullptr;
      return *this;
    }

    // 4. 【核心并发螃蟹锁】：先拿下一页的锁，再释放当前页的锁
    ReadPageGuard next_guard = bpm_->ReadPage(next_page_id);
    guard_.Drop();
    
    // 5. 将锁的所有权转移给当前迭代器，准备进入下一轮循环
    guard_ = std::move(next_guard);
    
    // 切换到了新的一页，必须把索引重置为 0，然后继续 while 循环跳过可能位于新页开头的墓碑
    index_ = 0;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  bool this_end = this->IsEnd();
  bool that_end = itr.IsEnd();

  if (this_end && that_end) {
    return true;
  }

  if (this_end || that_end) {
    return false;
  }

  return this->index_ == itr.index_ && this->guard_.GetPageId() == itr.guard_.GetPageId();
 }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { 
  return !(*this == itr);
 }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
