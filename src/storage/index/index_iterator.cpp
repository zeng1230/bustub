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
INDEXITERATOR_TYPE::IndexIterator(ReadPageGuard guard, int index, std::shared_ptr<TracedBufferPoolManager> bpm)
  : guard_(std::move(guard)), index_(index), bpm_(bpm) {}

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
  if(IsEnd()) {
    return *this;
  }

  const auto *leaf = guard_.As<const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
  index_++;

  if(index_ < leaf->GetSize()) {
    return *this;
  }

  page_id_t next_page_id = leaf->GetNextPageId();
  if(next_page_id == INVALID_PAGE_ID) {
    guard_.Drop();
    bpm_ = nullptr;
    index_ = 0;
    return *this;
  }

  guard_ = bpm_->ReadPage(next_page_id);
  index_ = 0;
  return *this;
 }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (IsEnd() && itr.bpm_ == nullptr) {
    return true;
  }
  if (IsEnd() || itr.bpm_ == nullptr) {
    return false;
  }

  return index_ == itr.index_ && guard_.GetPageId() == itr.guard_.GetPageId();
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
