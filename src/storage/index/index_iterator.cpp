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
  // 【终极安全判断】：直接看 bpm_ 智能指针是否被我们置空，绝不触碰底层 page_guard 的任何断言方法！
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
    while (index_ < leaf->GetSize() && leaf->IsTombstoned(index_)){
      index_++;
    }

    // 2. 如果跳过墓碑后，索引还在当前页内，说明找到了有效的下一个元素，直接返回
    if (index_ < leaf->GetSize()) {
      return *this;
    }

    // 3. 当前页的有效元素耗尽，需要跳到下一页
    page_id_t next_page_id = leaf->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      // 走到树的尽头了，释放锁
      guard_.Drop();
      bpm_ = nullptr; // 【终极救星】：主动将自身 bpm_ 置空，表明已进入 End 状态
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

  // 两个都不是 End 时，才去调用 GetPageId()，因为此时两个锁都是合法的，绝对不会触发 Assert！
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