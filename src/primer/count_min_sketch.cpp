//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>
#include <algorithm>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth), matrix_(width * depth, 0), locks_(std::make_unique<std::mutex[]>(depth)) {
  /** @TODO(student) Implement this function! */
  if(width == 0 || depth == 0) {
    throw std::invalid_argument("width and depth must be greater than 0");
  }

  /** @spring2026 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept 
  : width_(other.width_), depth_(other.depth_),
  matrix_(std::move(other.matrix_)),
  locks_(std::move(other.locks_)) {
  /** @TODO(student) Implement this function! */
  other.depth_ = 0;
  other.width_ = 0;

  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if(this == &other) {
    return *this;
  }

  width_ = other.width_;
  depth_ = other.depth_;
  matrix_ = std::move(other.matrix_);
  locks_ = std::move(other.locks_);
  hash_functions_ = std::move(other.hash_functions_);

  other.width_ = 0;
  other.depth_ = 0;

  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for(size_t i = 0; i < depth_; ++i) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  for(size_t i = 0; i < depth_; ++i) {
    uint64_t hash_val = hash_functions_[i](item);
    size_t col = hash_val % width_;

    std::lock_guard<std::mutex> lock(locks_[i]);

    matrix_[i * width_ + col]++;
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  for(size_t i = 0; i < depth_; ++i) {
    std::lock_guard<std::mutex> lock(locks_[i]);

    for(size_t j = 0; j < width_; ++j) {
      size_t index = i * width_ + j;
      matrix_[index] += other.matrix_[index];
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t min_count = UINT32_MAX;
  for(size_t i = 0; i < depth_; ++i) {
    uint64_t hash_val = hash_functions_[i](item);
    size_t col = hash_val % width_;

    uint32_t current_count = matrix_[i * width_ + col];
    min_count = std::min(min_count,current_count);
  }
  return min_count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  std::fill(matrix_.begin(),matrix_.end(),0);
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  std::vector<std::pair<KeyType,uint32_t>> results;

  results.reserve(candidates.size());

  for(const auto &candidate : candidates) {
    uint32_t count = this->Count(candidate);
    results.push_back({candidate,count});
  }

  std::sort(results.begin(), results.end(), 
            [](const std::pair<KeyType, uint32_t> &a, const std::pair<KeyType, uint32_t> &b) {
              return a.second > b.second; 
            });

  size_t result_size = std::min(static_cast<size_t>(k), results.size());

  return std::vector<std::pair<KeyType, uint32_t>>(results.begin(), results.begin() + result_size);
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
