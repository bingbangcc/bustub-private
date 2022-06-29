//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_capacity_(num_pages) {
  frame_list_.clear();
  lru_map_.clear();
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  // latch_.lock();
  if (frame_list_.empty()) {
    // latch_.unlock();
    return false;
  }

  *frame_id = frame_list_.back();
  lru_map_.erase(frame_list_.back());
  frame_list_.pop_back();
  // latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  // latch_.lock();
  if (lru_map_.count(frame_id) != 0) {
    frame_list_.erase(lru_map_[frame_id]);
    lru_map_.erase(frame_id);
  }
  // latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  // latch_.lock();
  if (lru_map_.count(frame_id) != 0) {
    // latch_.unlock();
    return;
  }
  while (this->Size() >= max_capacity_) {
    lru_map_.erase(frame_list_.back());
    frame_list_.pop_back();
  }
  frame_list_.push_front(frame_id);
  lru_map_.insert(std::make_pair(frame_id, frame_list_.begin()));
  // latch_.unlock();
}

size_t LRUReplacer::Size() { return frame_list_.size(); }

}  // namespace bustub
