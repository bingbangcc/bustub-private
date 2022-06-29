//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  // latch_.lock();
  // 如果page已经在内存里
  if (page_table_.count(page_id) != 0) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = pages_ + frame_id;
    page->pin_count_++;
    replacer_->Pin(frame_id);
    // latch_.unlock();
    return page;
  }
  // 如果没有空间再分配，则返回false
  if (free_list_.empty() && replacer_->Size() == 0) {
    // latch_.unlock();
    return nullptr;
  }
  // 如果page不在内存里，先获得可以插入的frame
  frame_id_t frame_id = 0;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
  }
  // 如果之前的frame是脏的，需要先把脏数据写会
  Page *page = pages_ + frame_id;

  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    // FlushPgImp(page->page_id_);
  }
  page_table_.erase(page->page_id_);
  // 将新数据读入，并将该page的信息设置为初始值
  disk_manager_->ReadPage(page_id, page->data_);
  page->page_id_ = page_id;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  // 修改页表
  page_table_[page_id] = frame_id;
  page->pin_count_++;
  replacer_->Pin(frame_id);

  // latch_.unlock();
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  // unpin是将page加入到lru_replacer里，即可被替换
  // LOG_INFO("function : UnpinPgImp");
  std::lock_guard<std::mutex> guard(latch_);
  // latch_.lock();
  if (page_table_.count(page_id) == 0) {
    // latch_.unlock();
    return false;
  }
  // 找到目标页框
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  // 不用进行unpin
  if (page->pin_count_ == 0) {
    // latch_.unlock();
    return false;
  }
  // 进行unpin
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  // latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // LOG_INFO("function : FlushPgImp");
  std::lock_guard<std::mutex> guard(latch_);
  // latch_.lock();
  // if (page_id == INVALID_PAGE_ID) {
  //   latch_.unlock();
  //   return false;
  // }
  // 如果该page不在内存里，返回
  if (page_table_.count(page_id) == 0 || page_id == INVALID_PAGE_ID) {
    // latch_.unlock();
    return false;
  }
  // 找到page在内存的页框
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  // 如果该页框内的page_id不合法，返回
  // if (page->page_id_ != page_id) {
  //   latch_.unlock();
  //   return true;
  // }
  // 如果有线程占用该page，返回
  // if (page->pin_count_ > 0) {
  //   latch_.unlock();
  //   return true;
  // }
  // 将数据写入到硬盘
  disk_manager_->WritePage(page_id, page->data_);
  // latch_.unlock();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  // LOG_INFO("function : NewPgImp");
  std::lock_guard<std::mutex> guard(latch_);
  // latch_.lock();
  // 缓冲区已满，塞不进去了
  if (free_list_.empty() && replacer_->Size() == 0) {
    // latch_.unlock();
    return nullptr;
  }
  // 返回page_id
  *page_id = disk_manager_->AllocatePage();
  // 如果缓冲区还有空位，则挑一个
  frame_id_t frame_id = 0;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
  }

  Page *page = pages_ + frame_id;
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page_table_.erase(page->page_id_);
  // 将新数据读入，并将该page的信息设置为初始值
  // disk_manager_->ReadPage(*page_id, page->data_);
  page->page_id_ = *page_id;
  // page->pin_count_ = 0;
  page->is_dirty_ = false;
  // 修改页表
  page->ResetMemory();
  page_table_[*page_id] = frame_id;
  // 只有pin_count为0的时候才会被分配的
  page->pin_count_++;
  replacer_->Pin(frame_id);
  // latch_.unlock();
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  // delete是将page变为free_page
  std::lock_guard<std::mutex> guard(latch_);
  // latch_.lock();
  if (page_table_.count(page_id) == 0) {
    // latch_.unlock();
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;

  if (page->pin_count_ > 0) {
    // latch_.unlock();
    return false;
  }
  if (page->is_dirty_) {
    disk_manager_->WritePage(page_id, page->data_);
  }
  disk_manager_->DeallocatePage(page_id);
  // this->DeallocatePage(page_id);

  page_table_.erase(page_id);
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;

  page->ResetMemory();
  replacer_->Pin(frame_id);
  free_list_.push_back(frame_id);

  // latch_.unlock();

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // LOG_INFO("function : FlushAllPgsImp");
  // 对所有在内存里的page进行写出
  for (auto it : page_table_) {
    FlushPageImpl(it.first);
    // FlushPgImp(it.first);
  }
}

}  // namespace bustub
