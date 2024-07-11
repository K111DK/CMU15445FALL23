//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::recursive_mutex> guard(lock_);
  if (free_list_.empty()) {
    frame_id_t victim_frame_id;
    if (replacer_->Evict(&victim_frame_id)) {
      auto &victim_page = pages_[victim_frame_id];
      if (victim_page.is_dirty_) {
        FlushPage(victim_page.page_id_);
      }
      page_table_.erase(victim_page.page_id_);
      auto new_page_id = AllocatePage();
      victim_page.page_id_ = new_page_id;
      victim_page.is_dirty_ = false;
      victim_page.pin_count_ = 1;
      page_table_[new_page_id] = victim_frame_id;
      *page_id = new_page_id;

      replacer_->RecordAccess(victim_frame_id, AccessType::Unknown);
      replacer_->SetEvictable(victim_frame_id, false);
      return &pages_[victim_frame_id];
    }
    return nullptr;
  }

  frame_id_t free_frame_id = *free_list_.begin();
  free_list_.erase(free_list_.begin());

  replacer_->RecordAccess(free_frame_id, AccessType::Unknown);
  replacer_->SetEvictable(free_frame_id, false);

  auto new_page_id = AllocatePage();
  *page_id = new_page_id;
  page_table_[new_page_id] = free_frame_id;
  auto &page = pages_[free_frame_id];
  page.pin_count_ = 1;
  page.is_dirty_ = false;
  page.page_id_ = new_page_id;

  return &pages_[free_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::recursive_mutex> guard(lock_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  auto res = page_table_.find(page_id);
  if (res != page_table_.end()) {
    auto frame_id = (*res).second;
    auto &page = pages_[frame_id];
    if (page.page_id_ == page_id) {
      page.pin_count_++;
      replacer_->RecordAccess(frame_id, AccessType::Unknown);
      replacer_->SetEvictable(frame_id, false);
      return &pages_[frame_id];
    }
  }

  if (free_list_.empty()) {
    frame_id_t victim_frame_id;
    if (replacer_->Evict(&victim_frame_id)) {
      auto &victim_page = pages_[victim_frame_id];
      if (victim_page.is_dirty_) {
        FlushPage(victim_page.page_id_);
      }
      page_table_.erase(victim_page.page_id_);
      page_table_[page_id] = victim_frame_id;
      victim_page.ResetMemory();
      auto promise1 = disk_scheduler_->CreatePromise();
      auto future1 = promise1.get_future();
      disk_scheduler_->Schedule({false, victim_page.GetData(), page_id, std::move(promise1)});
      future1.get();
      victim_page.page_id_ = page_id;
      victim_page.is_dirty_ = false;
      victim_page.pin_count_ = 1;

      replacer_->RecordAccess(victim_frame_id, AccessType::Unknown);
      replacer_->SetEvictable(victim_frame_id, false);
      return &pages_[victim_frame_id];
    }
    return nullptr;
  }

  frame_id_t free_frame_id = *free_list_.begin();
  free_list_.erase(free_list_.begin());

  replacer_->RecordAccess(free_frame_id, AccessType::Unknown);
  replacer_->SetEvictable(free_frame_id, false);

  page_table_[page_id] = free_frame_id;
  auto &page = pages_[free_frame_id];
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;
  page.ResetMemory();

  auto promise1 = disk_scheduler_->CreatePromise();
  auto future1 = promise1.get_future();
  disk_scheduler_->Schedule({false, page.GetData(), page.GetPageId(), std::move(promise1)});
  future1.get();

  return &pages_[free_frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::recursive_mutex> guard(lock_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto res = page_table_.find(page_id);
  if (res != page_table_.end()) {
    auto frame_id = (*res).second;
    auto &page = pages_[frame_id];

    if (page.pin_count_ <= 0) {
      return false;
    }

    page.pin_count_--;
    if (page.pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }

    page.is_dirty_ = page.is_dirty_ || is_dirty;
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> guard(lock_);
  auto res = page_table_.find(page_id);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (res != page_table_.end()) {
    auto frame_id = (*res).second;
    auto &page = pages_[frame_id];
    if (page.page_id_ == page_id) {
      auto promise1 = disk_scheduler_->CreatePromise();
      auto future1 = promise1.get_future();
      disk_scheduler_->Schedule({true, page.data_, page.page_id_, std::move(promise1)});
      future1.get();
      page.is_dirty_ = false;
      return true;
    }
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::recursive_mutex> guard(lock_);
  std::vector<std::future<bool>> flush_future{};
  for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
    auto &page = pages_[frame_id];
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule({true, page.data_, page.page_id_, std::move(promise1)});
    flush_future.push_back(std::move(future1));
  }

  // sync
  for (auto &future : flush_future) {
    future.get();
  }

  // change dirty state
  for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
    auto &page = pages_[frame_id];
    page.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> guard(lock_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto res = page_table_.find(page_id);
  if (res != page_table_.end()) {
    auto frame_id = (*res).second;
    auto &page = pages_[frame_id];
    if (page.page_id_ == page_id) {
      if (page.pin_count_ == 0) {
        FlushPage(page.page_id_);
        page_table_.erase(page_id);
        replacer_->Remove(frame_id);
        DeallocatePage(page_id);
        // free_list_.push_front(frame_id);
        page.ResetMemory();
        free_list_.push_front(frame_id);
        return true;
      }
      // inevitable
      return false;
    }
  }
  // not found in page_table or bufferPool
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
