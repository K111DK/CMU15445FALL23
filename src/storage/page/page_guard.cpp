#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  std::scoped_lock<std::recursive_mutex, std::recursive_mutex> guard(b_latch_, that.b_latch_);
  if (&that != this) {
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
  }
}

void BasicPageGuard::Drop() {
  std::scoped_lock<std::recursive_mutex> guard(b_latch_);
  if ((bpm_ != nullptr) && (page_ != nullptr)) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_, AccessType::Unknown);
    bpm_ = nullptr;
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  std::scoped_lock<std::recursive_mutex, std::recursive_mutex> guard(b_latch_, that.b_latch_);
  if (&that == this) {
    return *this;
  }
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  std::scoped_lock<std::recursive_mutex> guard(b_latch_);
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->FetchPage(page_->GetPageId(), AccessType::Unknown);
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_, AccessType::Unknown);
    auto bpm_cp = bpm_;
    auto page_cp = page_;
    bpm_ = nullptr;
    page_ = nullptr;
    page_cp->RLatch();
    return {bpm_cp, page_cp};
  }
  return {nullptr, nullptr};
}
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  std::scoped_lock<std::recursive_mutex> guard(b_latch_);
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->FetchPage(page_->GetPageId(), AccessType::Unknown);
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_, AccessType::Unknown);
    auto bpm_cp = bpm_;
    auto page_cp = page_;
    bpm_ = nullptr;
    page_ = nullptr;
    page_cp->WLatch();
    return {bpm_cp, page_cp};
  }
  return {nullptr, nullptr};
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  std::scoped_lock<std::recursive_mutex, std::recursive_mutex> guard(r_latch_, that.r_latch_);
  if (&that != this) {
    guard_ = std::move(that.guard_);
    that.valid_ = false;
  }
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  std::scoped_lock<std::recursive_mutex, std::recursive_mutex> guard(r_latch_, that.r_latch_);
  if (&that == this) {
    return *this;
  }
  Drop();
  guard_ = std::move(that.guard_);
  valid_ = that.valid_;
  that.valid_ = false;
  return *this;
}

void ReadPageGuard::Drop() {
  std::scoped_lock<std::recursive_mutex> guard(r_latch_);
  if (valid_) {
    guard_.page_->RUnlatch();
    guard_.Drop();
    valid_ = false;
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  std::scoped_lock<std::recursive_mutex, std::recursive_mutex> guard(w_latch_, that.w_latch_);
  if (&that != this) {
    guard_ = std::move(that.guard_);
    that.valid_ = false;
  }
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  std::scoped_lock<std::recursive_mutex, std::recursive_mutex> guard(w_latch_, that.w_latch_);
  if (&that == this) {
    return *this;
  }
  Drop();
  guard_ = std::move(that.guard_);
  valid_ = that.valid_;
  that.valid_ = false;
  return *this;
}

void WritePageGuard::Drop() {
  std::scoped_lock<std::recursive_mutex> guard(w_latch_);
  if (valid_) {
    guard_.page_->WUnlatch();
    guard_.Drop();
    valid_ = false;
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
