#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  bpm_->UnpinPage(page_->GetPageId(), page_->IsDirty(), AccessType::Unknown);
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if ((bpm_ != nullptr) && (page_ != nullptr)) {
    Drop();
  }
}
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  auto bpm_cp = bpm_;
  auto page_cp = page_;
  Drop();
  return {bpm_cp, page_cp};
}
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  auto bpm_cp = bpm_;
  auto page_cp = page_;
  Drop();
  return {bpm_cp, page_cp};
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.page_->RUnlatch();
  guard_.Drop();
  valid_ = false;
}

ReadPageGuard::~ReadPageGuard() {
  if (valid_) {
    Drop();
    valid_ = false;
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  guard_.page_->WUnlatch();
  guard_.Drop();
  valid_ = false;
}

WritePageGuard::~WritePageGuard() {
  if (valid_) {
    Drop();
    valid_ = false;
  }
}  // NOLINT

}  // namespace bustub
