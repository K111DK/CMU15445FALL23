//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"
namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  //throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  BUSTUB_ASSERT(max_depth >= 1, "Invalid max depth");
  max_depth_ = max_depth;
  global_depth_ = 0; // ?
  uint32_t max_buckets = HTABLE_DIRECTORY_ARRAY_SIZE;
  for(uint32_t i = 0; i < max_buckets; ++i){
      bucket_page_ids_[i] = INVALID_PAGE_ID;
      local_depths_[i] = 0;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "Invalid bucket_idx");
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "Invalid bucket_idx");
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "Invalid bucket_idx");

  auto original_bucket_page_id = bucket_page_ids_[bucket_idx];
  BUSTUB_ASSERT(original_bucket_page_id != INVALID_PAGE_ID, "Invalid bucket to split");

  auto bucket_depth = local_depths_[bucket_idx];
  BUSTUB_ASSERT(bucket_depth < global_depth_, "Bucket's local depth >= global depth, can't split bucket");

  auto after_split_depth_mask = static_cast<uint32_t>(1) << ( bucket_depth );
  return after_split_depth_mask | static_cast<uint32_t>(bucket_idx);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  uint32_t current_directory_size = 1 << global_depth_;
  for(uint32_t i = current_directory_size; i < current_directory_size * 2; ++i){
      bucket_page_ids_[i] = bucket_page_ids_[i - current_directory_size];
      local_depths_[i] = local_depths_[i - current_directory_size];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if(global_depth_ > 0 && CanShrink()){
      global_depth_--;//Dose this ok?
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  uint8_t max_local_depth = 1;
  for(unsigned char local_depth : local_depths_){
      max_local_depth = std::max(max_local_depth, local_depth);
  }
  return global_depth_ > max_local_depth;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  return 1 << global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "Invalid bucket_idx");
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "Invalid bucket_idx");
  BUSTUB_ASSERT(local_depth <= global_depth_, "Local depth can't higher than global depth");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "Invalid bucket_idx");
  BUSTUB_ASSERT(local_depths_[bucket_idx] < global_depth_, "Local depth can't higher than global depth");
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "Invalid bucket_idx");
  if(local_depths_[bucket_idx] > 1){
      local_depths_[bucket_idx]--;
  }
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  return ~( static_cast<uint32_t>(-1) << global_depth_ );
}
auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return ~( static_cast<uint32_t>(-1) << local_depths_[bucket_idx] );
}
auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }
auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

}  // namespace bustub
