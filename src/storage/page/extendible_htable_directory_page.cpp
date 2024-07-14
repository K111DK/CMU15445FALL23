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
  global_depth_ = 1; // ?
  uint32_t max_buckets = HTABLE_DIRECTORY_ARRAY_SIZE;
  for(uint32_t i = 0; i < max_buckets; ++i){
      bucket_page_ids_[i] = INVALID_PAGE_ID;
      local_depths_[i] = 1;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash >> ( 32 - global_depth_ );
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

  //backward search

  uint32_t lower_bound = bucket_idx;
  while(bucket_page_ids_[lower_bound] == original_bucket_page_id
         && lower_bound < bucket_idx){
      lower_bound--;
  }

  if(lower_bound > bucket_idx && bucket_idx != 0){
      return 0; //TBD:Do we need to set[split_idx] = Invalid page id ?
  }

  if (lower_bound < bucket_idx && lower_bound + 1 != bucket_idx){
      return lower_bound + 1;
  }

  uint32_t upper_bound = bucket_idx;
  while(bucket_page_ids_[upper_bound] == original_bucket_page_id
         && upper_bound > bucket_idx){
      upper_bound++;
  }

  if(upper_bound < bucket_idx && bucket_idx != static_cast<uint32_t>(-1)){
      BUSTUB_ASSERT(0, "Huge bucket_idx");
  }

  if(upper_bound > bucket_idx && upper_bound - 1 != bucket_idx){
      return upper_bound - 1;
  }

  return bucket_idx;//No split image
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if(global_depth_ > 0){
      global_depth_--;
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
  return 2 << global_depth_;
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

}  // namespace bustub
