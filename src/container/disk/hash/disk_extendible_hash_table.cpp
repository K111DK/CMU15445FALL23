//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size),
      bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  fmt::println("New hash table: header max depth:{} directory max depth:{} bucket_max_size:{}", header_max_depth,
               directory_max_depth, bucket_max_size);
  auto page = bpm->NewPage(&header_page_id_);
  if (page == nullptr) {
    throw Exception("Can't alloc page for hash table");
  }
  auto header_page_guard = BasicPageGuard(bpm, page).AsMut<ExtendibleHTableHeaderPage>();
  header_page_guard->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  std::lock_guard<std::mutex> guard(hash_table_lock_);
  uint32_t hash = Hash(key);

  auto header_page_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  BUSTUB_ASSERT(header_page != nullptr, "Can't fetch header page");

  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_idx));

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory_page_guard = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  BUSTUB_ASSERT(directory_page != nullptr, "Can't fetch directory page");

  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_page_guard = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  BUSTUB_ASSERT(bucket_page != nullptr, "Can't fetch bucket page");

  V value;
  bool success = bucket_page->Lookup(key, value, cmp_);
  if (success) {
    result->emplace_back(value);
  }
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::lock_guard<std::mutex> guard(hash_table_lock_);
  uint32_t hash = Hash(key);
  fmt::println("Insert: {}", hash);

  auto header_page_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  BUSTUB_ASSERT(header_page != nullptr, "Can't fetch header page");

  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_idx));

  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }

  auto directory_page_guard = bpm_->FetchPageBasic(directory_page_id);
  auto *directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  BUSTUB_ASSERT(directory_page != nullptr, "Can't fetch directory page");

TRY_INSERT:

  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }

  auto bucket_page_guard = bpm_->FetchPageBasic(bucket_page_id);
  auto *bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  BUSTUB_ASSERT(bucket_page != nullptr, "Can't fetch bucket page");

  if (!bucket_page->IsFull()) {
    return bucket_page->Insert(key, value, cmp_);
  }

  auto local_depth = directory_page->GetLocalDepth(bucket_idx);
  auto global_depth = directory_page->GetGlobalDepth();

  if (global_depth > local_depth) {
    auto split_image_idx = directory_page->GetSplitImageIndex(bucket_idx);

    // Get new split image page
    page_id_t new_bucket_page_idx;
    auto new_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_idx);
    BUSTUB_ASSERT(new_bucket_page_guard.GetData() != nullptr, "Can't fetch new page for bucket");
    auto new_bucket_page = new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);

    // Migrate kv pairs
    MigrateEntries(bucket_page, new_bucket_page, split_image_idx, static_cast<uint32_t>(1) << local_depth);

    auto old_local_depth_mask = directory_page->GetLocalDepthMask(bucket_idx);
    uint32_t new_local_depth_mask = (old_local_depth_mask << 1) | static_cast<uint32_t>(1);
    // Update mappings
    UpdateDirectoryMapping(directory_page, split_image_idx, new_bucket_page_idx, local_depth + 1, new_local_depth_mask);
    UpdateDirectoryMapping(directory_page, bucket_idx, bucket_page_id, local_depth + 1, new_local_depth_mask);
    VerifyIntegrity();

  } else {
    if (directory_page->GetGlobalDepth() == directory_max_depth_) {
      return false;
    }
    directory_page->IncrGlobalDepth();
  }
  goto TRY_INSERT;
}

template <typename K, typename V, typename KC>
[[maybe_unused]] auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header,
                                                                              uint32_t directory_idx, uint32_t hash,
                                                                              const K &key, const V &value) -> bool {
  page_id_t new_directory_page_idx;
  auto directory_page_guard = bpm_->NewPageGuarded(&new_directory_page_idx);
  BUSTUB_ASSERT(directory_page_guard.GetData() != nullptr, "Can't fetch new page for directory");
  auto directory = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);
  auto bucket_idx = directory->HashToBucketIndex(hash);
  bool success = InsertToNewBucket(directory, bucket_idx, key, value);
  if (success) {
    header->SetDirectoryPageId(directory_idx, new_directory_page_idx);
  }
  return success;
}

template <typename K, typename V, typename KC>
[[maybe_unused]] auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory,
                                                                           uint32_t bucket_idx, const K &key,
                                                                           const V &value) -> bool {
  page_id_t new_bucket_page_idx;
  auto bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_idx);
  BUSTUB_ASSERT(bucket_page_guard.GetData() != nullptr, "Can't fetch new page for bucket");
  auto bucket = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);
  bool success = bucket->Insert(key, value, cmp_);
  if (success) {
    directory->SetBucketPageId(bucket_idx, new_bucket_page_idx);
  }
  return success;
}

template <typename K, typename V, typename KC>
[[maybe_unused]] void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(
    ExtendibleHTableDirectoryPage *directory, uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
    uint32_t new_local_depth, uint32_t local_depth_mask) {
  for (uint32_t bucket_idx = 0; bucket_idx < (static_cast<uint32_t>(1) << directory->GetGlobalDepth()); bucket_idx++) {
    if ((bucket_idx & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
      directory->SetBucketPageId(bucket_idx, new_bucket_page_id);
      directory->SetLocalDepth(bucket_idx, new_local_depth);
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  // BUSTUB_ASSERT(old_bucket->IsFull(), "Splitting bucket must be a full bucket");
  auto old_bucket_size = old_bucket->Size();
  std::vector<uint32_t> migrate_idx{};
  for (uint32_t i = 0; i < old_bucket_size; ++i) {
    K key = old_bucket->KeyAt(i);
    uint32_t hash = Hash(key);
    if ((hash & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
      migrate_idx.emplace_back(i);
    }
  }

  for (auto it = migrate_idx.rbegin(); it != migrate_idx.rend(); ++it) {
    auto kv = old_bucket->EntryAt(*it);
    new_bucket->Insert(kv.first, kv.second, cmp_);
    old_bucket->RemoveAt(*it);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  std::lock_guard<std::mutex> guard(hash_table_lock_);
  uint32_t hash = Hash(key);
  fmt::println("Remove: {}", hash);
  auto header_page_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  BUSTUB_ASSERT(header_page != nullptr, "Can't fetch header page");

  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_idx));

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory_page_guard = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  BUSTUB_ASSERT(directory_page != nullptr, "Can't fetch directory page");

  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  // auto bucket_local_depth = directory_page->GetLocalDepth(bucket_idx);
  // auto bucket_local_depth_bit_mask = static_cast<uint32_t>(1) << bucket_local_depth;

  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_page_guard = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  BUSTUB_ASSERT(bucket_page != nullptr, "Can't fetch bucket page");

  bool success = bucket_page->Remove(key, cmp_);
  if (success) {
    // pass
    DirectoryBucketMerging(directory_page, bucket_page, bucket_idx, hash);
    while (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
    VerifyIntegrity();
  }
  return success;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::DirectoryBucketMerging(ExtendibleHTableDirectoryPage *directory,
                                                               ExtendibleHTableBucketPage<K, V, KC> *bucket_page,
                                                               uint32_t bucket_idx, uint32_t hash) {
  if (!bucket_page->IsEmpty()) {
    return;
  }

  uint32_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  uint32_t local_depth_mask = directory->GetLocalDepthMask(bucket_idx);
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  if (local_depth == 0) {
    return;
  }

  uint32_t bucket_highest_bit = static_cast<uint32_t>(1) << (local_depth - 1);

  uint32_t buddy_bucket_idx;
  if ((bucket_idx & bucket_highest_bit) == 0) {
    buddy_bucket_idx = bucket_idx | bucket_highest_bit;
  } else {
    buddy_bucket_idx = bucket_idx & (~bucket_highest_bit);
  }
  uint32_t buddy_local_depth = directory->GetLocalDepth(buddy_bucket_idx);
  if (buddy_local_depth != local_depth) {
    return;
  }

  auto buddy_bucket_page_id = directory->GetBucketPageId(buddy_bucket_idx);
  auto buddy_bucket_page_guard = bpm_->FetchPageBasic(buddy_bucket_page_id);
  auto buddy_bucket_page = buddy_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!buddy_bucket_page->IsEmpty()) {
    return;
  }

  auto new_local_depth_mask = (~bucket_highest_bit) & local_depth_mask;
  UpdateDirectoryMapping(directory, bucket_idx, bucket_page_id, local_depth - 1, new_local_depth_mask);
  DirectoryBucketMerging(directory, bucket_page, bucket_idx, hash);
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
