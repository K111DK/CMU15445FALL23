//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  auto upper_bound = HTableBucketArraySize(sizeof(std::pair<K,V>));
  BUSTUB_ASSERT(max_size >= 1 && max_size <= upper_bound, "Invalid bucket size");
  max_size_ = max_size;
  size_ = 0;
  memset(array_, 0, max_size * sizeof(std::pair<K, V>));
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for(uint32_t i = 0 ; i < max_size_ ; ++i){
    auto key_temp = array_[i].first;
    if(cmp(key, key_temp) == 0){
      value = array_[i].second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  //No more space
  if (size_ == max_size_) {
    return false;
  }

  uint32_t first_empty_slot;
  bool found_empty_slot = false;

  //check if key is exist in inserted keys
  for (uint32_t i = 0; i < max_size_; ++i) {

    // 0    1     2   ...   max_size - 1
    // deleted kv pair has the same key as it's previous kv pair

    auto pre_key_idx = i == 0 ? max_size_ - 1 : i - 1;
    auto pre_key_temp = array_[pre_key_idx].first;
    auto key_temp = array_[i].first;

    //found correspond key
    if (cmp(key, key_temp) == 0) {
      array_[i].second = value;
      return true;
    }

    //current key is an empty slot
    if(cmp(key_temp, pre_key_temp) == 0 && !found_empty_slot){
      first_empty_slot = i;
      found_empty_slot = true;
    }

  }
  BUSTUB_ASSERT(found_empty_slot, "Can't find empty slot while size < max_size");
  if(found_empty_slot){
    //Insert kv pair to tail
    array_[first_empty_slot] = std::make_pair(key, value);
    size_++;
    return true;
  }

  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  //empty bucket
  if(size_ == 0){
    return false;
  }

  for(uint32_t i=0; i < max_size_; ++i){
    auto key_temp = array_[i].first;
    if(cmp(key, key_temp) == 0){
      auto pre_kv_idx = i == 0 ? max_size_ - 1: i - 1;
      //current key = pre kv's key
      array_[i].first = array_[pre_kv_idx].first;
      return true;
    }
  }

  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < max_size_, "Invalid bucket_idx");
  auto pre_kv_idx = bucket_idx == 0 ? max_size_ - 1: bucket_idx - 1;

  //current key = pre kv's key
  array_[bucket_idx].first = array_[pre_kv_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  BUSTUB_ASSERT(bucket_idx < max_size_, "Invalid bucket_idx");
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  BUSTUB_ASSERT(bucket_idx < max_size_, "Invalid bucket_idx");
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  BUSTUB_ASSERT(bucket_idx < max_size_, "Invalid bucket_idx");
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
