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
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for(uint32_t i = 0 ; i < size_ ; ++i){
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

  //check if key is exist in inserted keys
  for (uint32_t i = 0; i < size_; ++i) {
    // 0    1     2   ...   max_size - 1
    // deleted kv pair has the same key as it's previous kv pair

    auto key_temp = array_[i].first;

    //found correspond key
    if (cmp(key, key_temp) == 0) {
      array_[i].second = value;
      return true;
    }
  }

  //Insert kv pair to tail
  array_[size_] = std::make_pair(key, value);
  size_++;
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  //empty bucket
  if(size_ == 0){
    return false;
  }

  uint32_t delete_idx = size_;
  bool found = false;
  for(uint32_t i=0; i < size_; ++i){
    auto key_temp = array_[i].first;
    if(cmp(key, key_temp) == 0){
      delete_idx = i;
      found = true;
      break ;
    }
  }

  if(found){
    BUSTUB_ASSERT(delete_idx != size_, "delete idx error!");
    for(uint32_t i = delete_idx + 1; i < size_; ++i){
      array_[i - 1] = array_[i];
    }
    size_--;
    return true;
  }

  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < size_, "Invalid bucket_idx");
  for(uint32_t i = bucket_idx + 1; i < size_; ++i){
    array_[i - 1] = array_[i];
  }
  size_--;
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
