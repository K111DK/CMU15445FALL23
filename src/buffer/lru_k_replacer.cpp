//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex>guard(latch_);
  for (auto it = history_queue_.rbegin();it != history_queue_.rend();it++){
      if(it->second.Evictable()){
        *frame_id = it->first;
        auto pos = history_hash_.find(*frame_id);
        history_queue_.erase(pos->second);
        history_hash_.erase(*frame_id);
        curr_size_--;
        return true;
      }
  }

  for (auto it = cache_queue_.begin();it != cache_queue_.end();it++){
//      auto node = *it.base();
      if(it->Evictable()){
        *frame_id = it->GetFid();
        cache_queue_.erase(it);
        cache_queue_hash_.erase(*frame_id);
        curr_size_--;
        return true;
      }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex>guard(latch_);
  current_timestamp_++;
  if(static_cast<size_t>(frame_id) >= replacer_size_){
    BUSTUB_ASSERT(0, "Frame id invalid!");
  }
  //If frame is in (< k) history queue
  auto pos = history_hash_.find(frame_id);
  if(pos != history_hash_.end()){
    //val = <frame_id, Node>v
    BUSTUB_ASSERT(pos->second != history_queue_.end(), "pos in hash doesn't exists in queue");
    auto &val = *(pos->second);
    auto &node = val.second;
    node.Access(access_type, current_timestamp_);
    if(node.Kaccess()){
      //Access more than K times
      auto cp_val = *(pos->second);
      //remove from his queue
      history_queue_.erase(pos->second);
      history_hash_.erase(frame_id);

      //insert to cache queue
      auto insert_res = cache_queue_.insert(cp_val.second);
      if(insert_res.second){
         cache_queue_hash_[frame_id] = insert_res.first;
      }else{
         BUSTUB_ASSERT(1, "Insert to cache queue fail!");
      }

    }
    return ;
  }

  auto cache_pos = cache_queue_hash_.find(frame_id);
  if(cache_pos != cache_queue_hash_.end()){
    LRUKNode cp_node = *cache_pos->second;
    cp_node.Access(access_type, current_timestamp_);
    cache_queue_.erase(cache_pos->second);
    cache_queue_hash_.erase(frame_id);
    auto insert_res = cache_queue_.insert(cp_node);
    if(insert_res.second){
      cache_queue_hash_[frame_id] = insert_res.first;
    }else{
      BUSTUB_ASSERT(1, "Insert to cache queue fail!");
    }
    return ;
  }


  auto node = LRUKNode(frame_id, k_);
  node.Access(access_type, current_timestamp_);

  //insert to the history_queue
  if(node.Kaccess()){
    //Access more than K times
    auto insert_res = cache_queue_.insert(node);
    if(insert_res.second){
      cache_queue_hash_[frame_id] = insert_res.first;
    }else{
      BUSTUB_ASSERT(1, "Insert to cache queue fail!");
    }

  }else{
    //Access less than K times
    history_queue_.emplace_front(frame_id, node);
    history_hash_[frame_id] = history_queue_.begin();
  }

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable){
  std::lock_guard<std::mutex>guard(latch_);
  if(static_cast<size_t>(frame_id) > replacer_size_){
    BUSTUB_ASSERT(1, "Frame id invalid!");
  }
  auto pos = history_hash_.find(frame_id);
  if(pos != history_hash_.end()){
    auto &node = pos->second->second;
    if(node.Evictable() && !set_evictable){
      curr_size_--;
    }else if(!node.Evictable() && set_evictable){
      curr_size_++;
    }
    node.SetEvictable(set_evictable);
    return;
  }

  auto cache_pos = cache_queue_hash_.find(frame_id);
  if(cache_pos != cache_queue_hash_.end()){
    auto node = *cache_pos->second;
    if(node.Evictable() && !set_evictable){
      curr_size_--;
    }else if(!node.Evictable() && set_evictable){
      curr_size_++;
    }
    node.SetEvictable(set_evictable);
    cache_queue_.erase(cache_pos->second);
    auto insert_res = cache_queue_.insert(node);
    if(insert_res.second){
      cache_queue_hash_[frame_id] = insert_res.first;
    }else{
      BUSTUB_ASSERT(1, "Insert to cache queue fail!");
    }
    return ;
  }
  BUSTUB_ASSERT(1, "Frame id invalid!");
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex>guard(latch_);
  auto pos = history_hash_.find(frame_id);
  if(pos != history_hash_.end()){
    auto &node = pos->second->second;
    if(node.Evictable()){
      history_hash_.erase(frame_id);
      history_queue_.erase(pos->second);
      curr_size_--;
      return ;
    }
    BUSTUB_ASSERT(0, "Try evict inevitable frame");

  }

  auto cache_pos = cache_queue_hash_.find(frame_id);
  if(cache_pos->second != cache_queue_.end()){
    auto &node = *cache_pos->second;
    if(node.Evictable()){
      cache_queue_.erase(cache_pos->second);
      cache_queue_hash_.erase(frame_id);
      curr_size_--;
      return ;
    }
    BUSTUB_ASSERT(0, "Try evict inevitable frame");

  }
}

void LRUKNode::SetEvictable(bool evictable) {
  is_evictable_ = evictable;
}

void LRUKNode::Access(AccessType accessType, size_t access_time) {
  access_time_ += 1;
  if(access_time_ > k_){
    history_access_.pop_back();
    history_access_.push_front(access_time);
  }else{
    history_access_.push_front(access_time);
  }
}

auto LRUKNode::Kaccess() const -> bool {return access_time_ >= k_;}
auto LRUKNode::Evictable() const -> bool {return is_evictable_;}
auto LRUKNode::KthAccessTime() const -> size_t { return *history_access_.rbegin(); }
auto LRUKNode::GetFid() const -> frame_id_t { return fid_; }

}  // namespace bustub
