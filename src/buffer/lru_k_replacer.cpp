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

  for (auto it = k_history_queue_.rbegin();it != k_history_queue_.rend();it++){
      if(it->second.Evictable()){
        *frame_id = it->first;
        auto pos = k_history_hash_.find(*frame_id);
        k_history_queue_.erase(pos->second);
        k_history_hash_.erase(*frame_id);
        curr_size_--;
        return true;
      }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex>guard(latch_);
  if(static_cast<size_t>(frame_id) > replacer_size_){
    BUSTUB_ASSERT(0, "Frame id invalid!");
  }
  //If frame is in (< k) history queue
  auto pos = history_hash_.find(frame_id);
  if(pos != history_hash_.end()){
    //val = <frame_id, Node>

    BUSTUB_ASSERT(pos->second != history_queue_.end(), "pos in hash doesn't exists in queue");
    auto &val = *(pos->second);
    auto &node = val.second;
    node.Access(access_type);
    if(node.Kaccess()){
      //Access more than K times
      auto cp_val = *(pos->second);
      //remove from his queue
      history_queue_.erase(pos->second);
      history_hash_.erase(frame_id);


      //insert to the front of k_his queue
      k_history_queue_.emplace_front(cp_val);
      k_history_hash_[frame_id] = k_history_queue_.begin();

    }else{
      //Using FIFO

//      //Access less than K times
//      auto cp_val = *(pos->second);
//      //insert to the front of his queue
//      history_queue_.erase(pos->second);
//      history_queue_.emplace_front(cp_val);
//      history_hash_[frame_id] = history_queue_.begin();

    }
    return ;
  }

  pos = k_history_hash_.find(frame_id);
  if(pos != k_history_hash_.end()){
    auto &val = *pos->second;
    BUSTUB_ASSERT(pos->second != k_history_queue_.end(), "pos in hash doesn't exists in queue");
    auto &node = val.second;
    node.Access(access_type);
    auto cp_val = *(pos->second);
    k_history_queue_.erase(pos->second);
    k_history_queue_.emplace_front(cp_val);
    k_history_hash_[frame_id] = k_history_queue_.begin();
    return ;
  }


  //Remove from node_store
  auto node = LRUKNode(frame_id, k_);
  node.Access(access_type);

  //insert to the history_queue
  if(node.Kaccess()){
    //Access more than K times
    k_history_queue_.emplace_front(frame_id, node);
    k_history_hash_[frame_id] = k_history_queue_.begin();

  }else{
    //Access less than K times
    history_queue_.emplace_front(frame_id, node);
    history_hash_[frame_id] = history_queue_.begin();
  }

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable){
  std::lock_guard<std::mutex>guard(latch_);
  if(static_cast<size_t>(frame_id) > replacer_size_){
    BUSTUB_ASSERT(0, "Frame id invalid!");
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

  pos = k_history_hash_.find(frame_id);
  if(pos != k_history_hash_.end()){
    auto &node = pos->second->second;
    if(node.Evictable() && !set_evictable){
      curr_size_--;
    }else if(!node.Evictable() && set_evictable){
      curr_size_++;
    }
    node.SetEvictable(set_evictable);
    return ;
  }
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
    BUSTUB_ASSERT(1, "Try evict inevitable frame");

  }

  pos = k_history_hash_.find(frame_id);
  if(pos != k_history_hash_.end()){
    auto &node = pos->second->second;
    if(node.Evictable()){
      k_history_hash_.erase(frame_id);
      k_history_queue_.erase(pos->second);
      curr_size_--;
      return ;
    }
    BUSTUB_ASSERT(1, "Try evict inevitable frame");

  }
}

void LRUKNode::SetEvictable(bool evictable) {
  is_evictable_ = evictable;
}

void LRUKNode::Access(AccessType accessType) {
  access_time_ += 1;
}

auto LRUKNode::Kaccess() -> bool {
  return access_time_ >= k_;
}

auto LRUKNode::Evictable() -> bool {
  return is_evictable_;
}

}  // namespace bustub
