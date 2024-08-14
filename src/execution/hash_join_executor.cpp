//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)){
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  done_ = false;
  join_side_scan_ = false;
  ht_.Clear();
  left_child_->Init();
  right_child_->Init();

  Tuple tp{};
  RID rid{};
  while(true){
    bool status = left_child_->Next(&tp, &rid);
    if(!status){
      break ;
    }
    JoinKey key = MakeJoinKey(&tp,
                              left_child_->GetOutputSchema(),
                              plan_->left_key_expressions_);
    JoinValue val = MakeJoinValue(&tp,
                                  left_child_->GetOutputSchema());
    ht_.Insert(key, val);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple tp_temp{};
  RID rid_temp{};

  POP_BACKUP:
  if(!backup_tuple_.empty()){
    *tuple = backup_tuple_.back();
    backup_tuple_.pop_back();
    return true;
  }

  while(true){
    bool status = right_child_->Next(&tp_temp, &rid_temp);
    if(!status){
      break;
    }
    JoinKey key = MakeJoinKey(&tp_temp,
                              right_child_->GetOutputSchema(),
                              plan_->right_key_expressions_);
    JoinValue val = MakeJoinValue(&tp_temp,
                                  right_child_->GetOutputSchema());
    auto join_bucket_ptr = ht_.Scan(key);
    if(join_bucket_ptr == nullptr){
      continue ;
    }
    for(auto &bucket_val: join_bucket_ptr->val_bucket_){
      bucket_val.been_compared_ = true;
      std::vector<Value> ret_val{};
      ret_val.insert(ret_val.end(),
                     bucket_val.val_.begin(),
                     bucket_val.val_.end());
      ret_val.insert(ret_val.end(),
                     val.val_.begin(),
                     val.val_.end());
      backup_tuple_.emplace_back(ret_val,&plan_->OutputSchema());
    }
    goto POP_BACKUP;
  }

  if(!join_side_scan_ && plan_->GetJoinType() == JoinType::LEFT){
    auto null_right_val = GetNullValueFromSchema(right_child_->GetOutputSchema());
    auto iter = ht_.Begin();
    while(iter != ht_.End()){
      for(auto &join_val: iter.Val().val_bucket_){
        if(join_val.been_compared_){
          continue ;
        }
        std::vector<Value> ret_val{};
        ret_val.insert(ret_val.end(),
                       join_val.val_.begin(),
                       join_val.val_.end());
        ret_val.insert(ret_val.end(),
                       null_right_val.begin(),
                       null_right_val.end());
        backup_tuple_.emplace_back(ret_val, &plan_->OutputSchema());
      }
      ++iter;
    }
    join_side_scan_ = true;
    goto POP_BACKUP;
  }

  return false;
}
}  // namespace bustub
//create table t1(v1 int);
//insert into t1 values (1), (1);
//select * from (t1 a inner join t1 b on a.v1 = b.v1) inner join t1 c on a.v1 = c.v1;