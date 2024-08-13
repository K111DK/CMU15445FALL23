//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)){
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER ) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_valid_ = false;
  right_found_ = false;
  done_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  bool left_status;
  bool right_status;
  RID rid_temp;
  while(!done_) {
    if (!left_valid_) {
      left_status = left_executor_->Next(&left_tuple_, &rid_temp);
      right_found_ = false;
      right_executor_->Init();
      if (!left_status) {
        done_ = true;
        return false;
      }
      left_valid_ = true;
    }

    right_status = right_executor_->Next(&right_tuple_, &rid_temp);
    if (!right_status) {
      left_valid_ = false;
      if(plan_->GetJoinType() == JoinType::LEFT && !right_found_){
        std::vector<Value> left_join_val = GetValueVectorFromTuple(&left_executor_->GetOutputSchema(),
                                                                   &left_tuple_);
        std::vector<Value> right_join_val = GetNullValueVector(&right_executor_->GetOutputSchema());
        left_join_val.insert(left_join_val.end(), right_join_val.begin(), right_join_val.end());
        *tuple = {left_join_val, &plan_->OutputSchema()};
        return true;
      }
      continue ;
    }

    auto join_result = plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                       &right_tuple_,right_executor_->GetOutputSchema());

    BUSTUB_ASSERT(join_result.GetTypeId() == TypeId::BOOLEAN, "Wrong join result");
    Value true_val = {TypeId::BOOLEAN, 1};
    if(join_result.CompareEquals(true_val) == CmpBool::CmpTrue){
      std::vector<Value> left_join_val = GetValueVectorFromTuple(&left_executor_->GetOutputSchema(),
                                                                 &left_tuple_);
      std::vector<Value> right_join_val = GetValueVectorFromTuple(&right_executor_->GetOutputSchema(),
                                                                 &right_tuple_);
      left_join_val.insert(left_join_val.end(), right_join_val.begin(), right_join_val.end());
      *tuple = {left_join_val, &plan_->OutputSchema()};
      right_found_ = true;
      return true;
    }

  }

  return false;
}
//select * from
//    __mock_table_tas_2023_fall inner join __mock_table_schedule_2023
//        on office_hour = day_of_week
//            where has_lecture = 1;
}  // namespace bustub
