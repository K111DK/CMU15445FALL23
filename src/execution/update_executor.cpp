//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //const TupleMeta delete_meta = {0,true};
  std::vector<Value> update_tuple{};

  Tuple child_tuple{};
  const auto status = child_executor_->Next(&child_tuple, rid);
  const TupleMeta delete_tuple = {0, true};
  if(!status){
    update_tuple.emplace_back(INTEGER, 0);
    *tuple = Tuple(update_tuple, &GetOutputSchema());
    return false;
  }

  //delete old expression
  table_info_->table_->UpdateTupleMeta(delete_tuple, *rid);

  // Compute update tuple
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  for (const auto &expr : plan_->target_expressions_) {
    values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
  }

  //Insert new tuple
  Tuple insert_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
  table_info_->table_->InsertTuple({0, false}, insert_tuple,
                                   exec_ctx_->GetLockManager());

  update_tuple.emplace_back(INTEGER, 1);
  *tuple = Tuple(update_tuple, &GetOutputSchema());
  return true;
}

}  // namespace bustub
//
//create table t1(v1 int, v2 int);
//insert into t1 values (1,1), (2,2), (3,3), (4,4);
//update t1 set v2=114514;
