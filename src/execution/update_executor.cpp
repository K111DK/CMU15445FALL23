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

  //delete old index
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for(const auto &idx:index_info) {
    auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idx->index_.get());
    auto delete_key = child_tuple.KeyFromTuple(
        child_executor_->GetOutputSchema(),
        *hash_table->GetKeySchema(),
        hash_table->GetKeyAttrs());
    hash_table->DeleteEntry(delete_key, *rid, exec_ctx_->GetTransaction());
  }

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

  //Insert new index
  for(const auto &idx:index_info) {
    auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idx->index_.get());
    auto insert_key = insert_tuple.KeyFromTuple(
        child_executor_->GetOutputSchema(),
        *hash_table->GetKeySchema(),
        hash_table->GetKeyAttrs());
    hash_table->DeleteEntry(insert_key, *rid, exec_ctx_->GetTransaction());
  }

  update_tuple.emplace_back(INTEGER, 1);
  *tuple = Tuple(update_tuple, &GetOutputSchema());
  return true;
}

}  // namespace bustub
//
//create table t1(v1 int, v2 int);
//insert into t1 values (1,1), (2,2), (3,3), (4,4);
//update t1 set v2=114514;
