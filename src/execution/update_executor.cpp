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
#include "execution/executors/update_executor.h"
#include <memory>
#include "execution/execution_common.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  delete_done_ = false;
  primary_index_update_ = CheckPrimaryKeyNeedUpdate(index_info_, plan_->target_expressions_).first;
  primary_index_constant_update_ = CheckPrimaryKeyNeedUpdate(index_info_, plan_->target_expressions_).second;
}
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // const TupleMeta delete_meta = {0,true};
  if (delete_done_) {
    return false;
  }
  Tuple temp_tuple{};
  int64_t total_update;
  std::vector<std::pair<Tuple, RID>> tuples_to_update{};
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(&temp_tuple, rid)) {
    tuples_to_update.emplace_back(temp_tuple, *rid);
  }

  if (primary_index_update_) {
    total_update = PrimaryKeyUpdate(tuples_to_update);
  } else {
    total_update = NormalUpdate(tuples_to_update);
  }
  CheckUncommittedTransactionValid(table_info_, exec_ctx_->GetTransaction());
  *tuple = Tuple({{INTEGER, total_update}}, &GetOutputSchema());
  delete_done_ = true;
  auto success = total_update != 0;
  if (!success) {
    FakeAbort(exec_ctx_->GetTransaction());
  }
  return success;
}
auto UpdateExecutor::PrimaryKeyUpdate(std::vector<std::pair<Tuple, RID>> &tuples_to_update) -> int64_t {
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  int64_t total_update = 0;

  if (!tuples_to_update.empty() && primary_index_constant_update_) {
    FakeAbort(exec_ctx_->GetTransaction());
  }

  // Delete all update tuple
  for (auto &[snapshot_tuple, rid] : tuples_to_update) {
    AtomicModifiedTuple(table_info_, exec_ctx_->GetTransaction(), exec_ctx_->GetTransactionManager(), rid, true,
                        snapshot_tuple, child_executor_->GetOutputSchema(), false);
    total_update++;
  }

  // Insert new tuple
  for (auto &[snapshot_tuple, rid] : tuples_to_update) {
    Tuple update_tuple = EvaluateTuple(child_executor_->GetOutputSchema(), child_executor_->GetOutputSchema(),
                                       snapshot_tuple, plan_->target_expressions_);

    auto conflict_result = CheckPrimaryKeyConflict(index_info_, exec_ctx_->GetTransaction(), update_tuple,
                                                   child_executor_->GetOutputSchema());

    // Violate primary key uniqueness;
    if (conflict_result.has_value()) {
      RID insert_rid = conflict_result.value();
      AtomicModifiedTuple(table_info_, exec_ctx_->GetTransaction(), exec_ctx_->GetTransactionManager(), insert_rid,
                          false, update_tuple, child_executor_->GetOutputSchema(), true);
    } else {
      // Do insert
      AtomicInsertNewTuple(table_info_, index_info_, exec_ctx_->GetTransaction(), update_tuple,
                           child_executor_->GetOutputSchema(), exec_ctx_->GetLockManager());
    }
  }
  return total_update;
}
auto UpdateExecutor::NormalUpdate(std::vector<std::pair<Tuple, RID>> &tuples_to_update) -> int64_t {
  int64_t total_update = 0;
  for (auto &[snapshot_tuple, rid] : tuples_to_update) {
    Tuple update_tuple = EvaluateTuple(child_executor_->GetOutputSchema(), child_executor_->GetOutputSchema(),
                                       snapshot_tuple, plan_->target_expressions_);
    AtomicModifiedTuple(table_info_, exec_ctx_->GetTransaction(), exec_ctx_->GetTransactionManager(), rid, false,
                        update_tuple, child_executor_->GetOutputSchema(), false);
    total_update++;
  }
  return total_update;
}
}  // namespace bustub
