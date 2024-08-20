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
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  delete_done_ = false;
  primary_index_update_ = CheckPrimaryKeyNeedUpdate(plan_->target_expressions_);
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
  }else{
    total_update = NormalUpdate(tuples_to_update);
  }

  *tuple = Tuple({{INTEGER, total_update}}, &GetOutputSchema());
  delete_done_ = true;
  return true;
}
auto UpdateExecutor::PrimaryKeyUpdate(std::vector<std::pair<Tuple, RID>> &tuples_to_update) -> int64_t {
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  int64_t total_update = 0;

  // Delete all update tuple
  for (auto &[snapshot_tuple, rid] : tuples_to_update) {
    AtomicModifiedTuple(rid, true, snapshot_tuple);
    total_update++;
  }

  // Insert new tuple
  for (auto &[snapshot_tuple, rid] : tuples_to_update) {
    Tuple update_tuple = EvaluateTuple(child_executor_->GetOutputSchema(),
                                       child_executor_->GetOutputSchema(),
                                       snapshot_tuple,
                                       plan_->target_expressions_);

    auto conflict_result = CheckPrimaryKeyConflict(update_tuple);

    // Violate primary key uniqueness;
    if (conflict_result.has_value()) {
      RID insert_rid = conflict_result.value();
      AtomicModifiedTuple(insert_rid, false, update_tuple);
    } else {
      // Do insert
      AtomicInsertNewTuple(update_tuple);
    }
  }
  return total_update;
}
auto UpdateExecutor::NormalUpdate(std::vector<std::pair<Tuple, RID>> &tuples_to_update) -> int64_t {
  int64_t total_update = 0;
  for (auto &[snapshot_tuple, rid] : tuples_to_update) {
    Tuple update_tuple = EvaluateTuple(child_executor_->GetOutputSchema(),
                                       child_executor_->GetOutputSchema(),snapshot_tuple,
                                       plan_->target_expressions_);
    AtomicModifiedTuple(rid, false, update_tuple);
    total_update++;
  }
  return total_update;
}
auto UpdateExecutor::CheckPrimaryKeyNeedUpdate(const std::vector<std::shared_ptr<AbstractExpression>> &update_expr) -> bool {
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (index_info.empty()) {
    return false;
  }
  auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info[0]->index_.get());
  for (const auto &expr : update_expr) {
    auto arith_expr = std::dynamic_pointer_cast<ArithmeticExpression>(expr);
    if (arith_expr == nullptr) {
      continue;
    }
    if (arith_expr->GetChildren().size() != 2) {
      continue;
    }
    auto column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(arith_expr->GetChildAt(0));
    auto const_expr = std::dynamic_pointer_cast<ConstantValueExpression>(arith_expr->GetChildAt(1));
    if (column_expr == nullptr || const_expr == nullptr) {
      continue;
    }
    if (column_expr->GetColIdx() == hash_table->GetKeyAttrs()[0] && hash_table->GetMetadata()->IsPrimaryKey() &&
        const_expr->val_.CompareEquals(ValueFactory::GetZeroValueByType(const_expr->GetReturnType())) ==
            CmpBool::CmpFalse) {
      return true;
    }
  }
  return false;
}
auto UpdateExecutor::CheckPrimaryKeyConflict(Tuple & tuple) -> std::optional<RID>{
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  const auto &primary_idx = index_info[0];
  auto primary_hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(primary_idx->index_.get());
  auto insert_key = tuple.KeyFromTuple(
      child_executor_->GetOutputSchema(),
      *primary_hash_table->GetKeySchema(),
      primary_hash_table->GetKeyAttrs());
  // First, check uniqueness of primary key
  std::vector<RID> result{};
  primary_hash_table->ScanKey(insert_key, &result, exec_ctx_->GetTransaction());
  return result.empty() ? std::nullopt: std::make_optional<RID>(result[0]);
}
auto UpdateExecutor::AtomicInsertNewTuple(Tuple &insert_tuple) -> void {
  auto txn = exec_ctx_->GetTransaction();
  auto insert_ts = txn->GetTransactionTempTs();
  const auto insert =
      table_info_->table_->InsertTuple({insert_ts, false},
                                       insert_tuple,
                                       exec_ctx_->GetLockManager(),
                                       txn,
                                       plan_->GetTableOid());
  // In project 4, we always assume insert is successful
  BUSTUB_ASSERT(insert.has_value(), "Insert fail!");
  RID insert_rid = insert.value();
  const auto &primary_idx = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if(!primary_idx.empty()){

    BUSTUB_ASSERT(primary_idx.size() == 1, "We only support one index for each table");
    auto primary_hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(primary_idx[0]->index_.get());
    auto insert_key = insert_tuple.KeyFromTuple(
         child_executor_->GetOutputSchema(),
         *primary_hash_table->GetKeySchema(),
         primary_hash_table->GetKeyAttrs());

    // Try update primary index
    bool try_update_primary_index = primary_hash_table->InsertEntry(insert_key, insert_rid, txn);

    // Fail! Other transaction already update index, mark insert tuple as deleted, then Abort
    if (!try_update_primary_index) {
       txn->SetTainted();
       throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
    }
  }
  // Success! Append write set
  txn->AppendWriteSet(table_info_->oid_, insert_rid);
}
auto UpdateExecutor::AtomicModifiedTuple(RID &rid, bool do_deleted, Tuple &update_tuple) -> void {
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto modify_ts = txn->GetTransactionTempTs();

  // Critical section begin
  auto [current_meta, current_tuple] = table_info_->table_->GetTuple(rid);
  bool self_uncommitted_transaction = current_meta.ts_ == txn->GetTransactionTempTs();
  bool can_not_see = current_meta.ts_ > txn->GetReadTs();

  if(!self_uncommitted_transaction) {
    auto current_version_link = txn_manager->GetVersionLink(rid);
    VersionUndoLink modified_link = current_version_link.has_value() ? current_version_link.value() : VersionUndoLink();
    modified_link.in_progress_ = true;
    bool success = txn_manager->UpdateVersionLink(rid, modified_link, VersionLinkInProgress);
    if (!success) {
       FakeAbort(txn);
    }
  }

  // Abort if a larger ts modify is committed or Any other transaction is modifying
  bool do_abort = can_not_see && !self_uncommitted_transaction;
  if (do_abort) {
    FakeAbort(txn);
  }
  auto first_undo_version = txn_manager->GetUndoLink(rid);

  // If this tuple haven't been modified by this txn yet, append undo log, update link
  if (!self_uncommitted_transaction) {
    auto [modified_tp, modified_fields] =
        GetTupleModifyFields(&child_executor_->GetOutputSchema(),
                             &current_tuple,
                             &update_tuple);
    UndoLog undo_log;
    undo_log.is_deleted_ = current_meta.is_deleted_;
    undo_log.ts_ = current_meta.ts_;
    undo_log.modified_fields_ = modified_fields;
    undo_log.tuple_ = modified_tp;
    undo_log.prev_version_ = first_undo_version.has_value() ? first_undo_version.value() : UndoLink();
    auto new_first_undo_version = txn->AppendUndoLog(undo_log);
    //Atomically update link
    txn_manager->UpdateUndoLink(rid, new_first_undo_version);
  } else {
    if (first_undo_version.has_value() && first_undo_version.value().IsValid()) {
      UndoLog old_undo_log = txn_manager->GetUndoLog(first_undo_version.value());
      UndoLog new_undo_log = old_undo_log;
      auto before_modified = ReconstructTuple(&child_executor_->GetOutputSchema(),
                                              current_tuple,
                                              current_meta,
                                              {old_undo_log});
      if (before_modified.has_value()) {
        auto [modified_tp, modified_fields] = GetTupleModifyFields(
            &child_executor_->GetOutputSchema(), &before_modified.value(), &update_tuple, &old_undo_log.modified_fields_);
        new_undo_log.modified_fields_ = modified_fields;
        new_undo_log.tuple_ = modified_tp;
      }
      //Atomically update undo log
      txn->ModifyUndoLog(first_undo_version->prev_log_idx_, new_undo_log);
    }
  }

  // do modify job (don't need lock since we're in snapshot read) only one transaction reach here
  table_info_->table_->UpdateTupleInPlace({modify_ts,
                                           do_deleted},
                                          update_tuple,rid);
  txn->AppendWriteSet(table_info_->oid_, rid);
}

}  // namespace bustub
//
// create table t1(v1 int, v2 int);
// insert into t1 values (1,1), (2,2), (3,3), (4,4);
// update t1 set v2=114514;
