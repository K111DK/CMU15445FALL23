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

  std::vector<std::pair<Tuple, RID>> tuples_to_update{};
  Tuple temp_tuple{};
  int64_t total_update;
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
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto modify_ts = txn->GetTransactionIdHumanReadable() + TXN_START_ID;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  int64_t total_update = 0;

  // Delete all update tuple
  for (auto &[child_tuple, child_rid] : tuples_to_update) {
    total_update++;
    auto [meta, tp] = table_info->table_->GetTuple(child_rid);
    // Do modify job
    meta.ts_ = modify_ts;
    meta.is_deleted_ = true;
    txn->AppendWriteSet(table_info->oid_, child_rid);
    // Do modify job
    TupleMeta insert_meta = {txn->GetTransactionTempTs(), true};
    auto table_write_page_guard = table_info->table_->AcquireTablePageWriteLock(child_rid);
    table_info->table_->UpdateTupleInPlaceWithLockAcquired(insert_meta, child_tuple, child_rid,
                                                           table_write_page_guard.AsMut<TablePage>());
    table_write_page_guard.Drop();
  }
  // Insert new tuple
  for (auto &[child_tuple, child_rid] : tuples_to_update) {
    // Compute update tuple
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    // Insert new tuple
    Tuple insert_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    const auto &primary_idx = index_info[0];
    auto primary_hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(primary_idx->index_.get());
    auto insert_key = insert_tuple.KeyFromTuple(
        child_executor_->GetOutputSchema(), *primary_hash_table->GetKeySchema(), primary_hash_table->GetKeyAttrs());

    // First, check uniqueness of primary key
    std::vector<RID> result{};
    primary_hash_table->ScanKey(insert_key, &result, txn);

    // Violate primary key uniqueness;
    if (!result.empty()) {
      RID insert_rid;
      insert_rid = result[0];

      // Check if we could modified this: Can be seen && is deleted
      auto pg_read_guard = table_info->table_->AcquireTablePageReadLock(insert_rid);
      auto [meta, exists_tp] = pg_read_guard.As<TablePage>()->GetTuple(insert_rid);
      pg_read_guard.Drop();

      // If uncommitted deleted in same txn, just update tuple inplace. o.w. add undo log
      if (meta.ts_ != txn->GetTransactionTempTs()) {
        // At most one transaction can reach here
        auto first_undo_version = txn_manager->GetUndoLink(insert_rid);
        UndoLog undo_log;
        undo_log.is_deleted_ = meta.is_deleted_;
        undo_log.ts_ = meta.ts_;
        undo_log.tuple_ = exists_tp;
        undo_log.prev_version_ = first_undo_version.has_value() ? first_undo_version.value() : UndoLink();
        undo_log.modified_fields_ = std::vector<bool>(child_executor_->GetOutputSchema().GetColumnCount(), true);
        auto new_first_undo_version = txn->AppendUndoLog(undo_log);
        txn->AppendWriteSet(plan_->table_oid_, insert_rid);
        txn_manager->UpdateUndoLink(insert_rid, new_first_undo_version);
      }

      // Do modify job
      TupleMeta insert_meta = {txn->GetTransactionTempTs(), false};
      auto table_write_page_guard = table_info->table_->AcquireTablePageWriteLock(insert_rid);
      table_info->table_->UpdateTupleInPlaceWithLockAcquired(insert_meta, insert_tuple, insert_rid,
                                                             table_write_page_guard.AsMut<TablePage>());
      table_write_page_guard.Drop();

    } else {
      // Do insert
      auto insert_ts = txn->GetTransactionTempTs();
      TupleMeta meta = {insert_ts, false};
      RID insert_rid;
      const auto insert =
          table_info->table_->InsertTuple(meta, insert_tuple, exec_ctx_->GetLockManager(), txn, plan_->GetTableOid());

      // In project 4, we always assume insert is successful
      BUSTUB_ASSERT(insert.has_value(), "Insert fail!");
      insert_rid = insert.value();

      // Try update primary index
      bool try_update_primary_index = primary_hash_table->InsertEntry(insert_key, insert_rid, txn);

      // Fail! Other transaction already update index, mark insert tuple as deleted, then Abort
      if (!try_update_primary_index) {
        meta = {insert_ts, true};
        table_info->table_->UpdateTupleMeta(meta, insert_rid);
        txn->SetTainted();
        throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
      }

      // Success! Append write set
      txn->AppendWriteSet(table_info_->oid_, insert_rid);
      txn_manager->UpdateUndoLink(insert_rid, std::nullopt);
    }
  }

  return total_update;
}
auto UpdateExecutor::NormalUpdate(std::vector<std::pair<Tuple, RID>> &tuples_to_update) -> int64_t {
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto read_ts = txn->GetReadTs();
  auto modify_ts = txn->GetTransactionIdHumanReadable() + TXN_START_ID;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  int64_t total_update = 0;
  for (auto &[child_tuple, child_rid] : tuples_to_update) {
    // Compute update tuple
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    // Insert new tuple
    Tuple insert_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    auto [meta, tp] = table_info->table_->GetTuple(child_rid);
    bool is_same_transaction = meta.ts_ == modify_ts;

    // Abort if a larger ts modify is committed or Any other transaction is modifying
    bool do_abort = meta.ts_ > read_ts && !is_same_transaction;
    if (do_abort) {
      txn->SetTainted();
      throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
    }
    // If this tuple haven't been modified by this txn yet, append undo log, update link
    if (!is_same_transaction) {
      auto [modified_tp, modified_fields] =
          GetTupleModifyFields(&child_executor_->GetOutputSchema(), &child_tuple, &insert_tuple);
      auto first_undo_version = txn_manager->GetUndoLink(child_rid);
      UndoLog undo_log;
      undo_log.is_deleted_ = meta.is_deleted_;
      undo_log.ts_ = meta.ts_;
      undo_log.modified_fields_ = modified_fields;
      undo_log.tuple_ = modified_tp;
      undo_log.prev_version_ = first_undo_version.has_value() ? first_undo_version.value() : UndoLink();
      auto new_first_undo_version = txn->AppendUndoLog(undo_log);
      txn_manager->UpdateUndoLink(child_rid, new_first_undo_version);
      txn->AppendWriteSet(table_info->oid_, child_rid);
    } else {
      auto first_undo_version = txn_manager->GetUndoLink(child_rid);
      if (first_undo_version.has_value() && first_undo_version.value().IsValid()) {
        UndoLog old_undo_log = txn_manager->GetUndoLog(first_undo_version.value());
        std::vector<UndoLog> undo_logs;
        UndoLog new_undo_log = old_undo_log;
        auto original_tp = ReconstructTuple(&child_executor_->GetOutputSchema(), tp, meta, {old_undo_log});
        if (original_tp.has_value()) {
          auto [modified_tp, modified_fields] = GetTupleModifyFields(
              &child_executor_->GetOutputSchema(), &original_tp.value(), &insert_tuple, &old_undo_log.modified_fields_);
          new_undo_log.modified_fields_ = modified_fields;
          new_undo_log.tuple_ = modified_tp;
        }
        // We only change modified tp
        txn->ModifyUndoLog(first_undo_version->prev_log_idx_, new_undo_log);
      }
      txn->AppendWriteSet(table_info->oid_, child_rid);
    }

    // do modify job
    meta.ts_ = modify_ts;
    meta.is_deleted_ = false;
    table_info->table_->UpdateTupleInPlace(meta, insert_tuple, child_rid);
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

}  // namespace bustub
//
// create table t1(v1 int, v2 int);
// insert into t1 values (1,1), (2,2), (3,3), (4,4);
// update t1 set v2=114514;
