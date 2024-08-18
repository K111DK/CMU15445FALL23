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

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // const TupleMeta delete_meta = {0,true};
  std::vector<Value> update_tuple{};

  Tuple child_tuple{};
  while (!delete_done_) {
    const auto status = child_executor_->Next(&child_tuple, rid);

    if (!status) {
      update_tuple.emplace_back(INTEGER, total_update_);
      *tuple = Tuple(update_tuple, &GetOutputSchema());
      delete_done_ = true;
      return true;
    }
    // Compute update tuple
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    // Insert new tuple
    Tuple insert_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    auto txn = exec_ctx_->GetTransaction();
    auto txn_manager = exec_ctx_->GetTransactionManager();
    auto read_ts = txn->GetReadTs();
    auto modify_ts = txn->GetTransactionTempTs();
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
    auto [meta, tp] = table_info->table_->GetTuple(*rid);
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
          GetTupleModifyFields(&child_executor_->GetOutputSchema(),
                               &child_tuple,
                               &insert_tuple);
      auto first_undo_version = txn_manager->GetUndoLink(*rid);
      UndoLog undo_log;
      undo_log.is_deleted_ = meta.is_deleted_;
      undo_log.ts_ = meta.ts_;
      undo_log.modified_fields_ = modified_fields;
      undo_log.tuple_ = modified_tp;
      undo_log.prev_version_ = first_undo_version.has_value() ? first_undo_version.value() : UndoLink();
      auto new_first_undo_version = txn->AppendUndoLog(undo_log);
      txn_manager->UpdateUndoLink(*rid, new_first_undo_version);
      txn->AppendWriteSet(table_info->oid_, *rid);
    } else {
      auto first_undo_version = txn_manager->GetUndoLink(*rid);
      if(first_undo_version.has_value() && first_undo_version.value().IsValid()){
        UndoLog old_undo_log = txn_manager->GetUndoLog(first_undo_version.value());
        std::vector<UndoLog> undo_logs;
        GetReconstructUndoLogs(txn_manager, txn->GetReadTs(), *rid, undo_logs);
        auto original_tp = ReconstructTuple(&child_executor_->GetOutputSchema(), tp, meta, undo_logs);
        auto [modified_tp, modified_fields] = GetTupleModifyFields(
            &child_executor_->GetOutputSchema(), &original_tp.value(), &insert_tuple, &old_undo_log.modified_fields_);

        UndoLog new_undo_log = old_undo_log;
        // We only change modified tp
        new_undo_log.modified_fields_ = modified_fields;
        new_undo_log.tuple_ = modified_tp;
        txn->ModifyUndoLog(first_undo_version->prev_log_idx_, new_undo_log);
      }
      txn->AppendWriteSet(table_info->oid_, *rid);
    }

    // do modify job
    meta.ts_ = modify_ts;
    meta.is_deleted_ = false;
    table_info->table_->UpdateTupleInPlace(meta, insert_tuple, *rid);

    // delete old index
    auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (const auto &idx : index_info) {
      auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idx->index_.get());
      auto delete_key = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *hash_table->GetKeySchema(),
                                                 hash_table->GetKeyAttrs());
      hash_table->DeleteEntry(delete_key, *rid, exec_ctx_->GetTransaction());
    }

    // Insert new index
    for (const auto &idx : index_info) {
      auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idx->index_.get());
      auto insert_key = insert_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *hash_table->GetKeySchema(),
                                                  hash_table->GetKeyAttrs());
      bool success = hash_table->InsertEntry(insert_key, *rid, exec_ctx_->GetTransaction());
      if (!success) {
        BUSTUB_ASSERT(0, "Error: Index insert error");
      }
    }

    total_update_++;
  }

  return false;
}

}  // namespace bustub
//
// create table t1(v1 int, v2 int);
// insert into t1 values (1,1), (2,2), (3,3), (4,4);
// update t1 set v2=114514;
