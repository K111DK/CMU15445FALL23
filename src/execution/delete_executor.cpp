//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  // Get the next tuple
  while (!delete_done_) {
    const auto status = child_executor_->Next(&child_tuple, rid);

    if (!status) {
      *tuple = Tuple({{Value(INTEGER, total_delete_)}, &GetOutputSchema()});
      delete_done_ = true;
      return true;
    }

    auto txn = exec_ctx_->GetTransaction();
    auto txn_manager = exec_ctx_->GetTransactionManager();
    auto read_ts = txn->GetReadTs();
    auto modify_ts = txn->GetTransactionIdHumanReadable() + TXN_START_ID;
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
      auto [modified_tp, modified_fields] = GetTupleModifyFields(&child_executor_->GetOutputSchema(), &tp, nullptr);
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
    }

    // do modify job
    meta.ts_ = modify_ts;
    meta.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(meta, *rid);

    //We don't do any modified on (primary)index info
//    auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(info_->name_);
//    for (const auto &idx : index_info) {
//      auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idx->index_.get());
//      auto delete_key = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *hash_table->GetKeySchema(),
//                                                 hash_table->GetKeyAttrs());
//      hash_table->DeleteEntry(delete_key, *rid, exec_ctx_->GetTransaction());
//    }

    total_delete_++;
  }

  return false;
}

}  // namespace bustub
