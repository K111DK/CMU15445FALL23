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
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
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
    AtomicModifiedTuple(*rid, child_tuple);
    total_delete_++;
  }

  return false;
}
auto DeleteExecutor::AtomicModifiedTuple(RID &rid, Tuple &update_tuple) -> void {
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
                             nullptr);
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
  table_info_->table_->UpdateTupleInPlace({modify_ts, true},
                                          update_tuple,rid);
  txn->AppendWriteSet(table_info_->oid_, rid);
}
}  // namespace bustub
