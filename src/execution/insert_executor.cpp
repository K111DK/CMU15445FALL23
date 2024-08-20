//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"
#include <memory>
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void InsertExecutor::Init() { child_executor_->Init(); }
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> update_tuple{};
  Tuple insert_tuple;
  while (!insert_done_) {
    // Get the next tuple
    const auto status = child_executor_->Next(&insert_tuple, rid);
    if (!status) {
      update_tuple.emplace_back(INTEGER, total_insert_);
      *tuple = Tuple(update_tuple, &GetOutputSchema());
      insert_done_ = true;
      return true;
    }
    auto conflict_result = CheckPrimaryKeyConflict(insert_tuple);

    // Violate primary key uniqueness;
    if (conflict_result.has_value()) {
      AtomicModifiedTuple(conflict_result.value(), false, insert_tuple);
    } else {
      // Do insert
      AtomicInsertNewTuple(insert_tuple);
    }
    CheckUncommittedTransactionValid();
    total_insert_++;
  }

  return false;
}
auto InsertExecutor::CheckPrimaryKeyConflict(Tuple & tuple) -> std::optional<RID>{
  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if(index_info.empty()){
    return std::nullopt;
  }
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
auto InsertExecutor::AtomicModifiedTuple(RID &rid, bool do_deleted, Tuple &update_tuple) -> void {
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

  if(!current_meta.is_deleted_){
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
auto InsertExecutor::AtomicInsertNewTuple(Tuple &insert_tuple) -> void {
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

    // Fail! Other transaction already update index
    if (!try_update_primary_index) {
      table_info_->table_->UpdateTupleMeta({insert_ts,true}, insert.value());
      FakeAbort(txn);
    }
  }
  // Success! Append write set
  txn->AppendWriteSet(table_info_->oid_, insert_rid);
}
auto InsertExecutor::CheckUncommittedTransactionValid() -> void {
  auto write_set = exec_ctx_->GetTransaction()->GetWriteSets();
  for(auto [table_oid,rids]:write_set){
    for(auto rid:rids){
      auto [meta, tp] = table_info_->table_->GetTuple(rid);
      if(meta.ts_ != exec_ctx_->GetTransaction()->GetTransactionTempTs()){
        throw ExecutionException("Insert: Uncommitted txn is modified by other txn");
      }
    }
  }
}
}  // namespace bustub
