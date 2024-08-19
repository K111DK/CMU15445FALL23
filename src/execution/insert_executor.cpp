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
  info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
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

    auto txn = exec_ctx_->GetTransaction();
    auto txn_manager = exec_ctx_->GetTransactionManager();
    auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(info_->name_);
    BUSTUB_ASSERT(index_info.size() <= 1, "In p4, always assume there's only one primary index");
    if(index_info.empty()){
      // Do insert
      auto insert_ts = txn->GetTransactionTempTs();
      TupleMeta meta = {insert_ts, false};
      RID insert_rid;
      const auto insert =
          info_->table_->InsertTuple(meta, insert_tuple, exec_ctx_->GetLockManager(), txn, plan_->GetTableOid());

      // In project 4, we always assume insert is successful
      BUSTUB_ASSERT(insert.has_value(), "Insert fail!");
      insert_rid = insert.value();

      // Success! Append write set
      txn->AppendWriteSet(info_->oid_, insert_rid);
      txn_manager->UpdateUndoLink(insert_rid, std::nullopt);
      total_insert_++;
      continue ;
    }


    const auto &primary_idx = index_info[0];
    auto primary_hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(primary_idx->index_.get());
    auto insert_key = insert_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *primary_hash_table->GetKeySchema(),
                                            primary_hash_table->GetKeyAttrs());

    //First, check uniqueness of primary key
    std::vector<RID> result{};
    primary_hash_table->ScanKey(insert_key, &result, txn);

    //Violate primary key uniqueness;
    if(!result.empty()) {

      RID insert_rid;
      insert_rid = result[0];

      // Check if we could modified this: Can be seen && is deleted
      auto pg_read_guard = info_->table_->AcquireTablePageReadLock(insert_rid);
      auto [meta, exists_tp] = pg_read_guard.As<TablePage>()->GetTuple(insert_rid);
      pg_read_guard.Drop();

      //this tuple must be deleted! o.w. insert violate pk unique rule
      if ( !meta.is_deleted_ ) {
        txn->SetTainted();
        throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
      }


      //If uncommitted deleted in same txn, just update tuple inplace. o.w. add undo log
      if (meta.ts_ != txn->GetTransactionTempTs()){
        //At most one transaction can reach here
        auto first_undo_version = txn_manager->GetUndoLink(insert_rid);
        UndoLog undo_log;
        undo_log.is_deleted_ = meta.is_deleted_;
        undo_log.ts_ = meta.ts_;
        undo_log.tuple_ = exists_tp;
        undo_log.prev_version_ = first_undo_version.has_value() ? first_undo_version.value() : UndoLink();
        undo_log.modified_fields_ = std::vector<bool>(child_executor_->GetOutputSchema().GetColumnCount(),
                                                      true);
        auto new_first_undo_version = txn->AppendUndoLog(undo_log);
        txn->AppendWriteSet(plan_->table_oid_, insert_rid);
        txn_manager->UpdateUndoLink(insert_rid, new_first_undo_version);
      }

      // Do modify job
      TupleMeta insert_meta = {txn->GetTransactionTempTs(), false};
      auto table_write_page_guard= info_->table_->AcquireTablePageWriteLock(insert_rid);
      info_->table_->UpdateTupleInPlaceWithLockAcquired(insert_meta,
                                                        insert_tuple,
                                                        insert_rid,
                                                        table_write_page_guard.AsMut<TablePage>());
      table_write_page_guard.Drop();

    }else {
      // Do insert
      auto insert_ts = txn->GetTransactionTempTs();
      TupleMeta meta = {insert_ts, false};
      RID insert_rid;
      const auto insert =
          info_->table_->InsertTuple(meta, insert_tuple, exec_ctx_->GetLockManager(), txn, plan_->GetTableOid());

      // In project 4, we always assume insert is successful
      BUSTUB_ASSERT(insert.has_value(), "Insert fail!");
      insert_rid = insert.value();

      // Try update primary index
      bool try_update_primary_index = primary_hash_table->InsertEntry(insert_key, insert_rid, txn);

      // Fail! Other transaction already update index, mark insert tuple as deleted, then Abort
      if (!try_update_primary_index) {
        meta = {insert_ts, true};
        info_->table_->UpdateTupleMeta(meta, insert_rid);
        txn->SetTainted();
        throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
      }

      // Success! Append write set
      txn->AppendWriteSet(info_->oid_, insert_rid);
      txn_manager->UpdateUndoLink(insert_rid, std::nullopt);
    }

    total_insert_++;
  }

  return false;
}

}  // namespace bustub
