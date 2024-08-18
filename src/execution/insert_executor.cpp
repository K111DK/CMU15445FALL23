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

  while (!insert_done_) {
    // Get the next tuple
    const auto status = child_executor_->Next(tuple, rid);
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
          info_->table_->InsertTuple(meta, *tuple, exec_ctx_->GetLockManager(), txn, plan_->GetTableOid());

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
    auto insert_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), *primary_hash_table->GetKeySchema(),
                                            primary_hash_table->GetKeyAttrs());

    //First, check uniqueness of primary key
    std::vector<RID> result{};
    primary_hash_table->ScanKey(insert_tuple, &result, txn);

    //Violate primary key uniqueness;
    if(!result.empty()) {

      RID insert_rid;
      insert_rid = result[0];

      // Check if we could modified this: Can be seen && is deleted
      auto pg_read_guard = info_->table_->AcquireTablePageReadLock(insert_rid);
      auto [meta, exists_tp] = pg_read_guard.As<TablePage>()->GetTuple(insert_rid);
      auto can_see_the_tuple = (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs());
      pg_read_guard.Drop();
      if (!meta.is_deleted_ || can_see_the_tuple) {
        txn->SetTainted();
        throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
      }


      //Get version link lock
      {
        std::unique_lock<std::shared_mutex> lck(txn_manager->version_info_mutex_);
        auto version_link = txn_manager->GetVersionLink(insert_rid);
        BUSTUB_ASSERT(version_link.has_value(), "Empty version link");
        if (version_link->in_progress_) {
          txn->SetTainted();
          throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
        }
        version_link->in_progress_ = true;
      }

      bool is_same_transaction = meta.ts_ == txn->GetTransactionTempTs();
      //At most one transaction can reach here
      if (!is_same_transaction) {
        auto [modified_tp, modified_fields] =
            GetTupleModifyFields(&child_executor_->GetOutputSchema(), &exists_tp, &insert_tuple);
        auto first_undo_version = txn_manager->GetUndoLink(insert_rid);
        UndoLog undo_log;
        undo_log.is_deleted_ = meta.is_deleted_;
        undo_log.ts_ = meta.ts_;
        undo_log.modified_fields_ = modified_fields;
        undo_log.tuple_ = modified_tp;
        undo_log.prev_version_ = first_undo_version.has_value() ? first_undo_version.value() : UndoLink();
        auto new_first_undo_version = txn->AppendUndoLog(undo_log);
        txn_manager->UpdateUndoLink(insert_rid, new_first_undo_version);
        txn->AppendWriteSet(info_->oid_, insert_rid);
      } else {
        auto first_undo_version = txn_manager->GetUndoLink(insert_rid);
        if (first_undo_version.has_value() && first_undo_version.value().IsValid()) {
          UndoLog old_undo_log = txn_manager->GetUndoLog(first_undo_version.value());
          std::vector<UndoLog> undo_logs;
          GetReconstructUndoLogs(txn_manager, txn->GetReadTs(), insert_rid, undo_logs);
          auto original_tp = ReconstructTuple(&child_executor_->GetOutputSchema(), exists_tp, meta, undo_logs);
          auto [modified_tp, modified_fields] = GetTupleModifyFields(
              &child_executor_->GetOutputSchema(), &original_tp.value(), &insert_tuple, &old_undo_log.modified_fields_);

          UndoLog new_undo_log = old_undo_log;
          // We only change modified tp
          new_undo_log.modified_fields_ = modified_fields;
          new_undo_log.tuple_ = modified_tp;
          txn->ModifyUndoLog(first_undo_version->prev_log_idx_, new_undo_log);
        }
        txn->AppendWriteSet(info_->oid_, insert_rid);
      }

      // Do modify job
      meta.ts_ = txn->GetTransactionTempTs();
      meta.is_deleted_ = false;
      auto table_write_page_guard= info_->table_->AcquireTablePageWriteLock(insert_rid);
      info_->table_->UpdateTupleInPlaceWithLockAcquired(meta,
                                                        insert_tuple,
                                                        insert_rid,
                                                        table_write_page_guard.AsMut<TablePage>());
      table_write_page_guard.Drop();

      // Release version link lock
      {
        std::unique_lock<std::shared_mutex> lck(txn_manager->version_info_mutex_);
        auto version_link = txn_manager->GetVersionLink(insert_rid);
        version_link->in_progress_ = false;
      }

    }else {
      // Do insert
      auto insert_ts = txn->GetTransactionTempTs();
      TupleMeta meta = {insert_ts, false};
      RID insert_rid;
      const auto insert =
          info_->table_->InsertTuple(meta, *tuple, exec_ctx_->GetLockManager(), txn, plan_->GetTableOid());

      // In project 4, we always assume insert is successful
      BUSTUB_ASSERT(insert.has_value(), "Insert fail!");
      insert_rid = insert.value();

      // Try update primary index
      bool try_update_primary_index = primary_hash_table->InsertEntry(insert_tuple, insert_rid, txn);

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
