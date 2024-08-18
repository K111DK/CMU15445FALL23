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
    auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(info_->name_);
    BUSTUB_ASSERT(index_info.size() == 1 && index_info[0]->is_primary_key_,
                  "In p4, always assume there's only one primary index");
    const auto &primary_idx = index_info[0];
    auto primary_hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(primary_idx->index_.get());
    auto insert_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), *primary_hash_table->GetKeySchema(),
                                            primary_hash_table->GetKeyAttrs());

    //First, check uniqueness of primary key
    std::vector<RID> result{};
    primary_hash_table->ScanKey(insert_tuple, &result, txn);

    //Violate primary key uniqueness. Abort;
    if(!result.empty()){
      txn->SetTainted();
      throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
    }

    //Do insert
    auto insert_ts = exec_ctx_->GetTransaction()->GetTransactionTempTs();
    TupleMeta meta = {insert_ts, false};
    RID insert_rid;
    const auto insert = info_->table_->InsertTuple(meta, *tuple,
                                                   exec_ctx_->GetLockManager(),
                                                       exec_ctx_->GetTransaction(),
                                                   plan_->GetTableOid());

    //In project 4, we always assume insert is successful
    BUSTUB_ASSERT(insert.has_value(), "Insert fail!");
    insert_rid = insert.value();

    //Try update primary index
    bool try_update_primary_index = primary_hash_table->InsertEntry(insert_tuple,
                                                                    insert_rid,
                                                                    exec_ctx_->GetTransaction());

    //Fail! Other transaction already update index, mark insert tuple as deleted, then Abort
    if(!try_update_primary_index){
      meta = {insert_ts, true};
      info_->table_->UpdateTupleMeta(meta, insert_rid);
      txn->SetTainted();
      throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
    }

    //Success! Append write set
    exec_ctx_->GetTransaction()->AppendWriteSet(info_->oid_, insert_rid);
    exec_ctx_->GetTransactionManager()->UpdateUndoLink(insert_rid, std::nullopt);


    total_insert_++;
  }

  return false;
}

}  // namespace bustub
