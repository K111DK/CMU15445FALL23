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
    AtomicInsertNewTuple(insert_tuple);
    total_insert_++;
  }

  return false;
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

    // Fail! Other transaction already update index, mark insert tuple as deleted, then Abort
    if (!try_update_primary_index) {
      txn->SetTainted();
      throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
    }
  }
  // Success! Append write set
  txn->AppendWriteSet(table_info_->oid_, insert_rid);
}
}  // namespace bustub
