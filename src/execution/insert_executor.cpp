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
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() { child_executor_->Init(); }
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> update_tuple{};
  Tuple insert_tuple;
  while (!insert_done_) {
    // Get the next tuple
    const auto status = child_executor_->Next(&insert_tuple, rid);
    if (!status) {
      CheckUncommittedTransactionValid(table_info_, exec_ctx_->GetTransaction());
      update_tuple.emplace_back(INTEGER, total_insert_);
      *tuple = Tuple(update_tuple, &GetOutputSchema());
      insert_done_ = true;
      return true;
    }
    auto conflict_result = CheckPrimaryKeyConflict(index_info_,
                                                   exec_ctx_->GetTransaction(),
                                                   insert_tuple,
                                                   child_executor_->GetOutputSchema());;


    // Violate primary key uniqueness;
    if (conflict_result.has_value()) {
      AtomicModifiedTuple(table_info_,
                          exec_ctx_->GetTransaction(),
                          exec_ctx_->GetTransactionManager(),
                          conflict_result.value(),
                          false,
                          insert_tuple,
                          child_executor_->GetOutputSchema(),
                          true);
    } else {
      // Do insert
      AtomicInsertNewTuple(table_info_,
                           index_info_,
                           exec_ctx_->GetTransaction(),
                           insert_tuple,
                           child_executor_->GetOutputSchema(),
                           exec_ctx_->GetLockManager());
    }
    total_insert_++;
  }

  return false;
}
}  // namespace bustub
