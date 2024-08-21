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
    AtomicModifiedTuple(table_info_,
                        exec_ctx_->GetTransaction(),
                        exec_ctx_->GetTransactionManager(),
                        *rid,
                        true,
                        child_tuple,
                        child_executor_->GetOutputSchema(),
                        true);
    CheckUncommittedTransactionValid(table_info_, exec_ctx_->GetTransaction());
    total_delete_++;
  }

  return false;
}
}  // namespace bustub
