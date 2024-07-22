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

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(tuple, rid);
    if (!status) {
      return false;
    }

    const TupleMeta meta = {0 , false};
    const auto insert = info_->table_->InsertTuple(meta, *tuple,
                               exec_ctx_->GetLockManager(),exec_ctx_->GetTransaction(),
                               plan_->GetTableOid());
    if(insert.has_value()){
      *rid = insert.value();
      return true;
    }
  }
}

}  // namespace bustub
