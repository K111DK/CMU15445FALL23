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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void InsertExecutor::Init() {
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
    const auto insert= info_->table_->InsertTuple(meta, *tuple,
                                                   exec_ctx_->GetLockManager(),
                                                   exec_ctx_->GetTransaction(),
                                                   plan_->GetTableOid());
    if(insert.has_value()){
      *rid = insert.value();

      auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(info_->name_);
      for(const auto &idx:index_info){
        auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idx->index_.get());
        auto insert_tuple = tuple->KeyFromTuple(
            child_executor_->GetOutputSchema(),
            *hash_table->GetKeySchema(),
            hash_table->GetKeyAttrs()
            );
        hash_table->InsertEntry(insert_tuple,
                                *rid,
                                exec_ctx_->GetTransaction());
      }

      return true;
    }
  }
}

}  // namespace bustub
