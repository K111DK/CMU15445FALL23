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

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
  info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

  const TupleMeta delete_meta = {0 , true};

  Tuple child_tuple{};
  // Get the next tuple
  const auto status = child_executor_->Next(&child_tuple, rid);

  if (!status) {
    *tuple = Tuple({{Value(INTEGER, 0)}, &GetOutputSchema()});
    return false;
  }

  auto index_info = exec_ctx_->GetCatalog()->GetTableIndexes(info_->name_);
  for(const auto &idx:index_info) {
    auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idx->index_.get());
    auto delete_key = child_tuple.KeyFromTuple(
        child_executor_->GetOutputSchema(),
        *hash_table->GetKeySchema(),
        hash_table->GetKeyAttrs());
    hash_table->DeleteEntry(delete_key, *rid, exec_ctx_->GetTransaction());
  }

  info_->table_->UpdateTupleMeta(delete_meta, *rid);
  *tuple = Tuple({{Value(INTEGER, 1)}, &GetOutputSchema()});
  return true;
}

}  // namespace bustub
