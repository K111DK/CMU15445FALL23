//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_(plan) {
    info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
    index_info_ = nullptr;
    htable_ = nullptr;
    auto idx_info = exec_ctx->GetCatalog()->GetTableIndexes(info_->name_);
    for(const auto &idx:idx_info){
      if(idx->index_oid_ == plan->index_oid_){
        index_info_ = idx;
        htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
      }
    }
}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    std::vector<RID> result_tmp{};
    Tuple search_key = {{plan_->pred_key_->val_}, &index_info_->key_schema_};
    htable_->ScanKey(search_key, &result_tmp, exec_ctx_->GetTransaction());
    BUSTUB_ASSERT(result_tmp.size() <= 1, "More than 1 value for 1 key in h table");
    auto tp = info_->table_->GetTuple(result_tmp[0]);
    if(!tp.first.is_deleted_){
      *tuple = tp.second;
      return true;
    }
    return false;
}

}  // namespace bustub
