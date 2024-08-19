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
    : AbstractExecutor(exec_ctx), plan_(plan) {
  info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
  index_info_ = nullptr;
  htable_ = nullptr;
  auto idx_info = exec_ctx->GetCatalog()->GetTableIndexes(info_->name_);
  for (const auto &idx : idx_info) {
    if (idx->index_oid_ == plan->index_oid_) {
      index_info_ = idx;
      htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
    }
  }
}

void IndexScanExecutor::Init() { done_ = false; }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  std::vector<RID> result_tmp{};
  Tuple search_key = {{plan_->pred_key_->val_}, &index_info_->key_schema_};
  htable_->ScanKey(search_key, &result_tmp, exec_ctx_->GetTransaction());
  BUSTUB_ASSERT(result_tmp.size() <= 1, "More than 1 value for 1 key in h table");
  if (result_tmp.empty()) {
    done_ = true;
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();
  auto [meta, tp] = info_->table_->GetTuple(result_tmp[0]);
  auto future_tuple = meta.ts_ > txn->GetReadTs();
  auto self_modified = meta.ts_ == txn->GetTransactionTempTs();

  if(future_tuple && !self_modified){
    done_ = true;
    return false;
  }

  if (!meta.is_deleted_) {
    *tuple = tp;
    *rid = result_tmp[0];
    done_ = true;
    return true;
  }

  done_ = true;
  return false;
}

}