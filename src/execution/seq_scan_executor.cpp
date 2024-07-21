//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) , plan_(plan){}

void SeqScanExecutor::Init() {
  info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  iterator_ = std::make_shared<TableIterator>(info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iterator_->IsEnd()) {
    auto tp = iterator_->GetTuple().second;
    auto tp_meta = iterator_->GetTuple().first;
    if(!tp_meta.is_deleted_){
      *tuple = std::move(tp);
      *rid = iterator_->GetRID();
      ++(*iterator_);
      return true;
    }
    ++(*iterator_);
  }
  return false;
}

}  // namespace bustub
