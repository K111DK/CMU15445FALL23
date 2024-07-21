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
    *tuple = iterator_->GetTuple().second;
    *rid = iterator_->GetRID();
    ++(*iterator_);
    return true;
  }
  return false;
}

}  // namespace bustub
