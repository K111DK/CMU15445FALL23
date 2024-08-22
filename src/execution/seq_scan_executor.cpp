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
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void SeqScanExecutor::Init() { iterator_ = std::make_shared<TableIterator>(info_->table_->MakeIterator()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto read_ts = exec_ctx_->GetTransaction()->GetReadTs();
  //auto txn_id_readable = exec_ctx_->GetTransaction()->GetTransactionIdHumanReadable();

  while (!iterator_->IsEnd()) {
    auto tp = iterator_->GetTuple().second;
    auto tp_meta = iterator_->GetTuple().first;
    auto is_deleted = tp_meta.is_deleted_;

    // Tuple in heap is modified by other txn
    if (tp_meta.ts_ != exec_ctx_->GetTransaction()->GetTransactionTempTs() && tp_meta.ts_ > read_ts) {
      std::vector<UndoLog> undo_logs;
      // Get all undo logs;
      bool got_valid_record =
          GetReconstructUndoLogs(exec_ctx_->GetTransactionManager(), read_ts, tp.GetRid(), undo_logs);

      // Reconstruct tuple
      auto reconstruct_tp = ReconstructTuple(&plan_->OutputSchema(), tp, tp_meta, undo_logs);

      if (reconstruct_tp.has_value()) {
        tp = reconstruct_tp.value();
      }

      is_deleted = !reconstruct_tp.has_value() || !got_valid_record;
    }

    if (!is_deleted) {
      if (plan_->filter_predicate_ != nullptr) {
        auto filter_expr = plan_->filter_predicate_;
        auto value = filter_expr->Evaluate(&tp, plan_->OutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          *tuple = std::move(tp);
          *rid = iterator_->GetRID();
          ++(*iterator_);
          return true;
        }
      } else {
        *tuple = std::move(tp);
        *rid = iterator_->GetRID();
        ++(*iterator_);
        return true;
      }
    }

    ++(*iterator_);
  }
  return false;
}

}  // namespace bustub
