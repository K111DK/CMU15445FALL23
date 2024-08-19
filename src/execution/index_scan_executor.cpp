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
#include "execution/execution_common.h"

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
  //auto txn_manager = exec_ctx_->GetTransactionManager();
  auto [meta, tp] = info_->table_->GetTuple(result_tmp[0]);
  auto self_modified = meta.ts_ == txn->GetTransactionTempTs();

  //Tuple modified by txn itself, and no deleted
  if(self_modified){
    *tuple = tp;
    *rid = result_tmp[0];
    done_ = true;
    return !meta.is_deleted_;
  }

  // Get all undo logs;
  std::vector<UndoLog> undo_logs;

//  //Get version link lock
//  {
//    std::unique_lock<std::shared_mutex> lck(txn_manager->version_info_mutex_);
//    auto version_link = txn_manager->GetVersionLink(result_tmp[0]);
//    BUSTUB_ASSERT(version_link.has_value(), "Empty version link");
//    if (version_link->in_progress_) {
//      txn->SetTainted();
//      throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
//    }
//    version_link->in_progress_ = true;
//  }

  bool need_undo = meta.ts_ > txn->GetReadTs();
  if( need_undo ){

    std::optional<Tuple> reconstruct_tp{std::nullopt};
    bool got_valid_record = false;
    auto is_deleted = false;

    //Get undo logs
    got_valid_record =
        GetReconstructUndoLogs(exec_ctx_->GetTransactionManager(),
                               txn->GetReadTs(),
                               tp.GetRid(),
                               undo_logs);
    // Reconstruct tuple
    reconstruct_tp = ReconstructTuple(&plan_->OutputSchema(),
                                           tp,
                                           meta,
                                           undo_logs);

    if (reconstruct_tp.has_value()) {
      tp = reconstruct_tp.value();
    }

    if( !reconstruct_tp.has_value() || !got_valid_record ){
      is_deleted = true;
    }
    *tuple = tp;
    *rid = result_tmp[0];
    done_ = true;
    return !is_deleted;
  }

  *tuple = tp;
  *rid = result_tmp[0];
  done_ = true;
  return !meta.is_deleted_;
}

}  // namespace bustub