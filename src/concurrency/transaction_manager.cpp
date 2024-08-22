//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_);
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  if (txn->GetTransactionId() == INVALID_TXN_ID) {
    return true;
  }
  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_ + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_.store(commit_ts);
  auto write_sets = txn->GetWriteSets();
  for (auto [table_oid, rid_sets] : write_sets) {
    auto table_info = catalog_->GetTable(table_oid);
    for (auto rid : rid_sets) {
      auto [meta, tuple] = table_info->table_->GetTuple(rid);
      BUSTUB_ASSERT(meta.ts_ == txn->GetTransactionTempTs(), "Try commit Txn whose modification is lost");
      meta.ts_ = commit_ts;
      table_info->table_->UpdateTupleMeta(meta, rid);
    }
  }

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  last_commit_ts_ = commit_ts;
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  auto watermark = GetWatermark();
  auto iter = txn_map_.begin();
  for (; iter != txn_map_.end();) {
    auto txn = iter->second;
    bool need_gc = true;
    for (auto [table_oid, rid_sets] : txn->GetWriteSets()) {
      if (!need_gc) {
        break;
      }
      for (auto rid : rid_sets) {
        auto link = GetUndoLink(rid);
        bool found = false;
        bool found_min_ts = false;
        auto [meta, tp] = catalog_->GetTable(table_oid)->table_->GetTuple(rid);
        found_min_ts = meta.ts_ <= watermark;
        while (link.has_value() && link.value().IsValid() && !found_min_ts) {
          if (link.value().prev_txn_ == txn->GetTransactionId()) {
            found = true;
          }
          auto undo_log = GetUndoLog(link.value());
          found_min_ts = undo_log.ts_ <= watermark;
          if (found_min_ts) {
            break;
          }
          link = undo_log.prev_version_;
        }
        if (found) {
          need_gc = false;
          break;
        }
      }
    }
    if (need_gc && txn->GetTransactionState() == TransactionState::COMMITTED) {
      iter = txn_map_.erase(iter);
    } else {
      iter++;
    }
  }
}

}  // namespace bustub
