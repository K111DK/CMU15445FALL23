#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {
auto VersionLinkInProgress(std::optional<VersionUndoLink> version_link) -> bool {
  if (!version_link.has_value()) {
    return true;
  }
  return !version_link->in_progress_;
}

auto FakeAbort(Transaction *txn) -> void {
  txn->SetTainted();
  throw ExecutionException("Abort Txn@" + std::to_string(txn->GetTransactionIdHumanReadable()));
}

auto GetTupleValueVector(const Schema *schema, const Tuple &tuple, std::vector<Value> &value) {
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    value.emplace_back(tuple.GetValue(schema, i));
  }
}

auto EvaluateTuple(const Schema &eval_schema, const Schema &out_schema, const Tuple &tuple,
                   const std::vector<std::shared_ptr<AbstractExpression>> &expressions) -> Tuple {
  std::vector<Value> values{};
  values.reserve(out_schema.GetColumnCount());
  for (const auto &expr : expressions) {
    values.push_back(expr->Evaluate(&tuple, eval_schema));
  }
  return {values, &out_schema};
}
/**
 *
 * Only used in update/delete!!!
 * Compare two tuple, return tuple before modification
 *
 * */
auto GetTupleModifyFields(const Schema *schema, const Tuple *before, const Tuple *after,
                          std::vector<bool> *modified_mask) -> std::pair<Tuple, std::vector<bool>> {
  BUSTUB_ASSERT(before != nullptr, "empty before tuple");
  if (after == nullptr) {
    return std::pair<Tuple, std::vector<bool>>{*before, std::vector<bool>(schema->GetColumnCount(), true)};
  }

  std::vector<Value> before_value;
  std::vector<Value> after_value;
  GetTupleValueVector(schema, *before, before_value);
  GetTupleValueVector(schema, *after, after_value);

  std::vector<bool> modify_fields;
  std::vector<Value> modify_value;
  std::vector<uint32_t> modify_idx;

  for (size_t i = 0; i < before_value.size(); ++i) {
    auto mask = (modified_mask != nullptr) && (*modified_mask)[i];
    if (!before_value[i].CompareExactlyEquals(after_value[i]) || mask) {
      modify_idx.emplace_back(i);
      modify_value.emplace_back(before_value[i]);
      modify_fields.emplace_back(true);
      continue;
    }
    modify_fields.emplace_back(false);
  }

  Schema modify_schema = Schema::CopySchema(schema, modify_idx);
  Tuple modify_tuple = {modify_value, &modify_schema};

  return std::pair<Tuple, std::vector<bool>>{modify_tuple, modify_fields};
}

auto ModifyTupleToString(const Schema *schema, const UndoLog &undo_log) {
  uint32_t i = 0;
  uint32_t modify_field = 0;
  std::vector<uint32_t> modify_idx;
  std::vector<Value> base_value;
  std::string modify_string;
  modify_string += "(";
  for (auto modify : undo_log.modified_fields_) {
    if (modify) {
      modify_idx.emplace_back(i);
    }
    i++;
  }
  auto modify_schema = Schema::CopySchema(schema, modify_idx);
  size_t cc = 0;
  for (auto modify : undo_log.modified_fields_) {
    cc++;
    if (modify) {
      modify_string += undo_log.tuple_.GetValue(&modify_schema, modify_field).ToString();
      modify_field++;
    } else {
      modify_string += "_";
    }
    if (cc == undo_log.modified_fields_.size()) {
      modify_string += ")";
    } else {
      modify_string += ", ";
    }
  }

  return modify_string;
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> base_value{};
  bool is_delete = base_meta.is_deleted_;
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    base_value.emplace_back(base_tuple.GetValue(schema, i));
  }
  for (const auto &undo_log : undo_logs) {
    uint32_t i = 0;
    std::vector<uint32_t> modify_idx;

    for (auto modify : undo_log.modified_fields_) {
      if (modify) {
        modify_idx.emplace_back(i);
      }
      i++;
    }

    auto modify_schema = Schema::CopySchema(schema, modify_idx);

    if (undo_log.is_deleted_) {
      is_delete = true;
      continue;
    }

    is_delete = false;
    for (i = 0; i < modify_idx.size(); ++i) {
      base_value[modify_idx[i]] = undo_log.tuple_.GetValue(&modify_schema, i);
    }
  }

  if (is_delete) {
    return {std::nullopt};
  }
  return {{base_value, schema}};
}

auto GetReconstructUndoLogs(TransactionManager *transaction_manager, timestamp_t current_ts, RID rid,
                            std::vector<UndoLog> &undo_logs) -> bool {
  auto undo_link = transaction_manager->GetUndoLink(rid);
  while (undo_link.has_value() && undo_link->IsValid()) {
    auto undo_log = transaction_manager->GetUndoLog(undo_link.value());
    undo_logs.emplace_back(undo_log);
    if (undo_log.ts_ <= current_ts) {
      return true;
    }
    undo_link = undo_log.prev_version_;
  }
  return false;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  for (auto [page_id, page_version_info] : txn_mgr->version_info_) {
    for (auto [slot_num, link] : page_version_info->prev_version_) {
      RID rid(page_id, slot_num);
      auto rid_current_ts = table_heap->GetTuple(rid).first.ts_ > TXN_START_ID
                                ? table_heap->GetTuple(rid).first.ts_ - TXN_START_ID
                                : table_heap->GetTuple(rid).first.ts_;
      fmt::println(stderr, "RID={}/{}  (ts={}{})  Tuple={}", page_id, slot_num,
                   table_heap->GetTuple(rid).first.ts_ > TXN_START_ID ? "Txn@" : "", rid_current_ts,
                   table_heap->GetTuple(rid).first.is_deleted_
                       ? "<deleted>"
                       : table_heap->GetTuple(rid).second.ToString(&table_info->schema_));
      auto undo_link = txn_mgr->GetUndoLink(rid);
      while (undo_link.has_value() && undo_link->IsValid()) {
        try {
          auto undo_log = txn_mgr->GetUndoLog(undo_link.value());
          fmt::println(stderr, "         (ts={})  Txn@{}  Modify:{}", undo_log.ts_,
                       undo_link.value().prev_txn_ - TXN_START_ID,
                       undo_log.is_deleted_ ? "<deleted>" : ModifyTupleToString(&table_info->schema_, undo_log));
          undo_link = undo_log.prev_version_;
        } catch (std::exception &e) {
          break;
        }
      }
      fmt::println(stderr, "");
    }
  }
  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}


auto CheckPrimaryKeyNeedUpdate(std::vector<IndexInfo*> &index_info, const std::vector<std::shared_ptr<AbstractExpression>> &update_expr)
    -> bool {
  if (index_info.empty()) {
    return false;
  }
  auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info[0]->index_.get());

  return std::any_of(
      update_expr.begin(), update_expr.end(), [&](const std::shared_ptr<AbstractExpression> &expr) -> bool {
        auto arith_expr = std::dynamic_pointer_cast<ArithmeticExpression>(expr);
        if (arith_expr == nullptr) {
          return false;
        }
        if (arith_expr->GetChildren().size() != 2) {
          return false;
        }
        auto column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(arith_expr->GetChildAt(0));
        auto const_expr = std::dynamic_pointer_cast<ConstantValueExpression>(arith_expr->GetChildAt(1));
        if (column_expr == nullptr || const_expr == nullptr) {
          return false;
        }
        return (column_expr->GetColIdx() == hash_table->GetKeyAttrs()[0] && hash_table->GetMetadata()->IsPrimaryKey() &&
                const_expr->val_.CompareEquals(ValueFactory::GetZeroValueByType(const_expr->GetReturnType())) ==
                    CmpBool::CmpFalse);
      });
}
auto CheckPrimaryKeyConflict(std::vector<IndexInfo*> &index_info, Transaction* txn, Tuple &tuple, const Schema & schema) -> std::optional<RID> {
  if (index_info.empty()) {
    return std::nullopt;
  }
  const auto &primary_idx = index_info[0];
  auto primary_hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(primary_idx->index_.get());
  auto insert_key = tuple.KeyFromTuple(schema, *primary_hash_table->GetKeySchema(),
                                       primary_hash_table->GetKeyAttrs());
  // First, check uniqueness of primary key
  std::vector<RID> result{};
  primary_hash_table->ScanKey(insert_key, &result, txn);
  return result.empty() ? std::nullopt : std::make_optional<RID>(result[0]);
}
auto AtomicInsertNewTuple(TableInfo * table_info, std::vector<IndexInfo*> &index_info, Transaction* txn, Tuple &insert_tuple, const Schema & schema, LockManager * lck_manager) -> void {
  auto insert_ts = txn->GetTransactionTempTs();
  const auto insert = table_info->table_->InsertTuple({insert_ts, false}, insert_tuple, lck_manager,
                                                       txn, table_info->oid_);
  // In project 4, we always assume insert is successful
  BUSTUB_ASSERT(insert.has_value(), "Insert fail!");
  RID insert_rid = insert.value();
  const auto &primary_idx = index_info;
  if (!primary_idx.empty()) {
    BUSTUB_ASSERT(primary_idx.size() == 1, "We only support one index for each table");
    auto primary_hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(primary_idx[0]->index_.get());
    auto insert_key = insert_tuple.KeyFromTuple(schema, *primary_hash_table->GetKeySchema(),
                                                primary_hash_table->GetKeyAttrs());

    // Try update primary index
    bool try_update_primary_index = primary_hash_table->InsertEntry(insert_key, insert_rid, txn);

    // Fail! Other transaction already update index, mark insert tuple as deleted, then Abort
    if (!try_update_primary_index) {
      table_info->table_->UpdateTupleMeta({insert_ts, true}, insert.value());
      FakeAbort(txn);
    }
  }
  // Success! Append write set
  txn->AppendWriteSet(table_info->oid_, insert_rid);
}
auto AtomicModifiedTuple(TableInfo * table_info, Transaction* txn, TransactionManager * txn_manager, RID &rid, bool do_deleted, Tuple &update_tuple, const Schema & schema, bool check_slot_deleted)
    -> void {
  auto modify_ts = txn->GetTransactionTempTs();

  // Critical section begin
  auto [current_meta, current_tuple] = table_info->table_->GetTuple(rid);
  bool self_uncommitted_transaction = current_meta.ts_ == txn->GetTransactionTempTs();
  bool can_not_see = current_meta.ts_ > txn->GetReadTs();

  if (!self_uncommitted_transaction) {
    auto current_version_link = txn_manager->GetVersionLink(rid);
    VersionUndoLink modified_link = current_version_link.has_value() ? current_version_link.value() : VersionUndoLink();
    modified_link.in_progress_ = true;
    bool success = txn_manager->UpdateVersionLink(rid, modified_link, VersionLinkInProgress);
    if (!success) {
      FakeAbort(txn);
    }
  }

  // Abort if a larger ts modify is committed or Any other transaction is modifying
  bool do_abort = can_not_see && !self_uncommitted_transaction;
  if (do_abort) {
    FakeAbort(txn);
  }

  if (check_slot_deleted && !current_meta.is_deleted_) {
    FakeAbort(txn);
  }

  auto first_undo_version = txn_manager->GetUndoLink(rid);

  // If this tuple haven't been modified by this txn yet, append undo log, update link
  if (!self_uncommitted_transaction) {
    auto [modified_tp, modified_fields] =
        GetTupleModifyFields(&schema, &current_tuple, &update_tuple);
    UndoLog undo_log;
    undo_log.is_deleted_ = current_meta.is_deleted_;
    undo_log.ts_ = current_meta.ts_;
    undo_log.modified_fields_ = modified_fields;
    undo_log.tuple_ = modified_tp;
    undo_log.prev_version_ = first_undo_version.has_value() ? first_undo_version.value() : UndoLink();
    auto new_first_undo_version = txn->AppendUndoLog(undo_log);
    // Atomically update link
    txn_manager->UpdateUndoLink(rid, new_first_undo_version);
  } else {
    if (first_undo_version.has_value() && first_undo_version.value().IsValid()) {
      UndoLog old_undo_log = txn_manager->GetUndoLog(first_undo_version.value());
      UndoLog new_undo_log = old_undo_log;
      auto before_modified =
          ReconstructTuple(&schema, current_tuple, current_meta, {old_undo_log});
      if (before_modified.has_value()) {
        auto [modified_tp, modified_fields] =
            GetTupleModifyFields(&schema, &before_modified.value(), &update_tuple,
                                 &old_undo_log.modified_fields_);
        new_undo_log.modified_fields_ = modified_fields;
        new_undo_log.tuple_ = modified_tp;
      }
      // Atomically update undo log
      txn->ModifyUndoLog(first_undo_version->prev_log_idx_, new_undo_log);
    }
  }

  // do modify job (don't need lock since we're in snapshot read) only one transaction reach here
  table_info->table_->UpdateTupleInPlace({modify_ts, do_deleted}, update_tuple, rid);
  txn->AppendWriteSet(table_info->oid_, rid);
}
auto CheckUncommittedTransactionValid(TableInfo * table_info, Transaction* txn) -> void {
  auto write_set = txn->GetWriteSets();
  for (auto [table_oid, rids] : write_set) {
    for (auto rid : rids) {
      auto [meta, tp] = table_info->table_->GetTuple(rid);
      if (meta.ts_ != txn->GetTransactionTempTs()) {
        throw ExecutionException("Update: Uncommitted txn is modified by other txn");
      }
    }
  }
}

}  // namespace bustub
