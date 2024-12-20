#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/table/tuple.h"
namespace bustub {

/**
 *  Reconstruct Tuple by Undo logs
 *  @param schema [in]  Tuple's full schema
 *  @param base_tuple [out] Tuple to be reconstructed
 *  @param base_meta [out]  Meta data(timestamp, is_deleted) of base tuple
 *  @param undo_logs [in]   undo logs chain
 * */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

auto GetReconstructUndoLogs(TransactionManager *transaction_manager, timestamp_t current_ts, RID rid,
                            std::vector<UndoLog> &undo_logs) -> bool;

auto GetTupleValueVector(const Schema *schema, const Tuple &tuple, std::vector<Value> &value);

auto GetTupleModifyFields(const Schema *schema, const Tuple *before, const Tuple *after,
                          std::vector<bool> *modified_mask = nullptr) -> std::pair<Tuple, std::vector<bool>>;

auto EvaluateTuple(const Schema &eval_schema, const Schema &out_schema, const Tuple &tuple,
                   const std::vector<std::shared_ptr<AbstractExpression>> &expressions) -> Tuple;

auto FakeAbort(Transaction *txn) -> void;

auto VersionLinkInProgress(std::optional<VersionUndoLink> version_link) -> bool;
auto CheckPrimaryKeyGlobalUpdate(std::vector<IndexInfo *> &index_info,
                                 const std::vector<std::shared_ptr<AbstractExpression>> &update_expr) -> bool;
auto CheckPrimaryKeyNeedUpdate(std::vector<IndexInfo *> &index_info,
                               const std::vector<std::shared_ptr<AbstractExpression>> &update_expr)
    -> std::pair<bool, bool>;
auto CheckPrimaryKeyConflict(std::vector<IndexInfo *> &index_info, Transaction *txn, Tuple &tuple, const Schema &schema)
    -> std::optional<RID>;
auto AtomicInsertNewTuple(TableInfo *table_info, std::vector<IndexInfo *> &index_info, Transaction *txn,
                          Tuple &insert_tuple, const Schema &schema, LockManager *lck_manager) -> void;
auto AtomicModifiedTuple(TableInfo *table_info, Transaction *txn, TransactionManager *txn_manager, RID &rid,
                         bool do_deleted, Tuple &update_tuple, const Schema &schema, bool check_slot_deleted) -> void;
auto CheckUncommittedTransactionValid(TableInfo *table_info, Transaction *txn) -> void;
// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
