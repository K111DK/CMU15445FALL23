#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> base_value{};
  bool is_delete = base_meta.is_deleted_;
  for(uint32_t i=0; i < schema->GetColumnCount(); ++i){
    base_value.emplace_back(base_tuple.GetValue(schema, i));
  }
  for(const auto & undo_log : undo_logs){
    uint32_t i = 0;
    std::vector<uint32_t> modify_idx;

    for(auto modify:undo_log.modified_fields_){
      if(modify){
        modify_idx.emplace_back(i);
      }
      i++;
    }

    auto modify_schema =  Schema::CopySchema(schema, modify_idx);

    if(undo_log.is_deleted_){
      is_delete = true;
      continue ;
    }

    is_delete = false;
    for(i = 0; i < modify_idx.size(); ++i){
      base_value[ modify_idx[i] ] = undo_log.tuple_.GetValue(&modify_schema,i);
    }

  }

  if(is_delete){
    return {std::nullopt};
  }
  return {{base_value, schema}};

}

auto GetReconstructUndoLogs(TransactionManager * transaction_manager,
                            timestamp_t current_ts, RID rid) -> std::vector<UndoLog> {
  std::vector<UndoLog> undo_logs{};
  auto undo_link = transaction_manager->GetUndoLink(rid);
  while(undo_link.has_value() && undo_link->IsValid()){
    auto undo_log = transaction_manager->GetUndoLog(undo_link.value());
    undo_logs.emplace_back(undo_log);
    if(undo_log.ts_ <= current_ts){
      break ;
    }
    undo_link = undo_log.prev_version_;
  }
  return undo_logs;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

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

}  // namespace bustub
