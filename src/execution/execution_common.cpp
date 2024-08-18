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

auto GetTupleValueVector(const Schema *schema, const Tuple &tuple, std::vector<Value> &value){
  for(uint32_t i=0; i < schema->GetColumnCount(); ++i){
    value.emplace_back(tuple.GetValue(schema, i));
  }
}

/**
 *
 * Only used in update/delete!!!
 * Compare two tuple, return tuple before modification
 *
 * */
auto GetTupleModifyFields(const Schema *schema, const Tuple *before, const Tuple *after,
                          std::vector<bool> * modified_mask) -> std::pair<Tuple, std::vector<bool>>{
  BUSTUB_ASSERT(before!= nullptr, "empty before tuple");
  if(after == nullptr){
    return std::pair<Tuple, std::vector<bool>>
        {*before, std::vector<bool>(schema->GetColumnCount(), true)};
  }

  std::vector<Value> before_value;
  std::vector<Value> after_value;
  GetTupleValueVector(schema, *before, before_value);
  GetTupleValueVector(schema, *after, after_value);

  std::vector<bool> modify_fields;
  std::vector<Value> modify_value;
  std::vector<uint32_t> modify_idx;

  for (size_t i=0; i < before_value.size(); ++i){
    auto mask = (modified_mask!= nullptr) && (*modified_mask)[i];
    if(!before_value[i].CompareExactlyEquals(after_value[i]) || mask){
      modify_idx.emplace_back(i);
      modify_value.emplace_back(before_value[i]);
      modify_fields.emplace_back(true);
      continue ;
    }
    modify_fields.emplace_back(false);
  }

  Schema modify_schema = Schema::CopySchema(schema, modify_idx);
  Tuple modify_tuple = {modify_value, &modify_schema};

  return std::pair<Tuple, std::vector<bool>>{modify_tuple, modify_fields};
}

auto ModifyTupleToString(const Schema *schema, const UndoLog &undo_log ){
  uint32_t i = 0;
  uint32_t modify_field = 0;
  std::vector<uint32_t> modify_idx;
  std::vector<Value> base_value;
  std::string modify_string;
  modify_string += "(";
  for(auto modify:undo_log.modified_fields_){
    if(modify){
      modify_idx.emplace_back(i);
    }
    i++;
  }
  auto modify_schema =  Schema::CopySchema(schema, modify_idx);
  size_t cc = 0;
  for(auto modify:undo_log.modified_fields_){
    cc++;
    if(modify){
      modify_string += undo_log.tuple_.GetValue(&modify_schema, modify_field).ToString();
      modify_field++;
    }else{
      modify_string += "_";
    }
    if(cc == undo_log.modified_fields_.size()){
      modify_string += ")";
    }else{
      modify_string += ", ";
    }
  }

  return modify_string;
}

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
                            timestamp_t current_ts, RID rid, std::vector<UndoLog> &undo_logs) -> bool{
  auto undo_link = transaction_manager->GetUndoLink(rid);
  while(undo_link.has_value() && undo_link->IsValid()){
    auto undo_log = transaction_manager->GetUndoLog(undo_link.value());
    undo_logs.emplace_back(undo_log);
    if(undo_log.ts_ <= current_ts){
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
  for(auto [page_id, page_version_info]:txn_mgr->version_info_){
    for(auto [slot_num ,link]:page_version_info->prev_version_){
      RID rid(page_id, slot_num);
      auto rid_current_ts = table_heap->GetTuple(rid).first.ts_ > TXN_START_ID ? table_heap->GetTuple(rid).first.ts_ - TXN_START_ID:
                                                                               table_heap->GetTuple(rid).first.ts_;
      fmt::println(stderr, "RID={}/{}  (ts={}{})  Tuple={}",
                   page_id,slot_num,
                   table_heap->GetTuple(rid).first.ts_ > TXN_START_ID ? "Txn@":"",rid_current_ts,
                   table_heap->GetTuple(rid).first.is_deleted_?"<deleted>":table_heap->GetTuple(rid).second.ToString(&table_info->schema_));
      auto undo_link = txn_mgr->GetUndoLink(rid);
      while(undo_link.has_value() && undo_link->IsValid()){
        try {
          auto undo_log = txn_mgr->GetUndoLog(undo_link.value());
          fmt::println(stderr, "         (ts={})  Txn@{}  Modify:{}", undo_log.ts_,
                       undo_link.value().prev_txn_ - TXN_START_ID,
                       undo_log.is_deleted_ ? "<deleted>" : ModifyTupleToString(&table_info->schema_, undo_log));
          undo_link = undo_log.prev_version_;
        }catch (std::exception &e){
          break ;
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

}  // namespace bustub
