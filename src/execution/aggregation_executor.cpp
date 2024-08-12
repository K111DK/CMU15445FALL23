//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {
// explain select office_hour, sum(office_hour), max(office_hour), count(*), count(office_hour=1) from __mock_table_tas_2023_fall group by office_hour;
//Agg   {
//      types=["sum", "max", "count_star", "count"],
//      aggregates=["#0.1", "#0.1", "1", "(#0.1=1)"],
//      group_by=["#0.1"]
//      }
//      |
//      (
//      __mock_table_tas_2023_fall.office_hour:VARCHAR,
//      <unnamed>:INTEGER,
//      <unnamed>:INTEGER,
//      <unnamed>:INTEGER,
//      <unnamed>:INTEGER
//      )
//                                                                                                                                  MockScan { table=__mock_table_tas_2023_fall } | (__mock_table_tas_2023_fall.github_id:VARCHAR, __mock_table_tas_2023_fall.office_hour:VARCHAR)

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin())
      {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  while(true){
    Tuple child_tuple{};
    RID rid{};
    const auto status = child_executor_->Next(&child_tuple, &rid);
    if (!status) {
      aht_iterator_ = aht_.Begin();
      break ;
    }
    auto agg_key = MakeAggregateKey(&child_tuple);
    auto agg_val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(agg_key, agg_val);
    agg_cnt_++;
  }


}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(!done_){
    std::vector<Value> output_val{};
    while(aht_iterator_ != aht_.End()){
      output_val.insert(output_val.end(),
                      aht_iterator_.Key().group_bys_.begin(),
                      aht_iterator_.Key().group_bys_.end());
      output_val.insert(output_val.end(),
                      aht_iterator_.Val().aggregates_.begin(),
                      aht_iterator_.Val().aggregates_.end());
      *tuple = { std::move(output_val),
                 &plan_->OutputSchema()};
      ++aht_iterator_;
      return true;
    }
    done_ = true;
    if(agg_cnt_ == 0 && plan_->group_bys_.empty()){

      std::vector<Value> empty_group_by_type{};
      for(auto &col: child_executor_->GetOutputSchema().GetColumns()){
        empty_group_by_type.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      Tuple empty_group_by_tuple = {empty_group_by_type, &child_executor_->GetOutputSchema()};

      AggregateKey agg_key = MakeAggregateKey(&empty_group_by_tuple);
      AggregateValue agg_val = aht_.GenerateInitialAggregateValue();
      output_val.insert(output_val.end(),
                        agg_key.group_bys_.begin(),
                        agg_key.group_bys_.end());

      output_val.insert(output_val.end(),
                        agg_val.aggregates_.begin(),
                        agg_val.aggregates_.end());

      *tuple = {std::move(output_val),
                &plan_->OutputSchema()};

      return true;

    }
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
