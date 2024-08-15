#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  output_tuple_.clear();
  child_tuple_group_.clear();
  window_value_.clear();
  //throw NotImplementedException("WindowFunctionExecutor is not implemented");
  RID rid{};
  Tuple tuple{};
  while(child_executor_->Next(&tuple, &rid)){
    child_tuple_group_.emplace_back(std::move(tuple));
  }

  for(const auto &window_func:plan_->window_functions_) {
    std::unordered_map<AggregateKey, std::vector<Tuple*>> partition_groups{};
    bool have_order_by = !window_func.second.order_by_.empty();
    auto get_partition_key = [&](const Tuple &tuple, const Schema &schema) {
      struct AggregateKey partition_key;
      partition_key.group_bys_ = {};
      for (const auto &expr : window_func.second.partition_by_) {
        partition_key.group_bys_.emplace_back(expr->Evaluate(&tuple, schema));
      }
      return partition_key;
    };


    // Do ORDER BY first
    if(have_order_by) {
      auto cmp_func = OrderByCmp(window_func.second.order_by_, child_executor_->GetOutputSchema());
      std::sort(child_tuple_group_.begin(), child_tuple_group_.end(), cmp_func);
      std::reverse(child_tuple_group_.begin(), child_tuple_group_.end());
    }

    //Do partition
    for(auto & tp:child_tuple_group_){
      AggregateKey partition_key = get_partition_key(tp, child_executor_->GetOutputSchema());
      auto res = partition_groups.find(partition_key);
      if(res != partition_groups.end()){
        partition_groups[partition_key].emplace_back(&tp);
      }else{
        partition_groups[partition_key] = {};
        partition_groups[partition_key].emplace_back(&tp);
      }
    }

    //Do window agg
    for(auto & partition_group_pair: partition_groups){
      auto & partition_group = partition_group_pair.second;
      auto iter = partition_group.begin();
      while(iter != partition_group.end()){
        auto start = have_order_by ? iter: partition_group.begin();
        auto end = partition_group.end();
        Value window_agg_val = GetPartitionWindowAggValue(window_func.second,
                                                      child_executor_->GetOutputSchema(),
                                                      start,end);
        auto res = window_value_.find(*iter);
        if(res != window_value_.end()){
          window_value_[*iter].emplace_back(window_agg_val);
        }else{
          window_value_[*iter] = {};
          window_value_[*iter].emplace_back(window_agg_val);
        }
        ++iter;
      }
    }
  }

  for(auto &tp:child_tuple_group_){
    std::vector<Value> out_tp{};
    uint32_t normal_col_size = plan_->OutputSchema().GetColumnCount() - plan_->window_functions_.size();
    for(uint32_t i = 0;i<normal_col_size;++i){
      out_tp.emplace_back(plan_->columns_[i]->Evaluate(&tp, child_executor_->GetOutputSchema()));
    }
    auto &window_agg_val = window_value_[&tp];
    std::reverse(window_agg_val.begin(),window_agg_val.end());
    out_tp.insert(out_tp.end(),
                  window_agg_val.begin(),
                  window_agg_val.end());
    output_tuple_.emplace_back(std::move(out_tp), &plan_->OutputSchema());
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(!output_tuple_.empty()){
    *tuple = output_tuple_.back();
    output_tuple_.pop_back();
    return true;
  }
  return false;
}
}  // namespace bustub
