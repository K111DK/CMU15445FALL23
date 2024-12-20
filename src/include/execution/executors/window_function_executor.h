//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
using TupleRef = std::shared_ptr<Tuple>;
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The initial Window aggregate value for this aggregation executor */
  auto GenerateInitialWindowFuncValue(WindowFunctionType agg_type) -> Value {
    Value values{};
    switch (agg_type) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        values = ValueFactory::GetIntegerValue(0);
        break;
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
      case WindowFunctionType::Rank:
        // Others starts at null.
        values = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
    }
    return {values};
  }

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /**
   * Get frame's aggregation values
   * @param window_func: window function for frame, include func type, column expr, order_by expr
   * @param schema: schema for tuple
   * @param frame_begin: Begin tuple (rend) in frame
   * @param frame_end: End tuple (rbegin) in frame
   * @param[out] window_agg_val: Return window agg value
   * @return Window agg Values for a Frame
   * */
  auto GetPartitionWindowAggValue(const WindowFunctionPlanNode::WindowFunction &window_func, const Schema &schema,
                                  const std::vector<Tuple *>::reverse_iterator &frame_begin,
                                  const std::vector<Tuple *>::reverse_iterator &frame_end,
                                  std::vector<Value> &window_agg_val) -> void {
    Value agg_val = GenerateInitialWindowFuncValue(window_func.type_);
    auto get_order_by_key = [&](Tuple &tuple) -> AggregateKey {
      AggregateKey agg_key;
      for (auto [type, expr] : window_func.order_by_) {
        agg_key.group_bys_.emplace_back(expr->Evaluate(&tuple, schema));
      }
      return agg_key;
    };
    bool have_order_by = !window_func.order_by_.empty();

    int32_t rank = 1;
    int32_t true_rank = 1;
    int32_t frame_len = 0;
    for (auto iter = frame_begin; iter != frame_end; ++iter) {
      Tuple tuple = *(*iter);
      auto val = window_func.function_->Evaluate(&tuple, schema);
      switch (window_func.type_) {
        case WindowFunctionType::CountStarAggregate:
        case WindowFunctionType::CountAggregate:
          if (agg_val.IsNull()) {
            agg_val = ValueFactory::GetIntegerValue(0);
          }
          agg_val = agg_val.Add(Value(INTEGER, 1));
          break;
        case WindowFunctionType::SumAggregate:
          if (agg_val.IsNull()) {
            agg_val = ValueFactory::GetIntegerValue(0);
          }
          agg_val = agg_val.Add(val);
          break;
        case WindowFunctionType::MinAggregate:
          if (agg_val.IsNull()) {
            agg_val = val;
          }
          agg_val = agg_val.CompareLessThan(val) == CmpBool::CmpTrue ? agg_val : val;
          break;
        case WindowFunctionType::MaxAggregate:
          if (agg_val.IsNull()) {
            agg_val = val;
          }
          agg_val = agg_val.CompareGreaterThan(val) == CmpBool::CmpTrue ? agg_val : val;
          break;
        case WindowFunctionType::Rank:
          // Others starts at null.
          agg_val = Value(INTEGER, rank);
          auto next = iter + 1;
          ++true_rank;
          if (next != frame_end) {
            auto current_val = (get_order_by_key(**iter));
            auto next_val = get_order_by_key(**next);
            if (!(current_val == next_val)) {
              rank = true_rank;
            }
          }
          break;
      }
      frame_len++;
      if (have_order_by) {
        window_agg_val.emplace_back(agg_val);
      }
    }

    if (window_agg_val.empty()) {
      for (auto i = 0; i < frame_len; ++i) {
        window_agg_val.emplace_back(agg_val);
      }
    }
  }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> child_tuple_group_;
  std::unordered_map<Tuple *, std::vector<Value>> window_value_;
  std::vector<Tuple> output_tuple_;
};
}  // namespace bustub
