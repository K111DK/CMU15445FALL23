//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
class OrderByCmp {
 public:
  explicit OrderByCmp(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys, const Schema &schema)
      : order_bys_(order_bys), schema_(schema){};
  auto operator()(const Tuple &a, const Tuple &b) -> bool {
    for (const auto &order_by_pair : order_bys_) {
      BUSTUB_ASSERT(order_by_pair.first != OrderByType::INVALID, "Invalid OrderBy type!");
      auto expr = order_by_pair.second;
      auto val_a = expr->Evaluate(&a, schema_);
      auto val_b = expr->Evaluate(&b, schema_);
      if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
        continue;
      }

      auto less = val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
      if (order_by_pair.first == OrderByType::DESC) {
        return !less;
      }
      return less;
    }
    return true;
  };

 private:
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys_;
  const Schema &schema_;
};
/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> child_executor_;
  std::shared_ptr<std::vector<Tuple>> store_tuple_ref_ = nullptr;
};
}  // namespace bustub
