//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
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
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

  class OrderByCmpLess {
   public:
    explicit OrderByCmpLess(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys,
                            const Schema &schema)
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

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::shared_ptr<std::priority_queue<Tuple, std::vector<Tuple>, OrderByCmpLess>> top_n_heap_ref_;
  std::vector<Tuple> top_n_{};
};
}  // namespace bustub
