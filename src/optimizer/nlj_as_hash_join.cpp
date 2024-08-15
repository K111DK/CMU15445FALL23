#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
#include "execution/plans/filter_plan.h"

namespace bustub {

auto PredicateHashJoinable(const AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> *left_key_expr,
                           std::vector<AbstractExpressionRef> *right_key_expr) -> bool {
  // Predicate must have exactly 2 children
  if (predicate->GetChildren().size() != 2) {
    return false;
  }

  // Predicate: <Child expr1> AND <Child expr2>
  if (auto and_exp = std::dynamic_pointer_cast<LogicExpression>(predicate)) {
    if (and_exp->logic_type_ != LogicType::And) {
      return false;
    }

    return PredicateHashJoinable(predicate->children_[0], left_key_expr, right_key_expr) &&
           PredicateHashJoinable(predicate->children_[1], left_key_expr, right_key_expr);
  }

  // Predicate: <Column i> = <Column j>
  if (auto cmp_expr = std::dynamic_pointer_cast<ComparisonExpression>(predicate)) {
    if (cmp_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }

    auto left_expr = std::dynamic_pointer_cast<ColumnValueExpression>(predicate->children_[0]);
    auto right_expr = std::dynamic_pointer_cast<ColumnValueExpression>(predicate->children_[1]);

    if (!left_expr || !right_expr) {
      return false;
    }

    if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
      left_key_expr->emplace_back(left_expr);
      right_key_expr->emplace_back(right_expr);
      return true;
    }

    if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
      right_key_expr->emplace_back(left_expr);
      left_key_expr->emplace_back(right_expr);
      return true;
    }
  }

  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  std::vector<AbstractExpressionRef> left_key_expressions{};
  std::vector<AbstractExpressionRef> right_key_expressions{};

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Filter && optimized_plan->GetChildren().size() == 1 &&
      optimized_plan->GetChildAt(0)->GetType() == PlanType::NestedLoopJoin) {
    auto merge_filter_plan = OptimizeMergeFilterNLJ(std::move(optimized_plan));
    optimized_plan = merge_filter_plan->CloneWithChildren(merge_filter_plan->GetChildren());
  }

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nested_join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nested_join_plan.predicate_ != nullptr) {
      //  recursively check if LoopJoin plan can be transformed into HashJoin
      bool can_optimize =
          PredicateHashJoinable(nested_join_plan.predicate_, &left_key_expressions, &right_key_expressions);

      if (can_optimize) {
        return std::make_shared<HashJoinPlanNode>(
            std::make_shared<Schema>(nested_join_plan.OutputSchema()), optimized_plan->GetChildAt(0),
            optimized_plan->GetChildAt(1), left_key_expressions, right_key_expressions, nested_join_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
