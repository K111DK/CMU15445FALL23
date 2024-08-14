#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"
#include "execution/plans/limit_plan.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {


  // TODO(student): implement sort + limit -> top N optimizer rule
  if( plan->GetType() == PlanType::Limit &&
      plan->GetChildAt(0)->GetType() == PlanType::Sort){
    const auto & limit_plan = dynamic_cast<const LimitPlanNode&>(*plan);
    const auto & sort_plan = dynamic_cast<const SortPlanNode&>(*plan->GetChildAt(0));
    const auto & top_n_plan = TopNPlanNode(plan->output_schema_,
                                   plan->GetChildAt(0),
                                   sort_plan.GetOrderBy(),
                                   limit_plan.GetLimit()
                                   );

    /* Do optimize on Sort's children */
    std::vector<AbstractPlanNodeRef> children;
    for (const auto &child : sort_plan.GetChildren()) {
      children.emplace_back(OptimizeSortLimitAsTopN(child));
    }
    return top_n_plan.CloneWithChildren(std::move(children));
  }
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  return optimized_plan;
}

}  // namespace bustub
