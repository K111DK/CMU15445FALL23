#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_plan.filter_predicate_ == nullptr) {
      return optimized_plan;
    }
    auto compare_expr = std::dynamic_pointer_cast<ComparisonExpression>(seq_plan.filter_predicate_);
    if (!compare_expr) {
      return optimized_plan;
    }
    if (compare_expr->comp_type_ != ComparisonType::Equal) {
      return optimized_plan;
    }
    auto column_exp = std::dynamic_pointer_cast<ColumnValueExpression>(seq_plan.filter_predicate_->GetChildAt(0));
    auto const_exp = std::dynamic_pointer_cast<ConstantValueExpression>(seq_plan.filter_predicate_->GetChildAt(1));

    if (column_exp == nullptr || const_exp == nullptr) {
      return optimized_plan;
    }

    auto table_info = catalog_.GetTable(seq_plan.table_name_);
    auto index = catalog_.GetTableIndexes(table_info->name_);
    for (const auto &idx : index) {
      auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idx->index_.get());
      BUSTUB_ASSERT(hash_table != nullptr, "Unsupport hash index!");
      if (hash_table->GetKeyAttrs()[0] == column_exp->GetColIdx()) {
        return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, seq_plan.table_oid_, idx->index_oid_,
                                                   seq_plan.filter_predicate_, const_exp.get());
      }
    }

    return optimized_plan;
  }
  return optimized_plan;
}

}  // namespace bustub
