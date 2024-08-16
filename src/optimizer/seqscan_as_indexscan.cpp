#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
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

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (filter_plan.GetPredicate()->GetChildren().empty()) {
      return optimized_plan;
    }
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
      if (seq_scan_plan.filter_predicate_ == nullptr) {
        BUSTUB_ASSERT(filter_plan.GetChildren().size() == 1, "must have exactly one children");
        auto column_exp = std::dynamic_pointer_cast<ColumnValueExpression>(filter_plan.GetPredicate()->GetChildAt(0));
        auto const_exp = std::dynamic_pointer_cast<ConstantValueExpression>(filter_plan.GetPredicate()->GetChildAt(1));

        if (column_exp == nullptr || column_exp == nullptr) {
          return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                   seq_scan_plan.table_name_, filter_plan.GetPredicate());
        }

        auto table_info = catalog_.GetTable(seq_scan_plan.table_name_);
        auto index = catalog_.GetTableIndexes(table_info->name_);
        for (const auto &idx : index) {
          auto hash_table = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idx->index_.get());
          BUSTUB_ASSERT(hash_table != nullptr, "Unsupport hash index!");
          if (hash_table->GetKeyAttrs()[0] == column_exp->GetColIdx()) {
            return std::make_shared<IndexScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                       idx->index_oid_, filter_plan.GetPredicate(), const_exp.get());
          }
        }

        return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                 seq_scan_plan.table_name_, filter_plan.GetPredicate());
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
