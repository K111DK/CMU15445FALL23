//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleHashJoinTable {
 public:
  auto Scan(const JoinKey &key) -> JoinValueBucket {
    auto find_res = ht_.find(key);
    if (find_res == ht_.end()) {
      return {};
    }
    return find_res->second;
  }

  auto Insert(const JoinKey &key, const JoinValue &val) -> void {
    if (ht_.find(key) == ht_.end()) {
      JoinValueBucket new_bucket{};
      new_bucket  +=  val;
      ht_.insert({key, std::move(new_bucket)});
      return ;
    }
    ht_[key]  += val;
  }
  /**
     * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  std::unordered_map<JoinKey, JoinValueBucket> ht_;
};


/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as an JoinKey */
  auto MakeJoinKey(const Tuple *tuple,  const SchemaRef& schema, std::vector<AbstractExpressionRef> & key_expr) -> JoinKey {
    std::vector<Value> keys{};
    for (const auto &expr : key_expr) {
      keys.emplace_back(expr->Evaluate(tuple, *schema));
    }
    return {keys};
  }

  /** @return The tuple as an JoinValue */
  auto MakeJoinValue(const Tuple *tuple,  const SchemaRef& schema) -> JoinValue {
    std::vector<Value> vals;
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      vals.emplace_back(tuple->GetValue(schema.get(), i));
    }
    return {vals};
  }


  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

};

}  // namespace bustub
