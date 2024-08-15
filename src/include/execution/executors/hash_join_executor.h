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
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
namespace bustub {
/** JoinKey represents a key in an join operation */
struct JoinKey {
  /** The group-by values */
  std::vector<Value> join_key_;

  /**
   * Compares two join keys for equality.
   * @param other the other join key to be compared with
   * @return `true` if both join keys have equivalent bucket id, `false` otherwise
   */
  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.join_key_.size(); i++) {
      if (join_key_[i].CompareEquals(other.join_key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** JoinValue represents a value in an join operation */
struct JoinValue {
  std::vector<Value> val_;
  bool been_compared_ = false;
  /**
   * Compares two join val for equality.
   * @param other the other join val to be compared with
   * @return `true` if both join val are equivalent, `false` otherwise
   */
  auto operator==(const JoinValue &other) const -> bool {
    for (uint32_t i = 0; i < other.val_.size(); i++) {
      if (val_[i].CompareEquals(other.val_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** JoinValueBucket represents a bucket for each of the joinKey */
struct JoinValueBucket {
  /** The join values bucket */
  std::vector<JoinValue> val_bucket_;

  auto operator+=(const JoinValue &&other) -> void { val_bucket_.emplace_back(other); }

  auto operator+=(const JoinValue &other) -> void { val_bucket_.emplace_back(other); }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on JoinKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.join_key_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {
/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleHashJoinTable {
 public:
  auto Scan(const JoinKey &key) -> JoinValueBucket * {
    auto find_res = ht_.find(key);
    if (find_res == ht_.end()) {
      return nullptr;
    }
    return &find_res->second;
  }

  auto Insert(const JoinKey &key, const JoinValue &val) -> void {
    if (ht_.find(key) == ht_.end()) {
      JoinValueBucket new_bucket{};
      new_bucket += val;
      ht_.insert({key, std::move(new_bucket)});
      return;
    }
    ht_[key] += val;
  }
  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<JoinKey, JoinValueBucket>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const JoinKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const JoinValueBucket & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<JoinKey, JoinValueBucket>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

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
  auto MakeJoinKey(const Tuple *tuple, const Schema &schema, const std::vector<AbstractExpressionRef> &key_expr)
      -> JoinKey {
    std::vector<Value> keys{};
    keys.reserve(key_expr.size());
    for (const auto &expr : key_expr) {
      keys.emplace_back(expr->Evaluate(tuple, schema));
    }
    return {keys};
  }

  /** @return The tuple as an JoinValue */
  auto MakeJoinValue(const Tuple *tuple, const Schema &schema) -> JoinValue {
    std::vector<Value> vals;
    for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
      vals.emplace_back(tuple->GetValue(&schema, i));
    }
    return {vals};
  }

  auto GetNullValueFromSchema(const Schema &schema) -> std::vector<Value> {
    std::vector<Value> empty_group_by_type{};
    for (auto &col : schema.GetColumns()) {
      empty_group_by_type.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
    }
    return empty_group_by_type;
  }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The hash join table in memory. */
  SimpleHashJoinTable ht_;

  std::vector<Tuple> backup_tuple_{};

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  bool join_side_scan_ = false;
  [[maybe_unused]] bool done_ = false;
};

}  // namespace bustub
