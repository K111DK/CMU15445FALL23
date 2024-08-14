#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)){
}

void SortExecutor::Init() {
  child_executor_->Init();
  store_tuple_ref_ = std::make_shared<std::vector<Tuple>>();
  RID rid{};
  Tuple tuple{};
  while(true){
    bool status = child_executor_->Next(&tuple, &rid);
    if(!status){
      break;
    }
    store_tuple_ref_->emplace_back(tuple);
  }
  std::sort((*store_tuple_ref_).begin(),
            (*store_tuple_ref_).end(),
            OrderByCmp(plan_->order_bys_,child_executor_->GetOutputSchema()));
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(!store_tuple_ref_->empty()){
      *tuple = store_tuple_ref_->front();
      store_tuple_ref_->erase(store_tuple_ref_->begin());
      return true;
  }
  return false;
}

}  // namespace bustub
