#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)){}

void TopNExecutor::Init() {
  child_executor_->Init();
  top_n_heap_ref_ = std::make_shared<std::priority_queue<Tuple, std::vector<Tuple>, OrderByCmpLess>>(
      OrderByCmpLess(plan_->order_bys_, *plan_->output_schema_),
      std::vector<Tuple>());
  top_n_.clear();
  RID rid{};
  Tuple tuple{};
  while(true){
   bool status = child_executor_->Next(&tuple, &rid);
   if(!status){
     break;
   }
   if(top_n_heap_ref_->size() == plan_->GetN()){
     top_n_heap_ref_->push(std::move(tuple));
     top_n_heap_ref_->pop();
   }else {
     top_n_heap_ref_->push(std::move(tuple));
   }
  }
  while(!top_n_heap_ref_->empty()){
   top_n_.emplace(top_n_.begin(),top_n_heap_ref_->top());
   top_n_heap_ref_->pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(!top_n_.empty()){
   *tuple = top_n_.front();
   top_n_.erase(top_n_.begin());
   return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return top_n_.size();
};

}  // namespace bustub
