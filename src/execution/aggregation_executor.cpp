//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_->Begin()) {
  // aht_ = std::make_unique<SimpleAggregationHashTable> (plan_->GetAggregates(), plan_->GetAggregateTypes());
  having_ = plan->GetHaving();
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  if (child_ != nullptr) {
    child_->Init();
  }
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    AggregateKey agg_key = MakeKey(&tuple);
    AggregateValue agg_value = MakeVal(&tuple);
    aht_->InsertCombine(agg_key, agg_value);
  }
  aht_iterator_ = aht_->Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_->End()) {
    // 得到当前遍历位置处的 aggregate key和value
    AggregateKey agg_key = aht_iterator_.Key();
    AggregateValue agg_value = aht_iterator_.Val();
    ++aht_iterator_;

    if (having_ == nullptr || having_->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_).GetAs<bool>()) {
      std::vector<Value> values(GetOutputSchema()->GetColumnCount());
      auto &output_columns = GetOutputSchema()->GetColumns();
      for (uint32_t i = 0; i < values.size(); ++i) {
        values[i] = output_columns[i].GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_);
      }

      *tuple = Tuple(values, GetOutputSchema());
      return true;
    }
  }

  return false;
}

}  // namespace bustub
