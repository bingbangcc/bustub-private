//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  if (left_executor_ != nullptr && right_executor_ != nullptr) {
    left_executor_->Init();
    right_executor_->Init();
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;
  const Schema *left_schema = plan_->GetLeftPlan()->OutputSchema();
  const Schema *right_schema = plan_->GetRightPlan()->OutputSchema();
  // 获得左孩子的tuple
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    // 获得右孩子的tuple
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      // 判断二者key是否相同，相同则合并
      if (plan_->Predicate() == nullptr ||
          plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        std::vector<Value> values(GetOutputSchema()->GetColumnCount());
        auto &output_columns = GetOutputSchema()->GetColumns();
        for (uint32_t i = 0; i < GetOutputSchema()->GetColumnCount(); ++i) {
          values[i] = output_columns[i].GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
        }

        *tuple = Tuple(values, GetOutputSchema());
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
