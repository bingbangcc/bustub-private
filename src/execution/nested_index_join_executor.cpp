//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  inner_table_info_ = exec_ctx->GetCatalog()->GetTable(plan->GetInnerTableOid());
  inner_index_info_ = exec_ctx->GetCatalog()->GetIndex(plan->GetIndexName(), inner_table_info_->name_);
  inner_table_schema_ = plan->InnerTableSchema();
  outer_table_schema_ = plan->OuterTableSchema();
  output_schema_ = plan->OutputSchema();

  auto outer_predict = dynamic_cast<const ColumnValueExpression *>(plan->Predicate()->GetChildAt(0));

  outer_col_idx_ = outer_predict->GetColIdx();
}

void NestIndexJoinExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    // 构建outer_table的key索引
    Tuple outer_key({tuple->GetValue(outer_table_schema_, outer_col_idx_)}, &inner_index_info_->key_schema_);

    std::vector<RID> find_result;
    inner_index_info_->index_->ScanKey(outer_key, &find_result, exec_ctx_->GetTransaction());
    if (find_result.empty()) {
      return false;
    }

    RID *target_rid = &(find_result[0]);
    Tuple inner_tuple;
    inner_table_info_->table_->GetTuple(*target_rid, &inner_tuple, exec_ctx_->GetTransaction());

    if (plan_->Predicate() == nullptr ||
        plan_->Predicate()->EvaluateJoin(tuple, outer_table_schema_, &inner_tuple, inner_table_schema_).GetAs<bool>()) {
      std::vector<Value> values(output_schema_->GetColumnCount());
      auto &output_columns = output_schema_->GetColumns();
      for (uint32_t i = 0; i < values.size(); ++i) {
        values[i] =
            output_columns[i].GetExpr()->EvaluateJoin(tuple, outer_table_schema_, &inner_tuple, inner_table_schema_);
      }

      *tuple = Tuple(values, output_schema_);
      return true;
    }
  }

  return false;
}

}  // namespace bustub
