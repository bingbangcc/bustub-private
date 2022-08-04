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

  key_schema_index_.reserve(inner_index_info_->key_schema_.GetColumnCount());
  for (uint32_t i = 0; i < inner_index_info_->key_schema_.GetColumnCount(); ++i) {
    std::string col_name = inner_index_info_->key_schema_.GetColumn(i).GetName();
    key_schema_index_.push_back(outer_table_schema_->GetColIdx(col_name));
  }
}

void NestIndexJoinExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    std::vector<Value> key_values(key_schema_index_.size());
    for (uint32_t i = 0; i < key_schema_index_.size(); ++i) {
      key_values[i] = tuple->GetValue(outer_table_schema_, key_schema_index_[i]);
    }
    // 构建outer_table的key索引
    Tuple outer_key(key_values, &inner_index_info_->key_schema_);

    std::vector<RID> find_result;
    inner_index_info_->index_->ScanKey(outer_key, &find_result, exec_ctx_->GetTransaction());
    if (find_result.empty()) {
      return false;
    }

    RID *target_rid = &find_result[0];
    Tuple inner_tuple(*target_rid);
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
