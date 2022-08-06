//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

// insert的时候schema
namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_meta_data_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
  index_info_vec_ = exec_ctx->GetCatalog()->GetTableIndexes(table_meta_data_->name_);
}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
  cur_insert_pos_ = 0;
}

// 插入的数据和在表中存储的数据的scheme是相同的

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    const std::vector<std::vector<Value>> &raw_values = plan_->RawValues();
    if (cur_insert_pos_ >= raw_values.size()) {
      return false;
    }
    const auto &values = raw_values[cur_insert_pos_++];
    // for (const auto& values : raw_values) {

    // *tuple = Tuple(values, &table_meta_data_->schema_);
    Tuple insert_tuple(values, &table_meta_data_->schema_);

    if (!table_meta_data_->table_->InsertTuple(insert_tuple, rid, exec_ctx_->GetTransaction())) {
      return false;
    }
    for (auto &index_info : index_info_vec_) {
      auto cur_index = index_info->index_.get();
      Tuple key(
          insert_tuple.KeyFromTuple(table_meta_data_->schema_, index_info->key_schema_, cur_index->GetKeyAttrs()));
      cur_index->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    }

    // }
    // std::cout << "raw insert success" << std::endl;
    return true;
  }

  Tuple insert_tuple;
  // 从child处获得insert数据
  if (child_executor_->Next(&insert_tuple, rid)) {
    if (!table_meta_data_->table_->InsertTuple(insert_tuple, rid, exec_ctx_->GetTransaction())) {
      return false;
    }
    for (auto &index_info : index_info_vec_) {
      auto cur_index = index_info->index_.get();
      Tuple key(
          insert_tuple.KeyFromTuple(table_meta_data_->schema_, index_info->key_schema_, cur_index->GetKeyAttrs()));
      cur_index->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    return true;
  }

  return false;
}

}  // namespace bustub
