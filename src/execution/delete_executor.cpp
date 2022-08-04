//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_meta_data_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
  index_info_vec_ = exec_ctx->GetCatalog()->GetTableIndexes(table_meta_data_->name_);
}

void DeleteExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    if (!table_meta_data_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      return false;
    }

    for (auto &index_info : index_info_vec_) {
      auto cur_index = index_info->index_.get();
      Tuple key(tuple->KeyFromTuple(table_meta_data_->schema_, index_info->key_schema_, cur_index->GetKeyAttrs()));
      cur_index->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
