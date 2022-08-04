//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
  index_info_vec_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

// page里的数据是从高地址向低地址扩展，元数据是从低地址向高地址扩展

// 索引表里存储的是rid，其由page_id和slum_id组成，page_id决定在哪个page，slum_id决定该tuple是该page里的第几个数据，
// 但是具体的tuple在page里的地址是由page里的元数据来进行最后指引的，
// 即page_id找到相应的page，slum_id找到tuple在该page中的序号，
// 根据序号搜索元数据表得到tuple在该page里的具体位置，
// 因此在update操作中虽然会对page中tuple的具体位置进行修改，但是不会影响索引表中rid的值。
bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    Tuple new_tuple = GenerateUpdatedTuple(*tuple);
    if (!table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction())) {
      return false;
    }

    for (auto &index_info : index_info_vec_) {
      auto cur_index = index_info->index_.get();
      Tuple old_key(tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, cur_index->GetKeyAttrs()));
      Tuple new_key(new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, cur_index->GetKeyAttrs()));
      cur_index->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
      cur_index->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}
}  // namespace bustub
