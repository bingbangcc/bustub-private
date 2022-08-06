//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), cur_iter_(nullptr, nullptr, 0), end_iter_(nullptr, nullptr, 0) {
  index_info_ = exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_meta_data_ = exec_ctx->GetCatalog()->GetTable(index_info_->table_name_);
}

void IndexScanExecutor::Init() {
  auto *b_plus_tree_index =
      dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get());
  cur_iter_ = b_plus_tree_index->GetBeginIterator();
  end_iter_ = b_plus_tree_index->GetEndIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (cur_iter_ != end_iter_) {
    *rid = (*cur_iter_).second;
    ++cur_iter_;
    table_meta_data_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(tuple, &table_meta_data_->schema_).GetAs<bool>()) {
      std::vector<Value> values(plan_->OutputSchema()->GetColumnCount());
      const Schema *output_schema = GetOutputSchema();

      auto output_columns = plan_->OutputSchema()->GetColumns();
      for (uint32_t i = 0; i < values.size(); ++i) {
        values[i] = output_columns[i].GetExpr()->Evaluate(tuple, &table_meta_data_->schema_);
      }

      *tuple = Tuple(values, output_schema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
