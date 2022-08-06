//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

// 读tuple的时候，根据tuple的位置找到tuple
// 同时要保证读取的各列是符合schema要求的

// schema里保存的是这个表的各个column
// column保存的是一个列的名字，长度等信息

// 涉及两方的schema
// 其一是表里存储数据时候满足的格式
// 其二是输出数据的时候要符合的格式
// 比如 存储的时候格式为：Age Gender Score Name
// 而输出的时候需要的格式为：Name Gender Age
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), cur_(nullptr, RID{}, nullptr), end_(nullptr, RID{}, nullptr) {
  table_meta_data_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
}

// ExecutorContext *exec_ctx
// const SeqScanPlanNode *plan
void SeqScanExecutor::Init() {
  // 指向的就是table中的tuple
  cur_ = table_meta_data_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_meta_data_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // std::cout << "carry out seq_scan_executor Next" << std::endl;
  while (cur_ != end_) {
    *rid = cur_->GetRid();
    *tuple = *cur_;
    cur_++;
    // 当没有谓词，或者符合谓词的要求的时候
    // 该tuple是需要向上传递进行输出的
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(tuple, &table_meta_data_->schema_).GetAs<bool>()) {
      const Schema *output_schema = GetOutputSchema();
      std::vector<Value> values(output_schema->GetColumnCount());
      auto output_columns = output_schema->GetColumns();

      for (uint32_t i = 0; i < values.size(); ++i) {
        // values[i] = tuple->GetValue(&table_meta_data_->schema_, out_schema_index_[i]);
        values[i] = output_columns[i].GetExpr()->Evaluate(tuple, &table_meta_data_->schema_);
      }
      *tuple = Tuple(values, output_schema);
      // std::cout << "success" << std::endl;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
