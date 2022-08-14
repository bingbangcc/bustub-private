//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
// 不能在lock_manager.h中包含transaction_manager.h否则会产生交叉引用的问题
// lock_manager.h中只要声明class TransactionManager;让编译器知道有这么个类就行
// 真正执行的时候在cpp文件中再包含
#include <utility>
#include <vector>
#include "common/logger.h"
#include "concurrency/transaction_manager.h"

// std::unordered_map<RID, LockRequestQueue> lock_table_;

namespace bustub {

std::list<LockManager::LockRequest>::iterator LockManager::GetIterator(std::list<LockRequest> *request_queue,
                                                                       txn_id_t txn_id) {
  // LOG_INFO("the request queue's size is %ld", request_queue->size());
  for (auto it = request_queue->begin(); it != request_queue->end(); ++it) {
    if (it->txn_id_ == txn_id) {
      return it;
    }
  }
  return request_queue->end();
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  // read uncomitted隔离等级的时候不需要对读进行加锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  // 2PL协议要求事务处于shrinking阶段的时候不允许获取锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
  // 找到当前待查tuple的lock request queu并插入当前得到request node
  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  lock_request_queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  // 获得锁的权限
  lock_request_queue->cv_.wait(
      lock, [&]() { return !lock_request_queue->is_writing_ || txn->GetState() == TransactionState::ABORTED; });
  // 如果当前进程已经aborted了，则其不能获得锁，返回异常
  // 判断aborted不能放在前面，因为可能在阻塞的过程中变成aborted
  // 因此只能在临将分配锁之前才进行判断
  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId());
    lock_request_queue->request_queue_.erase(iter);
    // 这里产生死锁的原因是：当前事务已经被abort
    // 如果仍然将该s锁加入到其s锁的set里的话
    // 如果另一个事务想获取该tuple的x锁，那其要等待所有s锁都被释放
    // 但这个abort事务的s锁永远不会释放，因此死锁
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 将该tuple放入到事务的s锁set里
  // 将该事务的lockrequest设置为已分配
  // s锁的数量+1
  txn->GetSharedLockSet()->emplace(rid);
  // 修改lock request queue相应的元数据
  GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId())->granted_ = true;
  lock_request_queue->share_lock_count_++;
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  LOG_INFO("txn %d wants rid %s x-lock", txn->GetTransactionId(), rid.ToString().c_str());

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  lock_request_queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  lock_request_queue->cv_.wait(lock, [&]() {
    // LOG_INFO("the tranaction %d is waiting", txn->GetTransactionId());
    return txn->GetState() == TransactionState::ABORTED ||
           (!lock_request_queue->is_writing_ && lock_request_queue->share_lock_count_ == 0);
  });
  // LOG_INFO("the tranaction %d is awaked", txn->GetTransactionId());

  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("the tranaction %d is aborted in LockExclusive", txn->GetTransactionId());
    auto iter = GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId());
    lock_request_queue->request_queue_.erase(iter);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId())->granted_ = true;
  lock_request_queue->is_writing_ = true;

  LOG_INFO("txn %d gets rid %s x-lock", txn->GetTransactionId(), rid.ToString().c_str());
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;

  if (lock_request_queue->upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  // 删除之前读的元数据
  txn->GetSharedLockSet()->erase(rid);
  lock_request_queue->share_lock_count_--;
  auto iter = GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId());
  iter->lock_mode_ = LockMode::EXCLUSIVE;
  iter->granted_ = false;
  // 占位update
  lock_request_queue->upgrading_ = true;
  lock_request_queue->cv_.wait(
      lock, [&]() { return !lock_request_queue->is_writing_ && lock_request_queue->share_lock_count_ == 0; });

  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId());
    lock_request_queue->request_queue_.erase(iter);
    lock_request_queue->upgrading_ = false;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 添加写的元数据
  txn->GetExclusiveLockSet()->emplace(rid);
  GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId())->granted_ = true;
  lock_request_queue->upgrading_ = false;
  lock_request_queue->is_writing_ = true;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  LOG_INFO("begin unlock txn %d  rid %s", txn->GetTransactionId(), rid.ToString().c_str());
  // unlock的时机确定让上层代码去保证，这里只进行unlock就行

  // 这个锁一直卡住
  std::unique_lock<std::mutex> lock(latch_);
  // 当前rid还没被锁住过
  if (lock_table_.find(rid) == lock_table_.end()) {
    LOG_INFO("not exist rid %s", rid.ToString().c_str());
    return false;
  }

  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  auto iter = GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId());

  assert(iter != lock_request_queue->request_queue_.end());

  LockMode lock_mode = iter->lock_mode_;
  lock_request_queue->request_queue_.erase(iter);

  if (!(lock_mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // 修改元数据
  if (lock_mode == LockMode::EXCLUSIVE) {
    lock_request_queue->is_writing_ = false;
    lock_request_queue->cv_.notify_all();
  } else {
    lock_request_queue->share_lock_count_--;
    if (lock_request_queue->share_lock_count_ == 0) {
      lock_request_queue->cv_.notify_all();
    }
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  LOG_INFO("finish unlock txn %d  rid %s", txn->GetTransactionId(), rid.ToString().c_str());
  return true;
}

// /** Lock table for lock requests. */
// std::unordered_map<RID, LockRequestQueue> lock_table_;
// /** Waits-for graph representation. */
// std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (find(waits_for_[t1].begin(), waits_for_[t1].end(), t2) == waits_for_[t1].end()) {
    waits_for_[t1].push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter = find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

bool LockManager::DFS(txn_id_t txn_id) {
  if (active_txn_set_.find(txn_id) != active_txn_set_.end()) {
    return true;
  }
  if (safe_txn_set_.find(txn_id) != safe_txn_set_.end()) {
    return false;
  }

  active_txn_set_.insert(txn_id);
  std::sort(waits_for_[txn_id].begin(), waits_for_[txn_id].end());
  for (txn_id_t next_txn_id : waits_for_[txn_id]) {
    if (DFS(next_txn_id)) {
      return true;
    }
  }

  active_txn_set_.erase(txn_id);
  safe_txn_set_.insert(txn_id);
  return false;
}
// std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;

// 如果产生死锁的话一定是因为最新插入的节点导致的，如果前面节点导致的话早就探测到了
// 因此不用考虑1->2->3->1和1->2->3->4->2这两种的差异，都是一样的要找到4即可
bool LockManager::HasCycle(txn_id_t *txn_id) {
  active_txn_set_.clear();
  safe_txn_set_.clear();
  txn_set_.clear();

  for (const auto &item : waits_for_) {
    txn_set_.insert(item.first);
  }

  for (const auto &txn : txn_set_) {
    if (safe_txn_set_.find(txn) != safe_txn_set_.end()) {
      continue;
    }

    // active_txn_set_.clear();
    if (DFS(txn)) {
      *txn_id = *std::max_element(active_txn_set_.begin(), active_txn_set_.end());
      return true;
    }
  }

  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> ans;
  for (auto &item : waits_for_) {
    txn_id_t t1 = item.first;
    for (auto &t2 : waits_for_[t1]) {
      ans.emplace_back(t1, t2);
    }
  }

  return ans;
}

// 这里abort都是隐式abort，因为显示调用TransactionManager::Abort的开销太大了
// 利用TransactionState::ABORTED来隐式abort，并将关系图回滚即可
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      waits_for_.clear();

      LOG_INFO("thread cycle detection is running");
      std::unique_lock<std::mutex> lock(latch_);

      // TODO(student): remove the continue and add your cycle detection and abort code here
      // 构件关系图
      for (auto &rid_lock : lock_table_) {
        std::vector<txn_id_t> granted_txn;
        std::vector<txn_id_t> ungranted_txn;
        for (auto &lock_request : rid_lock.second.request_queue_) {
          if (lock_request.granted_) {
            granted_txn.push_back(lock_request.txn_id_);
          } else {
            ungranted_txn.push_back(lock_request.txn_id_);
          }
        }

        for (auto &ungrant : ungranted_txn) {
          for (auto &grant : granted_txn) {
            AddEdge(ungrant, grant);
          }
        }
      }
      LOG_INFO("create graph finish");
      // 打印构建出的图
      // for (auto item : waits_for_) {
      //   LOG_INFO("t1 %d", item.first);
      //   for (auto t2 : item.second) {
      //     LOG_INFO("t2 %d", t2);
      //   }
      // }

      // 如果有环就abort
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        /*
          搜索的txn id的顺序要从小到大
          dfs的过程中也要从小到大进行遍历
        */
        LOG_INFO("the graph has cycle");
        auto txn = TransactionManager::GetTransaction(txn_id);
        LOG_INFO("the abort txn is %d", txn->GetTransactionId());
        txn->SetState(TransactionState::ABORTED);
        // 回滚关系图里和该abort节点相关的边

        // 将所有指向abort节点的边从图中删掉
        for (auto &rid : *txn->GetSharedLockSet()) {
          for (auto &lock_request : lock_table_[rid].request_queue_) {
            if (!lock_request.granted_) {
              RemoveEdge(lock_request.txn_id_, txn->GetTransactionId());
            }
          }
          lock_table_[rid].share_lock_count_--;
          if (lock_table_[rid].share_lock_count_ == 0) {
            lock_table_[rid].cv_.notify_all();
          }
        }

        for (auto &rid : *txn->GetExclusiveLockSet()) {
          for (auto &lock_request : lock_table_[rid].request_queue_) {
            if (!lock_request.granted_) {
              RemoveEdge(lock_request.txn_id_, txn->GetTransactionId());
              LOG_INFO("remove edge %d  %d", lock_request.txn_id_, txn->GetTransactionId());
            }
          }
          // 唤醒被该tuple的锁阻塞的事务
          lock_table_[rid].is_writing_ = false;
          lock_table_[rid].cv_.notify_all();
        }

        waits_for_.erase(txn->GetTransactionId());
        LOG_INFO("finish hascycle");
        // 打印删掉边之后的图
        // for (auto item : waits_for_) {
        //   LOG_INFO("t1 %d", item.first);
        //   for (auto t2 : item.second) {
        //     LOG_INFO("t2 %d", t2);
        //   }
        // }
      }
    }
  }
}

}  // namespace bustub
