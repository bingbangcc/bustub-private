//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"
namespace bustub {
// INDEX_TEMPLATE_ARGUMENTS
// int BPLUSTREE_TYPE::GetMaxSizeForDiffType(BPlusTreePage * node) const {

// }

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
// 点查询
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
// FindLeafPageByOperation最后leaf节点是在transaction里的，因此在外面要对leaf进行unpin等
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // Page *leaf_page = FindLeafPage(key, false);
  // LOG_INFO("Enter Function GetValue");
  Page *leaf_page = FindLeafPageByOperation(key, OperationType::FIND, transaction, false);
  // TODO
  // LOG_INFO("leaf page id is : %d", leaf_page->GetPageId());
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  ValueType value{};
  bool is_exist = leaf_node->Lookup(key, &value, comparator_);
  if (is_exist) {
    result->push_back(value);
  }
  UnlatchAndUnpin(transaction, OperationType::FIND);
  // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return is_exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
// 插入的时候
// 1. 如果现在是空树，则创建新的树进行插入
// 2. 不是空树，则插入一个叶子节点
// 2.1 插入的时候如果导致一个节点溢出则进行split, split的时候会产生新节点并向父节点中插入一个key
//     如果导致父节点也溢出，则递归的进行split

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  {
    std::lock_guard<std::mutex> guard(root_latch_);
    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
  }

  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::runtime_error("out of memory");
  }
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);

  LeafPage *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  new_node->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // LOG_INFO("Enter InsertIntoLeaf, the key is %ld ", key.ToString());
  // transaction里面存储了所有其目前还在保持的page
  Page *leaf_page = FindLeafPageByOperation(key, OperationType::INSERT, transaction, false);
  // Page *leaf_page = FindLeafPage(key, false);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // LOG_INFO("The target insert leaf page id is %d", leaf_node->GetPageId());

  // int key_index = leaf_node->KeyIndex(key, comparator_);
  // 如果当前key在leaf中已经存在，返回false
  ValueType leaf_value{};
  bool is_exist = leaf_node->Lookup(key, &leaf_value, comparator_);
  if (is_exist) {
    UnlatchAndUnpin(transaction, OperationType::INSERT);
    return false;
  }

  int current_size = leaf_node->Insert(key, value, comparator_);
  // 溢出，进行split
  if (current_size == leaf_max_size_) {
    LeafPage *new_leaf_node = Split<LeafPage>(leaf_node);
    InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
  }
  UnlatchAndUnpin(transaction, OperationType::INSERT);
  // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  // LOG_INFO("End InsertIntoLeaf, the key is %ld ", key.ToString());
  return true;
}

// 节点key太多的时候进行节点分裂
/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    // throw std::runtime_error("out of memory");
    throw std::runtime_error("out of memory in Split");
  }

  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  // new_node->SetPageType(node->GetPageType());
  // 编译的时候函数内部只看得到类型N，运行的时候才能确定是什么类型
  // MoveHalfTo在编译阶段，编译器不知道该把这个函数链接到哪个类的成员函数
  // 对编译器来说它可能把leaf和inter内部当成有两个MoveHalfTo重载函数了
  // 而加上明确的类型之后就可以明确指定编译器去调用哪个函数
  if (node->IsLeafPage()) {
    LeafPage *old_leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    old_leaf_node->MoveHalfTo(new_leaf_node);
    new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());
    old_leaf_node->SetNextPageId(new_page_id);
  } else {
    InternalPage *old_inter_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_inter_node = reinterpret_cast<InternalPage *>(new_node);
    old_inter_node->MoveHalfTo(new_inter_node, buffer_pool_manager_);
  }
  return new_node;
}

// 分裂后将指向新page的key和value存到父节点中
// 如果插入操作导致当前节点的key数 == max_size就要进行拆分
/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
// 传进来的参数不需要进行Unpin，因为对其unpin肯定是调用者的责任
// 在函数内部，函数只需要对其fetch和new的page进行unpin
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // LOG_INFO("Enter InsertIntoParent, the key is %ld ", key.ToString());
  if (old_node->IsRootPage()) {
    // B+ tree metadata
    std::lock_guard<std::mutex> guard(root_latch_);

    page_id_t new_root_page_id = INVALID_PAGE_ID;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (new_root_page == nullptr) {
      throw std::runtime_error("out of memory in InsertIntoParent");
    }
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(new_root_page_id);
    // root node metadata
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // child node metadata
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    // 处理结束，释放新的根节点
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    // LOG_INFO("End InsertIntoParent, the key is %ld ", key.ToString());
    return;
  }

  page_id_t parent_page_id = old_node->GetParentPageId();
  // 这里parent_page已经在transaction的set里
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int parent_current_size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // LOG_INFO("The parent node's size is %d", parent_current_size);

  // parent_current_size == parent_node->GetMaxSize()
  if (parent_current_size == internal_max_size_ + 1) {
    // LOG_INFO("current size is %d, max size is %d", parent_current_size, internal_max_size_);
    InternalPage *split_node = Split<InternalPage>(parent_node);
    KeyType new_parent_key = split_node->KeyAt(0);
    InsertIntoParent(parent_node, new_parent_key, split_node, transaction);
    buffer_pool_manager_->UnpinPage(split_node->GetPageId(), true);
  }
  // UnlatchAndUnpin(transaction, OperationType::INSERT);
  // buffer_pool_manager_->UnpinPage(parent_page_id, true);
  // LOG_INFO("End InsertIntoParent, the key is %ld ", key.ToString());
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  // Page* page = FindLeafPage(key, false);
  // transaction里只有internal的节点，没有最后的leaf节点，对于leaf节点要单独进行控制
  Page* page = FindLeafPageByOperation(key, OperationType::DELETE, transaction, false);
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(page->GetData());
  ValueType value{};
  if (!leaf_node->Lookup(key, &value, comparator_)) {
    UnlatchAndUnpin(transaction, OperationType::DELETE);
    return;
  }
  
  leaf_node->RemoveAndDeleteRecord(key, comparator_);
  bool should_delete = CoalesceOrRedistribute(leaf_node, transaction);
  // buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);

  if (should_delete) {
    // buffer_pool_manager_->DeletePage(leaf_node->GetPageId());
    transaction->AddIntoDeletedPageSet(leaf_node->GetPageId());
  }
  UnlatchAndUnpinAndDelete(transaction, OperationType::DELETE);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    std::lock_guard<std::mutex> guard(root_latch_);
    if (AdjustRoot(node)) {
      return true;
    }
    return false;
  }

  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }

  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  int node_index = parent_node->ValueIndex(node->GetPageId());

  Page* neighbor_page;
  if (node_index == 0) {
    neighbor_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(1));
  }
  else {
    neighbor_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(node_index-1));
  }
  neighbor_page->WLatch();
  transaction->AddIntoPageSet(neighbor_page);

  N* neighbor_node = reinterpret_cast<N*>(neighbor_page->GetData());

  if (neighbor_node->GetSize() + node->GetSize() >= node->GetMaxSize()) {
    // 重新分配
    Redistribute(neighbor_node, node, node_index);
    // UnlatchAndUnpin(transaction, OperationType::DELETE);
    // buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    return false;
  }
  // 合并
  bool parent_delete = Coalesce(&neighbor_node, &node, &parent_node, node_index, transaction);
  // buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  if (parent_delete) {
    transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    // buffer_pool_manager_->DeletePage(parent_node->GetPageId());
  }
  // UnlatchAndUnpin(transaction, OperationType::DELETE);
  // buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  // buffer_pool_manager_->DeletePage((*node)->GetPageId());
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */

// 要保证neighbor是node的前驱才能保证moveall之后是有序的
// 因此要严格限制neghbor和node的顺序关系以及index的值
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if (index == 0) {
    std::swap(neighbor_node, node);
    index = 1;
  }
  if ((*node)->IsLeafPage()) {
    LeafPage* leaf_neighbor_node = reinterpret_cast<LeafPage*>((*neighbor_node));
    LeafPage* leaf_current_node = reinterpret_cast<LeafPage*>((*node));
    leaf_current_node->MoveAllTo(leaf_neighbor_node);
    leaf_neighbor_node->SetNextPageId(leaf_current_node->GetNextPageId());
    // buffer_pool_manager_->DeletePage(leaf_current_node->GetPageId());
  }
  else {
    InternalPage* internal_neighbor_node = reinterpret_cast<InternalPage*>((*neighbor_node));
    InternalPage* internal_current_node = reinterpret_cast<InternalPage*>((*node));
    internal_current_node->MoveAllTo(internal_neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);
    // buffer_pool_manager_->DeletePage(internal_current_node->GetPageId());
  }
  // buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
  // buffer_pool_manager_->DeletePage((*node)->GetPageId());
  (*parent)->Remove(index);
  return CoalesceOrRedistribute((*parent), transaction);;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetPageId());
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());

  if (node->IsLeafPage()) {
    LeafPage* leaf_neighbor_node = reinterpret_cast<LeafPage*>(neighbor_node);
    LeafPage* leaf_current_node = reinterpret_cast<LeafPage*>(node);
    if (index == 0) {
      leaf_neighbor_node->MoveFirstToEndOf(leaf_current_node);
      parent_node->SetKeyAt(1, leaf_neighbor_node->KeyAt(0));
    }
    else {
      leaf_neighbor_node->MoveLastToFrontOf(leaf_current_node);
      parent_node->SetKeyAt(index, leaf_current_node->KeyAt(0));
    }
  }
  else {
    InternalPage* internal_neighbor_node = reinterpret_cast<InternalPage*>(neighbor_node);
    InternalPage* internal_current_node = reinterpret_cast<InternalPage*>(node);
    // KeyType parent_key = parent_node->KeyAt(index);
    if (index == 0) {
      KeyType parent_key = parent_node->KeyAt(1);
      internal_neighbor_node->MoveFirstToEndOf(internal_current_node, parent_key, buffer_pool_manager_);
      parent_node->SetKeyAt(1, internal_neighbor_node->KeyAt(0));
    }
    else {
      KeyType parent_key = parent_node->KeyAt(index);
      internal_neighbor_node->MoveLastToFrontOf(internal_current_node, parent_key, buffer_pool_manager_);
      parent_node->SetKeyAt(index, internal_current_node->KeyAt(0));
    }
  }

  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
// 根节点是叶子节点
// 1. size = 0,delete
// 根节点是中间节点
// 1. size = 1的时候用他的child来代替自己成为根节点
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {  
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      return true;
    }
  }
  else {
    if (old_root_node->GetSize() == 1) {
      InternalPage* old_internal_node = reinterpret_cast<InternalPage*>(old_root_node);
      page_id_t new_root_page_id = old_internal_node->RemoveAndReturnOnlyChild();
      Page* new_root_page = buffer_pool_manager_->FetchPage(new_root_page_id);
      LeafPage* new_root_node = reinterpret_cast<LeafPage*>(new_root_page->GetData());
      new_root_node->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = new_root_page_id;
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);
      return true;
    }
  }
  return false; 
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { 
  Page* left_most_leaf = FindLeafPage(KeyType{}, true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, left_most_leaf, 0); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
  Page* page = FindLeafPage(key, false);
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(page->GetData());
  int key_index = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, key_index); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  Page* page = FindLeafPage(KeyType{}, true);
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(page->GetData());
  while (leaf_node->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_leaf_page_id = leaf_node->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(next_leaf_page_id);
    leaf_node = reinterpret_cast<LeafPage*>(page->GetData());
  }
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, leaf_node->GetSize()); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *root_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  // 找到目标leaf_page
  while (!root_node->IsLeafPage()) {
    page_id_t target_page_id = INVALID_PAGE_ID;
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(root_node);
    if (leftMost) {
      target_page_id = internal_node->ValueAt(0);
    } else {
      target_page_id = internal_node->Lookup(key, comparator_);
    }

    root_page = buffer_pool_manager_->FetchPage(target_page_id);
    root_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
    buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
  }
  // buffer_pool_manager_->UnpinPage(root_page->GetPageId());
  return root_page;
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

// /** Concurrent index: the pages that were latched during index operation. */
// std::shared_ptr<std::deque<Page *>> page_set_;
// /** Concurrent index: the page IDs that were deleted during index operation.*/
// std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set_;

// Unlock和Unpin是连在一起进行的
INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType& key, OperationType op, Transaction* transaction, bool leftMost) {
  // LOG_INFO("Enter Function FindLeafPageByOperation");
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *root_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  // 找到目标leaf_page

  while (!root_node->IsLeafPage()) {
    // LOG_INFO("Enter Loop");
    if (op == OperationType::FIND) {
      root_page->RLatch();
    }
    else {
      root_page->WLatch();
    }
    if (IsSafe(root_node, op)) {
      UnlatchAndUnpin(transaction, op);
    }
    // LOG_INFO("here");
    transaction->AddIntoPageSet(root_page);
    // LOG_INFO("here");
    page_id_t target_page_id = INVALID_PAGE_ID;
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(root_node);
    if (leftMost) {
      target_page_id = internal_node->ValueAt(0);
    } else {
      target_page_id = internal_node->Lookup(key, comparator_);
    }
    
    // LOG_INFO("the target page id is : %d", target_page_id);

    root_page = buffer_pool_manager_->FetchPage(target_page_id);
    root_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
    // buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
  }
  // LOG_INFO("End LOOP");

  if (op == OperationType::FIND) {
    root_page->RLatch();
  }
  else {
    root_page->WLatch();
  }
  if (IsSafe(root_node, op)) {
    UnlatchAndUnpin(transaction, op);
  }
  transaction->AddIntoPageSet(root_page);
  // LOG_INFO("End Function FindLeafPageByOperation");
  return root_page;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IsSafe(N* node, OperationType op) {
  if (op == OperationType::FIND) {
    return true;
  }

  if (op == OperationType::INSERT) {
    return node->GetKeySize() < node->GetMaxSize();
  }

  if (node->IsRootPage()) {
    if (node->IsLeafPage()) {
      return true;
    }
    return node->GetSize() > 2;
  }
  return node->GetSize() > node->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlatchAndUnpin(Transaction* transaction, OperationType op) {
  // LOG_INFO("Enter UnlatchAndUnpin");
  if (transaction == nullptr) {
    // LOG_INFO("return");
    return;
  }
  // LOG_INFO("here 1");
  auto page_set = transaction->GetPageSet();
  // LOG_INFO("here 2");
  for (auto& page : *page_set) {
    // LOG_INFO("enter loop");
    if (op == OperationType::FIND) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  // LOG_INFO("here");
  page_set->clear();
  // LOG_INFO("End UnlatchAndUnpin");
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlatchAndUnpinAndDelete(Transaction* transaction, OperationType op) {
  if (transaction == nullptr) {
    return;
  }

  auto page_set = transaction->GetPageSet();
  for (auto& page : *page_set) {
    if (op == OperationType::FIND) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  page_set->clear();

  auto del_page_set = transaction->GetDeletedPageSet();
  for (auto& page_id : * del_page_set) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  del_page_set->clear();
}


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
