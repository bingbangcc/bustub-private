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
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key, false);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  ValueType value;
  bool is_exist = leaf_node->Lookup(key, &value, comparator_);
  if (is_exist) {
    result->push_back(value);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
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
// 2.1 插入的时候如果导致一个节点溢出则进行split,split的时候会产生新节点并向父节点中插入一个key
//     如果导致父节点也溢出，则递归的进行split

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
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
  UpdateRootPageId(root_page_id_);

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
  Page *leaf_page = FindLeafPage(key, false);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // LOG_INFO("The target insert leaf page id is %d", leaf_node->GetPageId());

  // int key_index = leaf_node->KeyIndex(key, comparator_);
  // 如果当前key在leaf中已经存在，返回false
  ValueType leaf_value{};
  bool is_exist = leaf_node->Lookup(key, &leaf_value, comparator_);
  if (is_exist) {
    return false;
  }

  int current_size = leaf_node->Insert(key, value, comparator_);
  // 溢出，进行split
  if (current_size == leaf_max_size_) {
    LeafPage *new_leaf_node = Split<LeafPage>(leaf_node);
    InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node);
    buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
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

  // if (node->IsLeafPage()) {
  //   node->MoveHalfTo(new_node);
  //   new_node->SetNextPageId(node->GetNextPageId());
  //   node->SetNextPageId(new_page_id);
  // }
  // else {
  //   node->MoveHalfTo(new_node, buffer_pool_manager_);
  // }

  // buffer_pool_manager_->UnpinPage(new_page_id);
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
    page_id_t new_root_page_id = INVALID_PAGE_ID;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (new_root_page == nullptr) {
      // throw std::runtime_error("out of memory");
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
  buffer_pool_manager_->UnpinPage(parent_page_id, true);

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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

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
  return false;
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
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  return false;
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
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
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
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

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
