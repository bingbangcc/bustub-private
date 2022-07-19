/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager* bpm, Page* page, int index) : buffer_pool_manager_(bpm), page_(page), index_(index){
    leaf_page_ = reinterpret_cast<LeafPage*>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    return leaf_page_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_page_->GetSize();
    // throw std::runtime_error("unimplemented"); 
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { 
    return leaf_page_->GetItem(index_);
    // throw std::runtime_error("unimplemented"); 
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() { 
    index_++;
    if (index_ == leaf_page_->GetSize() && leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
        Page* next_leaf_page = buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId());
        index_ = 0;
        buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
        page_ = next_leaf_page;
        leaf_page_ = reinterpret_cast<LeafPage*>(next_leaf_page->GetData());
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
