/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-12 14:59:28
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#ifndef _METADB_DIR_ITERATOR_H_
#define _METADB_DIR_ITERATOR_H_

#include <stdint.h>
#include <assert.h>

#include "metadb/inode.h"
#include "metadb/iterator.h"
#include "dir_nvm_node.h"
#include "nvm_node_allocator.h"

using namespace std;
namespace metadb {

class LinkNodeIterator : public Iterator {
public:
    LinkNodeIterator(pointer_t node, uint32_t key_offset, uint32_t key_num, uint32_t key_len);
    virtual ~LinkNodeIterator() {};

    virtual bool Valid() const = 0;
    //virtual void Seek(const Slice& target) = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual string fname() const = 0;
    virtual uint64_t hash_fname() const = 0;
    virtual inode_id_t value() const = 0;
private:
    LinkNode *cur_node_;
    uint32_t key_offset_;
    uint32_t key_num_;
    uint32_t key_len_;

    uint32_t cur_index_;
    uint32_t cur_offset_; 
};

LinkNodeIterator::LinkNodeIterator(pointer_t node, uint32_t key_offset, uint32_t key_num, uint32_t key_len) 
                : key_offset_(key_offset), key_num_(key_num), key_len_(key_len) {
    cur_node_ = nullptr;
    if(!IS_INVALID_POINTER(node)){
        cur_node_ = static_cast<LinkNode *>(NODE_GET_POINTER(node));
    }
    cur_index_ = 0;
    cur_offset_ = key_offset_;
}

bool LinkNodeIterator::Valid() const {
    return cur_index_ < key_num_;
}
void LinkNodeIterator::SeekToFirst() {
    cur_index_ = 0;
    cur_offset_ = key_offset_ + sizeof(inode_id_t) + 4 + 4;
}

void LinkNodeIterator::SeekToLast(){
    assert(0);
}

void LinkNodeIterator::Next(){
    cur_index_++;
    if(Valid()){
        uint64_t hash_fname;
        uint32_t value_len;
        cur_node_->DecodeBufGetHashfnameAndLen(cur_offset_, hash_fname, value_len);
        cur_offset_ += (8 + 4 + value_len);
    }
}

void LinkNodeIterator::Prev(){
    assert(0);
}

string LinkNodeIterator::fname() const {
    uint64_t hash_fname;
    uint32_t value_len;
    cur_node_->DecodeBufGetHashfnameAndLen(cur_offset_, hash_fname, value_len);
    return string(cur_node_->buf + cur_offset_ + 8, value_len - sizeof(inode_id_t));
}

uint64_t LinkNodeIterator::hash_fname() const {
    return *static_cast<const uint64_t *>(cur_node_->buf + cur_offset_);
}

inode_id_t LinkNodeIterator::value() const {
    uint64_t hash_fname;
    uint32_t value_len;
    cur_node_->DecodeBufGetHashfnameAndLen(cur_offset_, hash_fname, value_len);
    uint32_t value_offset = cur_offset_ + 8 + 4 + (value_len - sizeof(inode_id_t));
    inode_id_t value;
    cur_node_->DecodeBufGetKey(value_offset, value);
    return value;
}

class BptreeIterator : public Iterator {
public:
    BptreeIterator(BptreeLeafNode *head);
    virtual ~BptreeIterator() {};

    virtual bool Valid() const = 0;
    //virtual void Seek(const Slice& target) = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual string fname() const = 0;
    virtual uint64_t hash_fname() const = 0;
    virtual inode_id_t value() const = 0;
private:
    BptreeLeafNode *leaf_head_;
    BptreeLeafNode *cur_node_;
    uint32_t cur_index_;
    uint32_t cur_offset_; 
};

BptreeIterator::BptreeIterator(BptreeLeafNode *head) : leaf_head_(head) {
    cur_node_ = leaf_head_;
    cur_index_ = 0;
    cur_offset_ = 0;
}

bool BptreeIterator::Valid() const {
    return cur_node_ != nullptr && cur_index_ < cur_node_->num;
}
void BptreeIterator::SeekToFirst() {
    cur_node_ = leaf_head_;
    cur_index_ = 0;
    cur_offset_ = 0;
}

void BptreeIterator::SeekToLast(){
    assert(0);
}

void BptreeIterator::Next(){
    cur_index_++;
    if(!Valid()){
        if(!IS_INVALID_POINTER(cur_node_->next)) {
            cur_node_ = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(cur_node_->next));
            cur_index_ = 0;
            cur_offset_ = 0;
        } else {
            cur_node_ = nullptr;
            cur_index_ = 0;
            cur_offset_ = 0;
        }

    }
}

void BptreeIterator::Prev(){
    assert(0);
}

string BptreeIterator::fname() const {
    uint64_t hash_fname;
    uint32_t value_len;
    cur_node_->DecodeBufGetKeyValuelen(cur_offset_, hash_fname, value_len);
    return string(cur_node_->buf + cur_offset_ + 8, value_len - sizeof(inode_id_t));
}

uint64_t BptreeIterator::hash_fname() const {
    return *static_cast<const uint64_t *>(cur_node_->buf + cur_offset_);
}

inode_id_t BptreeIterator::value() const {
    uint64_t hash_fname;
    uint32_t value_len;
    cur_node_->DecodeBufGetKeyValuelen(cur_offset_, hash_fname, value_len);
    uint32_t value_offset = cur_offset_ + 8 + 4 + (value_len - sizeof(inode_id_t));
    return *static_cast<inode_id_t *>(cur_node_->buf + value_offset);
}

class EmptyIterator : public Iterator {
public:
    EmptyIterator() {};
    virtual ~EmptyIterator() {};

    virtual bool Valid() const { return false; }
    //virtual void Seek(const Slice& target) = 0;
    virtual void SeekToFirst() {};
    virtual void SeekToLast() {};
    virtual void Next() {};
    virtual void Prev() {};
    virtual string fname() const { return string(); };
    virtual uint64_t hash_fname() const { return 0; };
    virtual inode_id_t value() const { return 0; };
};

class IteratorWrapper {
public:
    IteratorWrapper(): iter_(NULL), valid_(false) { }
    explicit IteratorWrapper(Iterator* iter): iter_(NULL) {
        Set(iter);
    }
    ~IteratorWrapper() { delete iter_; }
    Iterator* iter() const { return iter_; }

    void Set(Iterator* iter) {
        delete iter_;
        iter_ = iter;
        if (iter_ == NULL) {
        valid_ = false;
        } else {
            Update();
        }
    }

    // Iterator interface methods
    bool Valid() const        { return valid_; }
    string fname() const { return iter_->fname(); };
    uint64_t hash_fname() const { return key_; };
    inode_id_t value() const { return iter_->value(); };
    void Next()               { iter_->Next();        Update(); }
    void Prev()               { iter_->Prev();        Update(); }

    void SeekToFirst()        { iter_->SeekToFirst(); Update(); }
    void SeekToLast()         { iter_->SeekToLast();  Update(); }

private:
    void Update() {
        valid_ = iter_->Valid();
        if (valid_) {
            key_ = iter_->hash_fname();
        }
    }

    Iterator* iter_;
    bool valid_;
    uint64_t key_;
};

class MergingIterator : public Iterator {
public:
    MergingIterator(Iterator** children, int n)
          : children_(new IteratorWrapper[n]),
            n_(n),
            current_(NULL),
            direction_(kForward) {
        for (int i = 0; i < n; i++) {
            children_[i].Set(children[i]);
        }
    }

    virtual ~MergingIterator() {
        delete[] children_;
    }

    virtual bool Valid() const {
        return (current_ != NULL);
    }

    virtual void SeekToFirst() {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToFirst();
        }
        FindSmallest();
        direction_ = kForward;
    }

    virtual void SeekToLast() {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }

    virtual void Next() {
        assert(Valid());

        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        // if (direction_ != kForward) {
        //     for (int i = 0; i < n_; i++) {
        //         IteratorWrapper* child = &children_[i];
        //         if (child != current_) {
        //             child->Seek(key());
        //             if (child->Valid() &&
        //                 comparator_->Compare(key(), child->key()) == 0) {
        //                 child->Next();
        //             }
        //         }
        //     }
        //     direction_ = kForward;
        // }

        current_->Next();
        FindSmallest();
    }

    virtual void Prev() {
        assert(Valid());

        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        // if (direction_ != kReverse) {
        //     for (int i = 0; i < n_; i++) {
        //         IteratorWrapper* child = &children_[i];
        //         if (child != current_) {
        //             child->Seek(key());
        //             if (child->Valid()) {
        //                 // Child is at first entry >= key().  Step back one to be < key()
        //                 child->Prev();
        //             } else {
        //                 // Child has no entries >= key().  Position at last entry.
        //                 child->SeekToLast();
        //             }
        //         }
        //     }
        //     direction_ = kReverse;
        // }

        current_->Prev();
        FindLargest();
    }
    virtual string fname() const { return current_->fname(); }
    virtual uint64_t hash_fname() const { return current_->hash_fname(); }
    virtual inode_id_t value() const { return current_->value(); }

private:
    void FindSmallest();
    void FindLargest();

    // We might want to use a heap in case there are lots of children.
    // For now we use a simple array since we expect a very small number
    // of children in leveldb.
    IteratorWrapper* children_;
    int n_;
    IteratorWrapper* current_;

    // Which direction is the iterator moving?
    enum Direction {
        kForward,
        kReverse
    };
    Direction direction_;
};

void MergingIterator::FindSmallest() {
    IteratorWrapper* smallest = NULL;
    for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child->Valid()) {
            if (smallest == NULL) {
                smallest = child;
            } else if (child->hash_fname() < smallest->hash_fname()) {
                smallest = child;
            } else if (child->hash_fname() == smallest->hash_fname()){
                child->Next();
            }
        }
    }
    current_ = smallest;
}

void MergingIterator::FindLargest() {
    IteratorWrapper* largest = NULL;
    for (int i = n_-1; i >= 0; i--) {
        IteratorWrapper* child = &children_[i];
        if (child->Valid()) {
            if (largest == NULL) {
                largest = child;
            } else if (child->hash_fname() > largest->hash_fname()) {
                largest = child;
            } else if (child->hash_fname() == largest->hash_fname()) {
                child->Next();
            }
        }
    }
    current_ = largest;
}

} // namespace name


#endif