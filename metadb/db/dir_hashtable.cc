/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-18 15:31:02
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "dir_hashtable.h"
#include "metadb/debug.h"

namespace metadb {

DirHashTable::DirHashTable(const Option &option, uint32_t hash_type) : option_(option) {
    hash_type_ = hash_type;
    is_rehash_ = false;
    version_ = nullptr;
    rehash_version_ = nullptr;

    //init
    version_ = new HashVersion(option_->FIRST_HASH_CAPACITY);
    version_->Ref();
}

DirHashTable::~DirHashTable(){
    if(version_) version_->Unref();
    if(rehash_version_) rehash_version_->Unref();
}

void DirHashTable::GetVersionAndRefByWrite(bool &is_rehash, HashVersion **version){
    MutexLock lock(&version_lock_);
    if(is_rehash_){
        is_rehash = true;
        *version = rehash_version_;
        rehash_version_->Ref();
    }
    else {
        is_rehash = false;
        *version = version_;
        version_->Ref();
    }
}

void DirHashTable::GetVersionAndRefByRead(bool &is_rehash, HashVersion **version, HashVersion **rehash_version){
    MutexLock lock(&version_lock_);
    if(is_rehash_){
        is_rehash = true;
        *version = version_;
        version_->Ref();
        *rehash_version = rehash_version_;
        rehash_version_->Ref();
    }
    else {
        is_rehash = false;
        *version = version_;
        version_->Ref();
    }
}

uint64_t DirHashTable::hash_id(const inode_id_t key, const uint64_t capacity){
    switch (hash_type_) {
        case 1:
            return (key % capacity);
            break;
        case 2:
            return (key / first_hash_capacity_) % capacity;   //二级hash先对齐一级hash，再hash
            break;
        default:
            ERROR_PRINT("hashtable type error:%d\n", hash_type_);
            break;
    }
    return 0;
}

void DirHashTable::HashEntryDealWithOp(NvmHashEntry *entry, LinkListOp &op){
    if(op.res != entry->root) {
        entry->SetRootPersist(op.res);
    }

    if(op.add_node_num != op.delete_node_num) {
        uint32_t new_node_num = 0;
        if((entry->node_num + op.add_node_num) < op.delete_node_num) {
            new_node_num = 0;
        }
        else {
            new_node_num = entry->node_num + op.add_node_num - op.delete_node_num;
        }
        entry->SetNodeNumPersist(new_node_num);
    }

    if(!op.free_list.empty()) {
        for(auto it : op.free_list) {
            node_allocator->Free(it, DIR_LINK_NODE_SIZE);
        }
    }
}

int DirHashTable::HashEntryInsetKV(NvmHashEntry *entry, const inode_id_t key, const Slice &fname, inode_id_t &value){
    pointer_t root = entry->root;
    int res = -1;
    if(IS_INVALID_POINTER(root)) { 
        LinkNode *root = AllocLinkNode();
        root->SetMaxkeyNodrain(MAX_INODE_ID_KEY);
        LinkListOp op;
        op.root = NODE_GET_OFFSET(root);
        res = LinkListInsert(op, key, fname, value);
        HashEntryDealWithOp(entry, op);
        return res;
    }
    else if(IS_SECOND_HASH_POINTER(root)) {  //二级hash

    }
    else {  //一级hash
        
    }
    return res;
}


int DirHashTable::Put(const inode_id_t key, const Slice &fname, const inode_id_t value){
    bool is_rehash = false;
    HashVersion *version;
    GetVersionAndRefByWrite(is_rehash, &version);
    uint64_t index = hash_id(key, version->capacity_);
    version->rwlock_[index].WriteLock();
    int res = HashEntryInsetKV(&version->buckets_[index], key, fname, value);

    version->rwlock_[index].Unlock();
    version->Unref();
    return res;
}

int DirHashTable::Get(const inode_id_t key, const Slice &fname, inode_id_t &value){

}

Iterator* DirHashTable::DirHashTableGetIterator(const inode_id_t target){
        
}


} // namespace name