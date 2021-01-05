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

bool IsSecondHashEntry(NvmHashEntry *entry){
    pointer_t root = entry->root;
    return IS_SECOND_HASH_POINTER(root);
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

uint32_t DirHashTable::hash_id(const inode_id_t key, const uint64_t capacity){
    switch (hash_type_) {
        case 1:
            return (key % capacity);
            break;
        case 2:
            return (key / first_hash_capacity_) % capacity;   //二级hash先对齐一级hash，再hash，最好first_hash_capacity_为定值
            break;
        default:
            ERROR_PRINT("hashtable type error:%d\n", hash_type_);
            break;
    }
    return 0;
}

void DirHashTable::HashEntryDealWithOp(HashVersion *version, uint32_t index, LinkListOp &op){
    NvmHashEntry *entry = &(version->buckets_[index]);
    if(op.res != entry->root) {
        entry->SetRootPersist(op.res);
    }
    uint32_t add_linknode_num = op.add_linknode_list.size();
    uint32_t free_linknode_num = op.free_linknode_list.size();
    
    if(add_linknode_num > free_linknode_num){
        entry->SetNodeNumPersist(entry->node_num + add_linknode_num - free_linknode_num);
        version->node_num_.fetch_add(add_linknode_num - free_linknode_num);
    }
    if(add_linknode_num < free_linknode_num) {
        entry->SetNodeNumPersist(entry->node_num + add_linknode_num - free_linknode_num);
        version->node_num_.fetch_sub(free_linknode_num - add_linknode_num);
    }

    if(!op.free_linknode_num.empty()) {  //后续最好原子记录一次操作的所有申请和删除的节点，作为空间管理日志
        for(auto it : op.free_linknode_list) {
            node_allocator->Free(it, DIR_LINK_NODE_SIZE);
        }
    }
    if(!op.free_indexnode_list.empty()){
        for(auto it : op.free_indexnode_list) {
            node_allocator->Free(it, DIR_BPTREE_INDEX_NODE_SIZE);
        }
    }
    if(!op.free_leafnode_list.empty()){
        for(auto it : op.free_leafnode_list) {
            node_allocator->Free(it, DIR_BPTREE_LEAF_NODE_SIZE);
        }
    }

    if(hash_type_ == 1){  //一级hash，暂时不会扩展，只会生成新的二级hash
        
    }

    if(hash_type_ == 2){  //有可能扩展

    }
}

int DirHashTable::HashEntryInsetKV(HashVersion *version, uint32_t index, const inode_id_t key, const Slice &fname, inode_id_t &value){
    NvmHashEntry *entry = &(version->buckets_[index]);
    pointer_t root = entry->root;
    int res = -1;
    if(IS_INVALID_POINTER(root)) { 
        LinkNode *root = AllocLinkNode();
        LinkListOp op;
        op.root = root;
        op.res = op.root;
        op.add_linknode_list.push_back(NODE_GET_OFFSET(root));
        res = LinkListInsert(op, key, fname, value);
        HashEntryDealWithOp(version, index, op);
        return res;
    } else if(IS_SECOND_HASH_POINTER(root)) {  //二级hash

    } else {  //一级hash
        LinkListOp op;
        op.root = root;
        op.res = op.root;
        res = LinkListInsert(op, key, fname, value);
        HashEntryDealWithOp(version, index, op);
        return res;
    }
    return res;
}


int DirHashTable::Put(const inode_id_t key, const Slice &fname, const inode_id_t value){
    bool is_rehash = false;
    HashVersion *version;
    GetVersionAndRefByWrite(is_rehash, &version);
    uint32_t index = hash_id(key, version->capacity_);
    bool is_second_hash = IsSecondHashEntry(&version->buckets_[index]);
    if(!is_second_hash) version->rwlock_[index].WriteLock();
    int res = HashEntryInsetKV(version, index, key, fname, value);

    if(!is_second_hash) version->rwlock_[index].Unlock();
    version->Unref();
    return res;
}

int DirHashTable::HashEntryGetKV(HashVersion *version, uint32_t index, const inode_id_t key, const Slice &fname, inode_id_t &value){
    NvmHashEntry *entry = &(version->buckets_[index]);
    pointer_t root = entry->root;
    int res = -1;
    if(IS_INVALID_POINTER(root)) { 
        return 2; //未找到
    } else if(IS_SECOND_HASH_POINTER(root)) {  //二级hash

    } else {  //一级hash
        LinkNode *root_node = static_cast<LinkNode *>(NODE_GET_POINTER(root));
        return LinkListGet(root_node, key, fname, value);
    }
    return 0;
}

int DirHashTable::Get(const inode_id_t key, const Slice &fname, inode_id_t &value){
    bool is_rehash = false;
    HashVersion *version;
    HashVersion *rehash_version;
    GetVersionAndRefByRead(is_rehash, &version, &rehash_version);
    if(is_rehash){  //正在rehash，先在rehash_version查找，再查找version
        uint32_t index = hash_id(key, rehash_version->capacity_);
        bool is_second_hash = IsSecondHashEntry(&version->buckets_[index]);
        if(!is_second_hash) rehash_version->rwlock_[index].ReadLock();  //可以不加读锁，因为都是MVCC控制，但是不加锁，什么时候删除垃圾节点时一个问题，延时删除可行，暂时简单处理，加读锁；
        int res = HashEntryGetKV(rehash_version, index, key, fname, value);
        if(!is_second_hash) rehash_version->rwlock_[index].Unlock();
        rehash_version->Unref();
        if(res == 0) return res;   //res == 0,意味着找到
    }
    uint32_t index = hash_id(key, version->capacity_);
    bool is_second_hash = IsSecondHashEntry(&version->buckets_[index]);
    if(!is_second_hash) version->rwlock_[index].ReadLock();  //可以不加读锁，因为都是MVCC控制，但是不加锁，什么时候删除垃圾节点时一个问题，延时删除可行，暂时简单处理，加读锁；
    int res = HashEntryGetKV(version, index, key, fname, value);
    if(!is_second_hash) version->rwlock_[index].Unlock();
    version->Unref();
    return res;   //
}

int HashEntryDeleteKV(HashVersion *version, uint32_t index, const inode_id_t key, const Slice &fname){
    NvmHashEntry *entry = &(version->buckets_[index]);
    pointer_t root = entry->root;
    int res = -1;
    if(IS_INVALID_POINTER(root)) { 
        return 2; //未找到
    } else if(IS_SECOND_HASH_POINTER(root)) {  //二级hash

    } else {  //一级hash
        LinkListOp op;
        op.root = root;
        op.res = op.root;
        res = LinkListDelete(op, key, fname);
        HashEntryDealWithOp(version, index, op);
        return 0;
    }
    return 0;
}

int Delete(const inode_id_t key, const Slice &fname){
    bool is_rehash = false;
    HashVersion *version;
    HashVersion *rehash_version;
    GetVersionAndRefByRead(is_rehash, &version, &rehash_version);  //只是为了获取两个版本

    uint32_t index = hash_id(key, version->capacity_);
    bool is_second_hash = IsSecondHashEntry(&version->buckets_[index]);
    if(!is_second_hash) version->rwlock_[index].WriteLock();  
    int res = HashEntryDeleteKV(version, index, key, fname);
    if(!is_second_hash) version->rwlock_[index].Unlock();
    version->Unref();

    if(is_rehash) { //正在rehash，先在version删除，再在rehash_version中删除
        uint32_t index = hash_id(key, rehash_version->capacity_);
        bool is_second_hash = IsSecondHashEntry(&version->buckets_[index]);
        if(!is_second_hash) rehash_version->rwlock_[index].WriteLock();  
        int res = HashEntryDeleteKV(rehash_version, index, key, fname);
        if(!is_second_hash) rehash_version->rwlock_[index].Unlock();
        rehash_version->Unref();

    }
    return 0;

}

Iterator* DirHashTable::DirHashTableGetIterator(const inode_id_t target){
        
}


} // namespace name