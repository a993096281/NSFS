/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-18 15:31:02
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "dir_hashtable.h"
#include "metadb/debug.h"
#include "dir_iterator.h"

namespace metadb {

HashVersion::~HashVersion() {
    if(buckets_ != nullptr){   //正常退出
        for(uint32_t i = 0; i < capacity_; i++){
            if(IS_SECOND_HASH_POINTER(buckets_[i].root)) {
                DirHashTable *second_hash = static_cast<DirHashTable *>(buckets_[i].GetSecondHashAddr());
                delete second_hash;
            }
        }
    }
    delete[] rwlock_;
}

DirHashTable::DirHashTable(const Option &option, uint32_t hash_type, uint64_t capacity) : option_(option) {
    hash_type_ = hash_type;
    is_rehash_ = false;
    version_ = nullptr;
    rehash_version_ = nullptr;

    //init
    version_ = new HashVersion(capacity);
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

inline uint32_t DirHashTable::hash_id(const inode_id_t key, const uint64_t capacity){
    switch (hash_type_) {
        case 1:
            return (key % capacity);
            break;
        case 2:
            return (key / option_.DIR_FIRST_HASH_MAX_CAPACITY) % capacity;   //二级hash先对齐一级hash，再hash，最好first_hash_capacity_为定值
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

    if(!op.free_linknode_list.empty()) {  //后续最好原子记录一次操作的所有申请和删除的节点，作为空间管理日志
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
        if(NeedHashEntryToSecondHash(entry)){
            AddHashEntryTranToSecondHashJob(version, index);
        }
    }

    if(hash_type_ == 2){  //有可能扩展
        if(NeedSecondHashDoRehash()){
            thread_pool->Schedule(&DirHashTable::SecondHashDoRehashJob, this);
        }
    }
}

int DirHashTable::HashEntryInsertKV(HashVersion *version, uint32_t index, const inode_id_t key, const Slice &fname, const inode_id_t &value){
    NvmHashEntry *entry = &(version->buckets_[index]);
    pointer_t root = entry->root;
    int res = -1;

    if(IS_SECOND_HASH_POINTER(root)) {   //二级hash
        DirHashTable *second_hash = static_cast<DirHashTable *>(entry->GetSecondHashAddr());
        return second_hash->Put(key, fname, value);
    }
    version->rwlock_[index].WriteLock();

    if(IS_INVALID_POINTER(root)) { 
        LinkNode *root_node = AllocLinkNode();
        LinkListOp op;
        op.root = NODE_GET_OFFSET(root_node);
        op.res = NODE_GET_OFFSET(root_node);
        op.add_linknode_list.push_back(NODE_GET_OFFSET(root_node));
        res = LinkListInsert(op, key, fname, value);
        HashEntryDealWithOp(version, index, op);
        
    } else {  //Linklist
        
        LinkListOp op;
        op.root = root;
        op.res = op.root;
        res = LinkListInsert(op, key, fname, value);
        HashEntryDealWithOp(version, index, op);
    }
    version->rwlock_[index].Unlock();
    return res;
}


int DirHashTable::Put(const inode_id_t key, const Slice &fname, const inode_id_t value){
    bool is_rehash = false;
    HashVersion *version;
    GetVersionAndRefByWrite(is_rehash, &version);
    uint32_t index = hash_id(key, version->capacity_);
    
    int res = HashEntryInsertKV(version, index, key, fname, value);

    version->Unref();
    return res;
}

int DirHashTable::HashEntryGetKV(HashVersion *version, uint32_t index, const inode_id_t key, const Slice &fname, inode_id_t &value){
    NvmHashEntry *entry = &(version->buckets_[index]);
    pointer_t root = entry->root;
    int res = -1;
    if(IS_SECOND_HASH_POINTER(root)) {   //二级hash
        DirHashTable *second_hash = static_cast<DirHashTable *>(entry->GetSecondHashAddr());
        return second_hash->Get(key, fname, value);
    }
     //可以不加读锁，因为都是MVCC控制，但是不加锁，什么时候删除垃圾节点是一个问题，延时删除可行或引用计数，
    //暂时简单处理，由于空间分配是往后分配，提前删除没有影响；
    if(IS_INVALID_POINTER(root)) { 
        return 2; //未找到
    } else {  //linklist
        LinkNode *root_node = static_cast<LinkNode *>(NODE_GET_POINTER(root));
        return LinkListGet(root_node, key, fname, value);
    }
    return 0;
}

Iterator *DirHashTable::HashEntryGetIterator(HashVersion *version, uint32_t index, const inode_id_t target){
    NvmHashEntry *entry = &(version->buckets_[index]);
    pointer_t root = entry->root;
    int res = -1;
    if(IS_SECOND_HASH_POINTER(root)) {   //二级hash
        return nullptr;
    }
     //可以不加读锁，因为都是MVCC控制，但是不加锁，什么时候删除垃圾节点是一个问题，延时删除可行或引用计数，
    //暂时简单处理，由于空间分配是往后分配，提前删除没有影响；
    if(IS_INVALID_POINTER(root)) { 
        return nullptr; //未找到
    } else {  //linklist
        LinkNode *root_node = static_cast<LinkNode *>(NODE_GET_POINTER(root));
        return LinkListGetIterator(root_node, target);
    }
    return nullptr;
}

int DirHashTable::Get(const inode_id_t key, const Slice &fname, inode_id_t &value){
    bool is_rehash = false;
    HashVersion *version;
    HashVersion *rehash_version;
    GetVersionAndRefByRead(is_rehash, &version, &rehash_version);
    if(is_rehash){  //正在rehash，先在rehash_version查找，再查找version
        uint32_t index = hash_id(key, rehash_version->capacity_);
        
        int res = HashEntryGetKV(rehash_version, index, key, fname, value);
        
        rehash_version->Unref();
        if(res == 0) return res;   //res == 0,意味着找到
    }
    uint32_t index = hash_id(key, version->capacity_);
    
    int res = HashEntryGetKV(version, index, key, fname, value);
    
    version->Unref();
    return res;   //
}

int DirHashTable::HashEntryDeleteKV(HashVersion *version, uint32_t index, const inode_id_t key, const Slice &fname){
    NvmHashEntry *entry = &(version->buckets_[index]);
    pointer_t root = entry->root;
    int res = -1;
    if(IS_SECOND_HASH_POINTER(root)) {  //二级hash
        DirHashTable *second_hash = static_cast<DirHashTable *>(entry->GetSecondHashAddr());
        return second_hash->Delete(key, fname);
    }
    version->rwlock_[index].WriteLock();
    if(IS_INVALID_POINTER(root)) { 
        res = 2; //未找到
    } else {  //linklist
        LinkListOp op;
        op.root = root;
        op.res = op.root;
        res = LinkListDelete(op, key, fname);
        if(res == 0) HashEntryDealWithOp(version, index, op);
    }
    version->rwlock_[index].Unlock();
    return res;
}

int DirHashTable::Delete(const inode_id_t key, const Slice &fname){
    bool is_rehash = false;
    HashVersion *version;
    HashVersion *rehash_version;
    GetVersionAndRefByRead(is_rehash, &version, &rehash_version);  //只是为了获取两个版本

    uint32_t index = hash_id(key, version->capacity_);

    int res = HashEntryDeleteKV(version, index, key, fname);

    version->Unref();

    if(is_rehash) { //正在rehash，先在version删除，再在rehash_version中删除
        uint32_t index = hash_id(key, rehash_version->capacity_);

        int res = HashEntryDeleteKV(rehash_version, index, key, fname);

        rehash_version->Unref();

    }
    return 0;

}

inline bool DirHashTable::NeedHashEntryToSecondHash(NvmHashEntry *entry){
    if(entry->node_num >= option_.DIR_LINKNODE_TRAN_SECOND_HASH_NUM){
        return true;
    }
    return false;
}

inline bool DirHashTable::NeedSecondHashDoRehash(){
    uint64_t node_num = version_->node_num_.load();
    if(!is_rehash_ && node_num >= (version_->capacity_ * option_.DIR_SECOND_HASH_TRIG_REHASH_TIMES)){
        return true;
    }
    return false;
}

struct TranToSecondHashJob{
    DirHashTable *hashtable;
    HashVersion *version;
    uint32_t index;

    TranToSecondHashJob(DirHashTable *a, HashVersion *b, uint32_t c) : hashtable(a), version(b), index(c) {}
    ~TranToSecondHashJob() {}
};

void DirHashTable::AddHashEntryTranToSecondHashJob(HashVersion *version, uint32_t index){
    TranToSecondHashJob *job = new TranToSecondHashJob(this, version, index);
    thread_pool->Schedule(&DirHashTable::HashEntryTranToSecondHashWork, job);   //添加后台任务
}

void DirHashTable::HashEntryTranToSecondHashWork(void *arg){
    TranToSecondHashJob *job = static_cast<TranToSecondHashJob *>(arg);
    NvmHashEntry *entry = &(job->version->buckets_[job->index]);
    job->version->rwlock_[job->index].WriteLock();
    if(IS_SECOND_HASH_POINTER(entry->root) || entry->node_num < job->hashtable->option_.DIR_LINKNODE_TRAN_SECOND_HASH_NUM) {   //可能被转了
        job->version->rwlock_[job->index].Unlock();
        delete job;
        return ;
    }
    uint64_t second_hash_capacity = job->hashtable->option_.DIR_SECOND_HASH_INIT_SIZE;
    DirHashTable *second_hash = new DirHashTable(job->hashtable->option_, 2, second_hash_capacity);
    
    vector<pointer_t> free_list;  //要删除的节点
    vector<vector<string>> buckets(second_hash_capacity, vector<string>());  //内存保存所有bukets的kvs，每个string就是一个kv，然后再转为NVM节点

    //后期可直接写入NVMNode节点，暂时不那样处理，麻烦
    //将NVM linknode KVs hash复制到内存buckets中；
    pointer_t cur = entry->root;
    LinkNode *cur_node;
    while(!IS_INVALID_POINTER(cur)) {
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
        
        inode_id_t temp_key;
        uint32_t kv_len;
        uint32_t key_num, key_len;
        uint32_t offset = 0;
        for(uint32_t i = 0; i < cur_node->num; i++){
            cur_node->DecodeBufGetKeyNumLen(offset, temp_key, key_num, key_len);
            if(key_num == 0){  //kv是bptree
                kv_len = sizeof(inode_id_t) + 4 + 8;
            } else {
                kv_len = sizeof(inode_id_t) + 4 + 4 + key_len;
            }
            uint32_t hash_index =  second_hash->hash_id(temp_key, second_hash_capacity);
            buckets[hash_index].push_back(string(cur_node->buf + offset, kv_len));  //由于原来有序，直接push_back也有序
            offset += kv_len;
        }
        free_list.push_back(cur);
        cur = cur_node->next;
    }

    
    //将内存buckets中的所有kv string填到NVM linknode中，然后更新version的entry
    pointer_t root = INVALID_POINTER;
    uint32_t nodes_num = 0;
    HashVersion *version = second_hash->version_;
    for(uint32_t i = 0; i < second_hash_capacity; i++){
        if(!buckets[i].empty()){
            MemoryTranToNVMLinkNode(buckets[i], root, nodes_num);
            version->buckets_[i].SetRootPersist(root);
            version->buckets_[i].SetNodeNumPersist(nodes_num);
        }
    }
    pointer_t old_entry_root = SECOND_HASH_POINTER | NODE_GET_OFFSET(version->buckets_);
    entry->SetRootPersist(old_entry_root);
    entry->SetSecondHashPersist(second_hash);
    job->version->rwlock_[job->index].Unlock();

    for(auto it : free_list){
        node_allocator->Free(it, DIR_LINK_NODE_SIZE);
    }
    delete job;
}

void DirHashTable::SecondHashDoRehashJob(void *arg){
    reinterpret_cast<DirHashTable *>(arg)->SecondHashDoRehashWork();
}

void DirHashTable::SecondHashDoRehashWork(){
    version_lock_.Lock();
    if(!NeedSecondHashDoRehash()){
        version_lock_.Unlock();
        return ;
    }
    rehash_version_ = new HashVersion(version_->capacity_ * 2);
    version_->Ref();
    rehash_version_->Ref();
    is_rehash_ = true;
    version_lock_.Unlock();

    //开始逐步迁移数据到rehash_version_中
    for(uint32_t index = 0; index < version_->capacity_; index++){
        MoveEntryToRehash(version_, index, rehash_version_);
    }
    //迁移完
    version_lock_.Lock();
    HashVersion *old_version = version_;
    version_ = rehash_version_;
    rehash_version_ = nullptr;
    is_rehash_ = false;
    version_lock_.Unlock();

    old_version->Unref();   //解除本函数的调用
    old_version->FreeNvmSpace();
    old_version->Unref();   //删除旧version
}

//rehash时迁移kvs；
int DirHashTable::RehashInsertKvs(HashVersion *version, uint32_t index, const inode_id_t key, string &kvs){
    NvmHashEntry *entry = &(version->buckets_[index]);
    pointer_t root = entry->root;
    int res = -1;
    version->rwlock_[index].WriteLock();

    if(IS_INVALID_POINTER(root)) { 
        LinkNode *root_node = AllocLinkNode();
        LinkListOp op;
        op.root = NODE_GET_OFFSET(root_node);
        op.res = NODE_GET_OFFSET(root_node);
        op.add_linknode_list.push_back(NODE_GET_OFFSET(root_node));
        res = RehashLinkListInsert(op, key, kvs);
        HashEntryDealWithOp(version, index, op);
        
    } else {  //一级hash
        LinkListOp op;
        op.root = root;
        op.res = op.root;
        res = RehashLinkListInsert(op, key, kvs);
        HashEntryDealWithOp(version, index, op);
    }
    version->rwlock_[index].Unlock();
    return res;
}

void DirHashTable::MoveEntryToRehash(HashVersion *version, uint32_t index, HashVersion *rehash_version){
    version->rwlock_[index].WriteLock();   //
    NvmHashEntry *entry = &(version->buckets_[index]);
    vector<pointer_t> free_list;
    pointer_t cur = entry->root;
    LinkNode *cur_node;
    inode_id_t key;
    uint32_t key_num, key_len, kv_len;
    uint32_t key_index;
    pointer_t value;
    string kvs;
    int res = 0;
    while(!IS_INVALID_POINTER(cur)) {
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
        uint32_t offset = 0;
        for(uint16_t i = 0; i < cur_node->num; i++){
            cur_node->DecodeBufGetKeyNumLen(offset, key, key_num, key_len);
            if(key_num == 0){  //kv是bptree
                kv_len = sizeof(inode_id_t) + 4 + 8;
            } else {
                kv_len = sizeof(inode_id_t) + 4 + 4 + key_len;
            }
            kvs.assign(cur_node->buf + offset, kv_len);
            key_index = hash_id(key, rehash_version->capacity_);
            res = RehashInsertKvs(rehash_version, key_index, key, kvs);  //这个插入超麻烦
        }
        free_list.push_back(cur);
        cur = cur_node->next;
    }
    entry->SetRootPersist(INVALID_POINTER);  //旧version的entry变为空
    version->rwlock_[index].Unlock();

    for(auto it : free_list) {
        node_allocator->Free(it, INODE_HASH_ENTRY_SIZE);
    }
}


Iterator* DirHashTable::DirHashTableGetIterator(const inode_id_t target){
        bool is_rehash = false;
        HashVersion *version;
        HashVersion *rehash_version;
        GetVersionAndRefByRead(is_rehash, &version, &rehash_version);
        Iterator* rehash_it = nullptr;
        Iterator* vesion_it = nullptr;
        if(is_rehash){  //正在rehash，先在rehash_version查找，再查找version
            uint32_t index = hash_id(target, rehash_version->capacity_);
            
            rehash_it = HashEntryGetIterator(rehash_version, index, target);
            
            rehash_version->Unref();
        }
        uint32_t index = hash_id(target, version->capacity_);
        
        vesion_it = HashEntryGetIterator(version, index, target);
        
        version->Unref();
        if(rehash_it != nullptr && vesion_it != nullptr){  //两版本都不为空，合并iterator
            Iterator *two_it[2];
            two_it[0] = rehash_it;
            two_it[1] = vesion_it;
            return new MergingIterator(two_it, 2);
        }
        return (rehash_version != nullptr) ? rehash_it : vesion_it;   //
}


} // namespace name