/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-05 14:52:25
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#include "inode_hashtable.h"
#include "metadb/debug.h"

namespace metadb {

///// link 操作
struct InodeHashEntrySearchResult {
    bool key_find;
    uint16_t index;
    pointer_t node;

    InodeHashEntrySearchResult() : key_find(false), index(0), node(INVALID_POINTER) {}
    ~InodeHashEntrySearchResult() {}
};

NvmInodeHashEntryNode *AllocHashEntryNode(){
    NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(node_allocator->AllocateAndInit(INODE_HASH_ENTRY_SIZE, 0));
    return node;
}

//将slot的index位置位1
static inline uint16_t slot_set_index(uint16_t slot, uint16_t index){
    uint16_t temp = slot;
    temp = temp | (1 << index);
    return temp;
}

//获取slot的index位置的值
static inline bool slot_get_index(uint16_t slot, uint16_t index){
    uint16_t temp = slot;
    return (temp >> index) & 1;
}

void HashEntryLinkSearchKey(InodeHashEntrySearchResult &res, pointer_t root, const inode_id_t key){
    pointer_t cur = root;
    NvmInodeHashEntryNode *cur_node;
    while(!IS_INVALID_POINTER(cur)) {
        cur_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(cur));
        for(uint16_t i = 0; i < INODE_HASH_ENTRY_NODE_CAPACITY; i++){
            if(!slot_get_index(cur_node->slot, i)) continue;
            if(compare_inode_id(cur_node->entry[i].key, key) == 0){
                res.key_find = true;
                res.index = i;
                res.node = cur;
                return ;
            }
        }
        cur = cur_node->next;
    }
}

//找到一个空位，或者新创一个节点，生成空位，返回ret，index
void FindFreeSpaceOrCreatEntry(InodeHashEntryLinkOp &op, pointer_t &ret, uint16_t &index){
    //root不能为空
    pointer_t cur = op.root;
    NvmInodeHashEntryNode *cur_node;
    pointer_t prev = cur;
    while(!IS_INVALID_POINTER(cur)) {
        cur_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(cur));
        if(cur_node->GetFreeSpace() > 0){
            for(uint16_t i = 0; i < INODE_HASH_ENTRY_NODE_CAPACITY; i++){
                if(!slot_get_index(cur_node->slot, i)) {
                    ret = cur;
                    index = i;
                    return ;
                }
            }
        }
        prev = cur;
        cur = cur_node->next;
    }
    //新建节点
    NvmInodeHashEntryNode *new_node = AllocHashEntryNode();
    ret = NODE_GET_OFFSET(new_node);
    index = 0;
    new_node->SetPrevNodrain(prev);
    new_node->Flush();
    if(!IS_INVALID_POINTER(prev)){
        NvmInodeHashEntryNode *prev_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(prev));
        prev_node->SetNextPersist(NODE_GET_OFFSET(new_node));
    } else {  //这是新根节点
        op.res = NODE_GET_OFFSET(new_node);
    }
    op.add_list.push_back(NODE_GET_OFFSET(new_node));
}

int InodeHashEntryLinkInsert(InodeHashEntryLinkOp &op, const inode_id_t key, const pointer_t value, pointer_t &old_value){
    InodeHashEntrySearchResult res;
    HashEntryLinkSearchKey(res, op.root, key);
    if(res.key_find){  //已存在
        NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(res.node));
        old_value = node->entry[res.index].pointer;
        node->SetEntryPointerPersist(res.index, value);
        return 2;
    }
    pointer_t insert;
    pointer_t index;
    FindFreeSpaceOrCreatEntry(op, insert, index);
    NvmInodeHashEntryNode *insert_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(insert));
    insert_node->SetEntryPointerPersist(index, value);
    uint16_t slot = slot_set_index(insert_node->slot, index);
    insert_node->SetNumAndSlotPersist(insert_node->num + 1, slot);
    return 0;
}

int InodeHashEntryLinkUpdate(InodeHashEntryLinkOp &op, const inode_id_t key, const pointer_t new_value, pointer_t &old_value){
    InodeHashEntrySearchResult res;
    HashEntryLinkSearchKey(res, op.root, key);
    if(res.key_find){  //已存在
        NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(res.node));
        old_value = node->entry[res.index].pointer;
        node->SetEntryPointerPersist(res.index, new_value);
        return 0;
    }
    return 2; //不存在，无法修改
}

int InodeHashEntryLinkGet(pointer_t root, const inode_id_t key, const pointer_t &value){
    InodeHashEntrySearchResult res;
    HashEntryLinkSearchKey(res, root, key);
    if(res.key_find){  //已存在
        NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(res.node));
        value = node->entry[res.index].key;
        return 0;
    }
    return 2;  //key不存在
}

int InodeHashEntryLinkDelete(InodeHashEntryLinkOp &op, const inode_id_t key, const pointer_t &value){
    InodeHashEntrySearchResult res;
    HashEntryLinkSearchKey(res, op.root, key);
    if(res.key_find){  //已存在
        NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(res.node));
        value = node->entry[res.index].key;
        //合并暂不考虑
        if(node->num == 1){  //需要删除节点
            if(!IS_INVALID_POINTER(node->next)){
                NvmInodeHashEntryNode *next_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(node->next));
                next_node->SetPrevPersist(node->prev);
            }
            if(!IS_INVALID_POINTER(node->prev)){
                NvmInodeHashEntryNode *prev_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(node->prev));
                prev_node->SetNextPersist(node->next);
            } else {
                op.res = node->next;
            }
            op.del_list.push_back(res.node);
        } else {  //直接删除一条entry
            uint16_t slot = slot_set_index(node->slot, res.index);
            node->SetNumAndSlotPersist(node->num - 1, slot);
        }
        
        return 0;
    }
    return 2; //不存在，无法删除
}


////

InodeHashTable::InodeHashTable(const Option &option) : option_(option) {
    is_rehash_ = false;
    version_ = nullptr;
    rehash_version_ = nullptr;

    //init
    version_ = new InodeHashVersion(option_.INODE_HASHTABLE_INIT_SIZE);
    version_->Ref();
}

InodeHashTable::~InodeHashTable(){
    if(version_) version_->Unref();
    if(rehash_version_) rehash_version_->Unref();
}

uint32_t InodeHashTable::hash_id(const inode_id_t key, const uint64_t capacity){
    return (key / option_.INODE_MAX_ZONE_NUM) % capacity;
}

bool InodeHashTable::NeedRehash(InodeHashVersion *version){
    uint64_t node_num = version->node_num_.load();
    if(!rehash_version_ && node_num >= (version->capacity_ * option_.INODE_HASHTABLE_TRIG_REHASH_TIMES)){

    }
}

void InodeHashTable::HashEntryDealWithOp(InodeHashVersion *version, uint32_t index, InodeHashEntryLinkOp &op){
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    if(op.res != entry->root) {
        entry->SetRootPersist(op.res);
    }
    uint32_t add_num = op.add_list.size();
    uint32_t del_num = op.del_list.size();
    
    if(add_num > del_num){
        version->node_num_.fetch_add(add_num - del_num);
    }
    if(add_num < del_num) {
        version->node_num_.fetch_sub(del_num - add_num);
    }

    if(!op.del_list.empty()) {  //后续最好原子记录一次操作的所有申请和删除的节点，作为空间管理日志
        for(auto it : op.del_list) {
            node_allocator->Free(it, INODE_HASH_ENTRY_SIZE);
        }
    }

    if(NeedRehash(version)){
        //rehash
    }
    
}

void InodeHashTable::GetVersionAndRefByWrite(bool &is_rehash, InodeHashVersion **version){
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

void InodeHashTable::GetVersionAndRefByRead(bool &is_rehash, InodeHashVersion **version, InodeHashVersion **rehash_version){
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

int InodeHashTable::HashEntryInsertKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t value, pointer_t &old_value){
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    InodeHashEntryLinkOp op;
    op.root = entry->root;
    op.res = entry->root;
    int res = InodeHashEntryLinkInsert(op, key, value, old_value);
    HashEntryDealWithOp(version, index, op);
    return res;

}

int HashEntryUpdateKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t new_value, pointer_t &old_value){
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    InodeHashEntryLinkOp op;
    op.root = entry->root;
    op.res = entry->root;
    int res = InodeHashEntryLinkUpdate(op, key, new_value, old_value);
    if(res == 0) HashEntryDealWithOp(version, index, op);
    return res;
}

int HashEntryGetKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t &value){
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    return InodeHashEntryLinkGet(entry->root, key, value);
}

int HashEntryDeleteKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t &value){
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    InodeHashEntryLinkOp op;
    op.root = entry->root;
    op.res = entry->root;
    int res = InodeHashEntryLinkDelete(op, key, value);
    if(res == 0) HashEntryDealWithOp(version, index, op);
    return res;
}

int InodeHashTable::Put(const inode_id_t key, const pointer_t value){
    bool is_rehash = false;
    InodeHashVersion *version;
    GetVersionAndRefByWrite(is_rehash, &version);
    uint32_t index = hash_id(key, version->capacity_);
    version->rwlock_[index].WriteLock();
    int res = HashEntryInsertKV(version, index, key, value);
    version->rwlock_[index].Unlock();
    version->Unref();
    return res;
}

int InodeHashTable::Get(const inode_id_t key, const pointer_t &value){
    bool is_rehash = false;
    InodeHashVersion *version;
    InodeHashVersion *rehash_version;
    GetVersionAndRefByRead(is_rehash, &version, &rehash_version);
    if(is_rehash){  //正在rehash，先在rehash_version查找，再查找version
        uint32_t index = hash_id(key, rehash_version->capacity_);
        rehash_version->rwlock_[index].ReadLock();  //可以不加读锁，因为都是MVCC控制，但是不加锁，什么时候删除垃圾节点时一个问题，延时删除可行，暂时简单处理，加读锁；
        int res = HashEntryGetKV(rehash_version, index, key, value);
        rehash_version->rwlock_[index].Unlock();
        rehash_version->Unref();
        if(res == 0) return res;   //res == 0,意味着找到
    }
    uint32_t index = hash_id(key, version->capacity_);
    version->rwlock_[index].ReadLock();  //可以不加读锁，因为都是MVCC控制，但是不加锁，什么时候删除垃圾节点时一个问题，延时删除可行，暂时简单处理，加读锁；
    int res = HashEntryGetKV(version, index, key, value);
    version->rwlock_[index].Unlock();
    version->Unref();
    return res;   //
}

int InodeHashTable::Update(const inode_id_t key, const pointer_t new_value, pointer_t &old_value){
    bool is_rehash = false;
    InodeHashVersion *version;
    GetVersionAndRefByWrite(is_rehash, &version);
    uint32_t index = hash_id(key, version->capacity_);
    version->rwlock_[index].WriteLock();
    int res = HashEntryInsertKV(version, index, key, new_value, old_value);
    version->rwlock_[index].Unlock();
    version->Unref();
    return res;
}

int InodeHashTable::Delete(const inode_id_t key, const pointer_t &value){
    bool is_rehash = false;
    InodeHashVersion *version;
    InodeHashVersion *rehash_version;
    GetVersionAndRefByRead(is_rehash, &version, &rehash_version);  //只是为了获取两个版本

    uint32_t index = hash_id(key, version->capacity_);
    version->rwlock_[index].WriteLock();  
    int res1 = HashEntryDeleteKV(version, index, key, value);
    version->rwlock_[index].Unlock();
    version->Unref();
    int res2;
    if(is_rehash) { //正在rehash，先在version删除，再在rehash_version中删除
        uint32_t index = hash_id(key, rehash_version->capacity_);
        rehash_version->rwlock_[index].WriteLock();  
        res2 = HashEntryDeleteKV(rehash_version, index, key, value);
        rehash_version->rwlock_[index].Unlock();
        rehash_version->Unref();

    }
    return res1 & res2;  //有一个为0则为0
}

} // namespace name