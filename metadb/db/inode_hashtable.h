/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-05 14:35:22
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_INODE_HASHTABLE_H_
#define _METADB_INODE_HASHTABLE_H_

#include <string>
#include <atomic>
#include <vector>


#include "metadb/option.h"
#include "metadb/slice.h"
#include "metadb/inode.h"
#include "metadb/iterator.h"
#include "format.h"
#include "util/rwlock.h"
#include "util/lock.h"
#include "nvm_node_allocator.h"

using namespace std;

namespace metadb {

struct InodeKeyPointer{
    inode_id_t key;
    pointer_t pointer;

    InodeKeyPointer() : key(0), pointer(INVALID_POINTER) {}
    InodeKeyPointer(inode_id_t key1, pointer_t pointer1) : key(key1), pointer(pointer1) {}
    ~InodeKeyPointer() {}
};

static const uint32_t INODE_HASH_ENTRY_NODE_CAPACITY = (INODE_HASH_ENTRY_SIZE - 32) / sizeof(InodeKeyPointer);

struct NvmInodeHashEntryNode {       //无序存储key
    uint16_t num;
    uint16_t slot;   //从低位开始，1标志存在key
    uint32_t padding;  //暂时没用
    uint64_t padding2;  //暂时没用
    pointer_t prev;    //
    pointer_t next;
    InodeKeyPointer entry[INODE_HASH_ENTRY_NODE_CAPACITY];

    NvmInodeHashEntryNode() {}
    ~NvmInodeHashEntryNode() {}
    uint32_t GetFreeSpace() {
        return INODE_HASH_ENTRY_NODE_CAPACITY - num;
    }
    void Flush(){
        node_allocator->nvm_persist(this, sizeof(NvmInodeHashEntryNode));
    }
    void SetNumAndSlotPersist(uint16_t num_temp, uint16_t slot_temp){
        char buff[4];
        memcpy(buff, &num_temp, 2);
        memcpy(buff + 2, &slot_temp, 2);
        node_allocator->nvm_memmove_persist(&num, buff, 4);
    }
    void SetNumAndSlotNodrain(uint16_t num_temp, uint16_t slot_temp){
        char buff[4];
        memcpy(buff, &num_temp, 2);
        memcpy(buff + 2, &slot_temp, 2);
        node_allocator->nvm_memmove_nodrain(&num, buff, 4);
    }
    void SetPrevPersist(pointer_t ptr){
        node_allocator->nvm_memcpy_persist(&prev, &ptr, sizeof(pointer_t));
    }
    void SetPrevNodrain(pointer_t ptr){
        node_allocator->nvm_memcpy_nodrain(&prev, &ptr, sizeof(pointer_t));
    }

    void SetNextPersist(pointer_t ptr){
        node_allocator->nvm_memcpy_persist(&next, &ptr, sizeof(pointer_t));
    }
    void SetNextNodrain(pointer_t ptr){
        node_allocator->nvm_memcpy_nodrain(&next, &ptr, sizeof(pointer_t));
    }
    void SetEntryPersistByOffset(uint32_t offset, const void *ptr, uint32_t len){
        void *buf = static_cast<char *>(entry) + offset;
        node_allocator->nvm_memmove_persist(buf, ptr, len);
    }
    void SetEntryNodrainByOffset(uint32_t offset, const void *ptr, uint32_t len){
        void *buf = static_cast<char *>(entry) + offset;
        node_allocator->nvm_memmove_nodrain(buf, ptr, len);
    }
    void SetEntryPersistByIndex(uint32_t index, uint64_t key, pointer_t ptr){
        InodeKeyPointer temp(key, ptr);
        node_allocator->nvm_memcpy_persist(&(entry[index]), &temp, sizeof(InodeKeyPointer));
    }
    void SetEntryNodrainByIndex(uint32_t index, uint64_t key, pointer_t ptr){
        InodeKeyPointer temp(key, ptr);
        node_allocator->nvm_memcpy_nodrain(&(entry[index]), &temp, sizeof(InodeKeyPointer));
    }
    void SetEntryPointerPersist(uint32_t index, pointer_t ptr){
        void *buf = static_cast<char *>(&(entry[index])) + sizeof(8);
        node_allocator->nvm_memmove_persist(buf, &ptr, sizeof(pointer_t));
    }
};

struct NvmInodeHashEntry {
    pointer_t root;    //链地址，指向NvmInodeHashEntryNode

    NvmInodeHashEntry() : root(INVALID_POINTER) {}
    ~NvmInodeHashEntry() {}
    void SetRootPersit(pointer_t ptr){
        node_allocator->nvm_memcpy_persist(&root, &ptr, sizeof(pointer_t));
    }
};

struct InodeHashVersion {
public:
    NvmInodeHashEntry *buckets_;  //连续数组
    RWLock *rwlock_;  //连续读写锁
    uint64_t capacity_;
    atomic<uint64_t> node_num_;   //LinkNode num
    atomic<uint32_t> refs_;

    InodeHashVersion(uint64_t capacity) {
        capacity_ = capacity;
        rwlock_ = new RWLock[capacity];
        buckets_ = node_allocator->AllocateAndInit(sizeof(NvmInodeHashEntry) * capacity, 0);
        node_num_.store(capacity);
        refs_.store(0);
    }

    virtual ~InodeHashVersion() {
        delete[] rwlock_;
    }

    void FreeNvmSpace(){
        node_allocator->Free(buckets_, sizeof(NvmInodeHashEntry) * capacity_);
    }

    Ref() {
        refs_.fetch_add(1);
    }

    Unref() {
        refs_.fetch_sub(1);
        
        if(refs_.load() == 0){
            delete this;
        }
    }
};

class InodeHashTable {
public:
    InodeHashTable(const Option &option);
    virtual ~InodeHashTable();

    virtual int Put(const inode_id_t key, const pointer_t value);
    virtual int Get(const inode_id_t key, const pointer_t &value);
    virtual int Update(const inode_id_t key, const pointer_t value);
    virtual int Delete(const inode_id_t key);

private:
    const Option option_;
    
    Mutex version_lock_;
    bool is_rehash_;
    InodeHashVersion *version_;
    InodeHashVersion *rehash_version_; //rehash时，这个版本是正在rehash版本；


    void GetVersionAndRefByWrite(bool &is_rehash, InodeHashVersion **version);
    void GetVersionAndRefByRead(bool &is_rehash, InodeHashVersion **version, InodeHashVersion **rehash_version);
    uint32_t hash_id(const inode_id_t key);
    void HashEntryDealWithOp(InodeHashVersion *version, uint32_t index, InodeHashEntryLinkOp &op);
    int HashEntryInsetKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t value);
    int HashEntryUpdateKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t value);
    int HashEntryGetKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t &value);
    int HashEntryDeleteKV(InodeHashVersion *version, uint32_t index, const inode_id_t key);
    bool NeedRehash(InodeHashVersion *version);

};

struct InodeHashEntryLinkOp{
    pointer_t root;
    pointer_t res;  //返回的根节点
    vector<pointer_t> add_list;  //增加的节点
    vector<pointer_t> del_list;  //删除的节点

    InodeHashEntryLinkOp() : root(INVALID_POINTER) , res(INVALID_POINTER) {
        add_list.reserve(2);
        del_list.reserve(2);
    }
};

NvmInodeHashEntryNode *AllocHashEntryNode();
int InodeHashEntryLinkInsert(InodeHashEntryLinkOp &op, const inode_id_t key, const pointer_t value);
int InodeHashEntryLinkUpdate(InodeHashEntryLinkOp &op, const inode_id_t key, const pointer_t value);
int InodeHashEntryLinkGet(pointer_t root, const inode_id_t key, const pointer_t &value);
int InodeHashEntryLinkDelete(InodeHashEntryLinkOp &op, const inode_id_t key);

} // namespace name








#endif