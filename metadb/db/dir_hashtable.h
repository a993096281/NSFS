/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-17 15:37:07
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_DIR_HASHTABLE_H_
#define _METADB_DIR_HASHTABLE_H_

#include <string>
#include <atomic>


#include "metadb/option.h"
#include "metadb/slice.h"
#include "metadb/inode.h"
#include "metadb/iterator.h"
#include "format.h"
#include "util/rwlock.h"
#include "util/lock.h"
#include "nvm_node_allocator.h"
#include "dir_nvm_node.h"

using namespace std;

namespace metadb {

struct NvmHashEntry {       //如果root执向二级hash地址，则后面8字节是内存类DirHashTable地址；
    pointer_t root;  //链根节点,只存偏移
    uint32_t node_num;   //LinkNode num
    uint32_t kv_num;   //暂时不用,凑字节

    NvmHashEntry() : root(0), node_num(0), kv_num(0) {}
    ~NvmHashEntry() {}

    SetRootPersist(pointer_t addr) {
        node_allocator->nvm_memcpy_persist(&root, &addr, sizeof(pointer_t));
    }

    SetNodeNumPersist(uint32_t num) {
        node_allocator->nvm_memcpy_persist(&node_num, &num, sizeof(uint32_t));
    }
    SetNodeNumNodrain(uint32_t num) {
        node_allocator->nvm_memcpy_nodrain(&node_num, &num, sizeof(uint32_t));
    }
    
};                    

struct HashVersion {
public:
    NvmHashEntry *buckets_;  //连续数组
    RWLock *rwlock_;  //连续读写锁
    uint64_t capacity_;
    atomic<uint64_t> node_num_;   //LinkNode num
    atomic<uint32_t> refs_;

    HashVersion(uint64_t capacity) {
        capacity_ = capacity;
        rwlock_ = new RWLock[capacity];
        buckets_ = node_allocator->AllocateAndInit(sizeof(NvmHashEntry) * capacity, 0);
        node_num_.store(0);
        refs_.store(0);
    }

    virtual ~HashVersion() {
        delete[] rwlock_;
    }

    void FreeNvmSpace(){
        node_allocator->Free(buckets_, sizeof(NvmHashEntry) * capacity_);
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

class DirHashTable {
public:
    DirHashTable(const Option &option, uint32_t hash_type);
    virtual ~DirHashTable();

    virtual int Put(const inode_id_t key, const Slice &fname, const inode_id_t value);
    virtual int Get(const inode_id_t key, const Slice &fname, inode_id_t &value);
    virtual int Delete(const inode_id_t key, const Slice &fname);
    virtual Iterator* DirHashTableGetIterator(const inode_id_t target);

    virtual void SetFirstHashCapacity(uint64_t size) { first_hash_capacity_ = size; }
    virtual uint64_t GetFirstHashCapacity() { return first_hash_capacity_; }
private:
    const Option option_;
    uint32_t hash_type_;  //1是一级hash，2是二级hash；
    
    Mutex version_lock_;
    bool is_rehash_;
    HashVersion *version_;
    HashVersion *rehash_version_; //rehash时，这个版本是正在rehash版本；

    uint64_t first_hash_capacity_;  //二级hash才会用到


    bool IsSecondHashEntry(NvmHashEntry *entry);
    void GetVersionAndRefByWrite(bool &is_rehash, HashVersion **version);
    void GetVersionAndRefByRead(bool &is_rehash, HashVersion **version, HashVersion **rehash_version);
    uint32_t hash_id(const inode_id_t key, const uint64_t capacity);
    int HashEntryInsertKV(HashVersion *version, uint32_t index, const inode_id_t key, const Slice &fname, inode_id_t &value);
    void HashEntryDealWithOp(HashVersion *version, uint32_t index, LinkListOp &op);
    int HashEntryGetKV(HashVersion *version, uint32_t index, const inode_id_t key, const Slice &fname, inode_id_t &value);
    int HashEntryDeleteKV(HashVersion *version, uint32_t index, const inode_id_t key, const Slice &fname);
};



} // namespace name








#endif