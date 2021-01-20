/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-05 14:31:59
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_INODE_ZONE_H_
#define _METADB_INODE_ZONE_H_

#include <string>
#include <map>

#include "metadb/option.h"
#include "metadb/slice.h"
#include "metadb/inode.h"
#include "metadb/iterator.h"
#include "inode_hashtable.h"
#include "inode_file.h"
#include "../util/lock.h"
#include "nvm_file_allocator.h"

using namespace std;

namespace metadb {

class InodeZone {   //一级hash的分区，包含一个hashtable存储key-offset，包含多个文件存储value
public: 
    InodeZone(const Option &option);
    InodeZone() {}
    void InitInodeZone(const Option &option);
    virtual ~InodeZone();

    virtual int InodePut(const inode_id_t key, const Slice &value);
    virtual int InodeUpdate(const inode_id_t key, const Slice &new_value);
    virtual int InodeGet(const inode_id_t key, std::string &value);
    virtual int InodeDelete(const inode_id_t key);

    int DeleteFlie(pointer_t value_addr);   ////在文件中删除该地址，标记无效kv的个数
private: 
    Option option_;
    InodeHashTable *hashtable_;   //全部key-offset存储结构

    NVMInodeFile *write_file_;  //nvm正在写文件的结构，指针是nvm指针
    Mutex write_mu_;   //write_file_的锁，

    map<uint64_t, NVMInodeFile *> files_;  //所有文件的map，uint64_t是文件id，对应空间分配的id
    Mutex files_mu_;   //map->files_的锁
    map<uint64_t, Mutex *> files_locks_;   //对NVMInodeFile的invalid_num进行操作时需要原子操作，加一个锁,
    Mutex locks_mu_;   //files_locks_的锁

    void FilesMapInsert(NVMInodeFile *file);   //files_操作
    NVMInodeFile *FilesMapGet(uint64_t id);   //files_操作,  
    NVMInodeFile *FilesMapGetAndGetLock(uint64_t id, Mutex **lock);   //files_操作,  lock也返回文件的锁
    void FilesMapDelete(uint64_t id);         //files_操作

    pointer_t WriteFile(const inode_id_t key, const Slice &value);   //返回的是地址
    int ReadFile(uint64_t offset, std::string &value);

};



} // namespace name








#endif