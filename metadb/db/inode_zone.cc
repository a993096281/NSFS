/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-05 19:49:05
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#include "inode_zone.h"

namespace metadb {

InodeZone::InodeZone(const Option &option) : option_(option) {
    write_file_ = nullptr;
    hashtable_ = new InodeHashTable(option);
}

InodeZone::~InodeZone(){
    delete hashtable_;
    for(auto it : files_locks_){
        delete it.second;
    }
}

void InodeZone::FilesMapInsert(NVMInodeFile *file){
    files_mu_.Lock();
    files_.insert(make_pair(GetId(file), file));
    files_mu_.Unlock();

    Mutex *new_lock = new Mutex();
    locks_mu_.Lock();
    files_locks_.insert(make_pair(GetId(file), new_lock));
    locks_mu_.Unlock();
}

NVMInodeFile *InodeZone::FilesMapGet(uint64_t id){
    NVMInodeFile *file = nullptr;
    files_mu_.Lock();
    auto it = files_.find(id);
    if(it != files_.end()){
        file = it->second;
    }
    files_mu_.Unlock();
    return file;
}

NVMInodeFile *InodeZone::FilesMapGetAndGetLock(uint64_t id, Mutex **lock){
    NVMInodeFile *file = nullptr;
    files_mu_.Lock();
    auto it = files_.find(id);
    if(it != files_.end()){
        file = it->second;
    }
    files_mu_.Unlock();

    locks_mu_.Lock();
    auto it_lock = files_locks_.find(id);
    if(it_lock != files_locks_.end()){
        *lock = it_lock->second;
    }
    locks_mu_.Unlock();
    return file;
}

void InodeZone::FilesMapDelete(uint64_t id){
    files_mu_.Lock();
    auto it = files_.find(id);
    if(it != files_.end()){
        files_.erase(it);
    }
    files_mu_.Unlock();

    locks_mu_.Lock();
    auto it_lock = files_locks_.find(id);
    if(it_lock != files_locks_.end()){
        files_locks_.erase(it_lock);
    }
    locks_mu_.Unlock();
}

pointer_t InodeZone::WriteFile(const inode_id_t key, const Slice &value){
    write_mu_.Lock();
    if(write_file_ == nullptr){
        write_file_ = AllocNVMInodeFlie();
        FilesMapInsert(write_file_);
    }
    uint64_t offset = 0;
    if(write_file_->InsertKV(key, value, offset) != 0){
        write_file_ = AllocNVMInodeFlie();
        FilesMapInsert(write_file_);
        write_file_->InsertKV(key, value, offset);
    }
    pointer_t key_addr = NODE_GET_OFFSET(write_file_) + offset;
    write_mu_.Unlock();
    return key_addr;
}

int InodeZone::InodePut(const inode_id_t key, const Slice &value){
    pointer_t key_offset = WriteFile(key, value);
    int res = hashtable_->Put(key, key_offset);
    return res;
}


} // namespace name