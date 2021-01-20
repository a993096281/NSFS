/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-05 19:49:05
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#include "inode_zone.h"
#include "metadb/debug.h"

namespace metadb {

InodeZone::InodeZone(const Option &option) : option_(option) {
    write_file_ = nullptr;
    hashtable_ = new InodeHashTable(option, this);
}

void InodeZone::InitInodeZone(const Option &option){
    write_file_ = nullptr;
    option_ = option;
    hashtable_ = new InodeHashTable(option, this);
}

InodeZone::~InodeZone(){
    delete hashtable_;
    for(auto it : files_locks_){
        delete it.second;
    }
}

void InodeZone::FilesMapInsert(NVMInodeFile *file){
    files_mu_.Lock();
    files_.insert(make_pair(GetFileId(FILE_GET_OFFSET(file)), file));
    files_mu_.Unlock();

    Mutex *new_lock = new Mutex();
    locks_mu_.Lock();
    files_locks_.insert(make_pair(GetFileId(FILE_GET_OFFSET(file)), new_lock));
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
    NVMInodeFile *file = nullptr;
    Mutex *lock = nullptr;
    files_mu_.Lock();
    auto it = files_.find(id);
    if(it != files_.end()){
        file = it->second;
        files_.erase(it);
    }
    files_mu_.Unlock();

    locks_mu_.Lock();
    auto it_lock = files_locks_.find(id);
    if(it_lock != files_locks_.end()){
        files_locks_.erase(it_lock);
    }
    locks_mu_.Unlock();
    if(file != nullptr) file_allocator->Free(file, INODE_FILE_SIZE);
    if(lock != nullptr) delete lock;
}

pointer_t InodeZone::WriteFile(const inode_id_t key, const Slice &value){
    write_mu_.Lock();
    if(write_file_ == nullptr){
        write_file_ = AllocNVMInodeFlie();
        FilesMapInsert(write_file_);
    }
    pointer_t key_addr = 0;
    if(write_file_->InsertKV(key, value, key_addr) != 0){
        write_file_ = AllocNVMInodeFlie();
        FilesMapInsert(write_file_);
        write_file_->InsertKV(key, value, key_addr);
    }
    write_mu_.Unlock();
    return key_addr;
}

int InodeZone::InodePut(const inode_id_t key, const Slice &value){
    pointer_t key_offset = WriteFile(key, value);
    pointer_t old_value = INVALID_POINTER;
    int res = hashtable_->Put(key, key_offset, old_value);
    if(res == 2){  //key以存在
        DeleteFlie(old_value);
    }
    return res;
}

int InodeZone::ReadFile(pointer_t addr, std::string &value){
    uint64_t id = GetFileId(addr);
    uint64_t offset = GetFileOffset(addr);
    NVMInodeFile *file = FilesMapGet(id);
    if(file == nullptr) {
        ERROR_PRINT("not find file! addr:%lu id:%lu offset:%lu\n", addr, id, offset);
        return 2;
    }
    return file->GetKV(offset, value);
}

int InodeZone::InodeGet(const inode_id_t key, std::string &value){
    pointer_t addr;
    int res = hashtable_->Get(key, addr);
    if(res == 0){
        res = ReadFile(addr, value);
    }
    return res;
}

int InodeZone::DeleteFlie(pointer_t value_addr){
    uint64_t id = GetFileId(value_addr);
    uint64_t offset = GetFileOffset(value_addr);
    Mutex *file_lock;
    NVMInodeFile *file = FilesMapGetAndGetLock(id, &file_lock);
    if(file == nullptr){
         ERROR_PRINT("not find file! addr:%lu id:%lu offset:%lu\n", value_addr, id, offset);
        return 2;
    }
    file_lock->Lock();
    file->SetInvalidNumPersist(file->invalid_num + 1);
    file_lock->Unlock();
    if(file != write_file_ && file->num == file->invalid_num){  //直接回收
        FilesMapDelete(id);
    }
    return 0;
}

int InodeZone::InodeDelete(const inode_id_t key){
    pointer_t value_addr;
    int res = hashtable_->Delete(key, value_addr);
    if(res == 0){
        res = DeleteFlie(value_addr);
    }
    return res;
}

int InodeZone::InodeUpdate(const inode_id_t key, const Slice &new_value){
    pointer_t key_offset = WriteFile(key, new_value);
    pointer_t old_value = INVALID_POINTER;
    int res = hashtable_->Update(key, key_offset, old_value);
    if(res == 2){  //key以存在
        DeleteFlie(old_value);
    }
    return res;
}


} // namespace name