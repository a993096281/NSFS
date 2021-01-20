/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-04 15:18:23
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include <assert.h>
#include "nvm_file_allocator.h"
#include "metadb/debug.h"

using namespace std;

namespace metadb {

char *file_pool_pointer = nullptr;
NVMFileAllocator *file_allocator = nullptr;

int InitNVMFileAllocator(const std::string path, uint64_t size){
    file_allocator = new NVMFileAllocator(path, size);
    file_pool_pointer = file_allocator->GetPmemAddr();
    return 0;
}

static inline uint64_t GetId(pointer_t addr){
    return addr / FILE_BASE_SIZE;
}

static inline uint64_t GetOffset(pointer_t addr){
    return addr % FILE_BASE_SIZE;
}

static inline uint64_t GetId(void *addr){
    return GetId(FILE_GET_OFFSET(addr));
}

static inline uint64_t GetOffset(void *addr){
    return GetOffset(FILE_GET_OFFSET(addr));
}


NVMFileAllocator::NVMFileAllocator(const std::string path, uint64_t size){
    pmemaddr_ = static_cast<char *>(pmem_map_file(path.c_str(), size, PMEM_FILE_CREATE, 0666, &mapped_len_, &is_pmem_));
                
    if (pmemaddr_ == nullptr) {
        ERROR_PRINT("mmap nvm path:%s failed!", path.c_str());
        exit(-1);
    } else {
        DBG_LOG("file allocator ok. path:%s size:%lu mapped_len:%lu is_pmem:%d", path.c_str(), size, mapped_len_, is_pmem_);
    }
    assert(size == mapped_len_);
    capacity_ = size;

    bitmap_ = new BitMap(capacity_ / FILE_BASE_SIZE);
    last_allocate_ = START_ALLOCATOR_INDEX;
    for(uint32_t i = 0; i < MAX_GROUP_BLOCK_TYPE; i++){
        groups_[i] = nullptr;
    }
}

NVMFileAllocator::~NVMFileAllocator(){
    Sync();
    pmem_unmap(pmemaddr_, mapped_len_);
    delete bitmap_;
}

uint64_t NVMFileAllocator::GetFreeIndex(){
    //mu_.Lock();
    MutexLock lock(&bitmap_mu_);
    uint64_t i = last_allocate_;
    uint64_t max = capacity_ / FILE_BASE_SIZE;

    for(; i < max;){
        if(!bitmap_->get(i)) {
            last_allocate_ = i + 1;
            bitmap_->set(i);
            return i;
        }
        i++;
    }
    last_allocate_ = START_ALLOCATOR_INDEX;
    i = last_allocate_;

    for(; i < max;){
        if(!bitmap_->get(i)) {
            last_allocate_ = i + 1;
            bitmap_->set(i);
            return i;
        }
        i++;
    }
    ERROR_PRINT("file allocate failed, no free space!\n");
    exit(-1);
    return 0;

}

void NVMFileAllocator::SetFreeIndex(uint64_t index){
    MutexLock lock(&bitmap_mu_);
    bitmap_->set(index);
}

NVMGroupBlockType NVMFileAllocator::SelectGroup(uint64_t size){
    assert(size <= FILE_BASE_SIZE);
    static const uint64_t block_1kb = 1ULL * 1024;
    static const uint64_t block_4kb = 4ULL * 1024;
    static const uint64_t block_16kb = 16ULL * 1024;
    static const uint64_t block_64kb = 64ULL * 1024;
    static const uint64_t block_256kb = 256ULL * 1024;
    static const uint64_t block_1mb = 1ULL * 1024 * 1024;
    static const uint64_t block_4mb = 4ULL * 1024 * 1024;
    static const uint64_t block_16mb = 16ULL * 1024 * 1024;
    static const uint64_t block_32mb = 32ULL * 1024 * 1024;
    static const uint64_t block_64mb = 64ULL * 1024 * 1024;
    switch (size){  //直接一块可存下，则选取该块
        case block_1kb:
            return NVMGroupBlockType::NVMGROUPBLOCK_1K_TYPE;
        case block_4kb:
            return NVMGroupBlockType::NVMGROUPBLOCK_4K_TYPE;
        case block_16kb:
            return NVMGroupBlockType::NVMGROUPBLOCK_16K_TYPE;
        case block_64kb:
            return NVMGroupBlockType::NVMGROUPBLOCK_64K_TYPE;
        case block_256kb:
            return NVMGroupBlockType::NVMGROUPBLOCK_256K_TYPE;
        case block_1mb:
            return NVMGroupBlockType::NVMGROUPBLOCK_1MB_TYPE;
        case block_4mb:
            return NVMGroupBlockType::NVMGROUPBLOCK_4MB_TYPE;
        case block_16mb:
            return NVMGroupBlockType::NVMGROUPBLOCK_16MB_TYPE;
        case block_32mb:
            return NVMGroupBlockType::NVMGROUPBLOCK_32MB_TYPE;
        case block_64mb:
            return NVMGroupBlockType::NVMGROUPBLOCK_64MB_TYPE;
        default:
            break;
    }

    if(size < block_64mb && size >  block_16mb){
        return NVMGroupBlockType::NVMGROUPBLOCK_4MB_TYPE;
    } else if(size < block_16mb && size >  block_1mb){
        return NVMGroupBlockType::NVMGROUPBLOCK_1MB_TYPE;
    } else if(size < block_1mb && size >  block_256kb){
        return NVMGroupBlockType::NVMGROUPBLOCK_64K_TYPE;
    } else if(size < block_256kb && size >  block_64kb){
        return NVMGroupBlockType::NVMGROUPBLOCK_16K_TYPE;
    } else if(size <  block_64kb && size > block_4kb){
        return NVMGroupBlockType::NVMGROUPBLOCK_4K_TYPE;
    } else {
        return NVMGroupBlockType::NVMGROUPBLOCK_1K_TYPE;
    }

    return NVMGroupBlockType::UNKNOWN_TYPE;
}

NVMGroupManager *NVMFileAllocator::CreateNVMGroupManager(NVMGroupBlockType type){
    uint64_t index = GetFreeIndex();
    NVMGroupManager *ret = new NVMGroupManager(index, type);
    map_mu_.Lock();
    map_groups_.insert(pair<uint64_t, NVMGroupManager *>(index, ret));
    map_mu_.Unlock();
    return ret;
}


void *NVMFileAllocator::Allocate(uint64_t size){
    uint8_t type = static_cast<uint8_t>(SelectGroup(size));
    groups_mu_[type].Lock();
    if(groups_[type] == nullptr){
        groups_[type] = CreateNVMGroupManager(static_cast<NVMGroupBlockType>(type));
    }
    NVMGroupManager *cur = groups_[type];
    int offset = cur->Allocate(size);
    if(offset == -1){
        cur = CreateNVMGroupManager(type);
        groups_[type] = cur;
        offset = cur->Allocate(size);
    }
    groups_mu_[type].Unlock();
    return pmemaddr_ + (cur->GetId() * FILE_BASE_SIZE + offset);
}

void *NVMFileAllocator::AllocateAndInit(uint64_t size, int c){
    void *addr = Allocate(size);
    nvm_memset_persist(addr, c, size);
    return addr;
}

void NVMFileAllocator::Free(void *addr, uint64_t len){
    Free(static_cast<char *>(addr) - pmemaddr_, len);
}

void NVMFileAllocator::Free(pointer_t addr, uint64_t len){
    NVMGroupBlockType type = static_cast<uint8_t>(SelectGroup(len));
    uint64_t id = GetId(addr);
    uint64_t offset = GetOffset(addr);
    map_mu_.Lock();
    auto it = map_groups_.find(id);
    it->second->Free(offset, len);
    if(it->second->FreeSpace() == FILE_BASE_SIZE && it->second != groups_[type]){
        map_groups_.erase(it);
    }
    map_mu_.Unlock();
}


}