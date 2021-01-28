/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-14 14:12:48
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include <assert.h>
#include "nvm_node_allocator.h"
#include "metadb/debug.h"

namespace metadb {

char *node_pool_pointer = nullptr;
NVMNodeAllocator *node_allocator = nullptr;

int InitNVMNodeAllocator(const std::string path, uint64_t size){
    node_allocator = new NVMNodeAllocator(path, size);
    node_pool_pointer = node_allocator->GetPmemAddr();
    return 0;
}

NVMNodeAllocator::NVMNodeAllocator(const std::string path, uint64_t size){
    pmemaddr_ = static_cast<char *>(pmem_map_file(path.c_str(), size, PMEM_FILE_CREATE, 0666, &mapped_len_, &is_pmem_));
                
    if (pmemaddr_ == nullptr) {
        ERROR_PRINT("mmap nvm path:%s failed!", path.c_str());
        exit(-1);
    } else {
        DBG_LOG("node allocator ok. path:%s size:%lu mapped_len:%lu is_pmem:%d addr:%p", path.c_str(), size, mapped_len_, is_pmem_, pmemaddr_);
    }
    assert(size == mapped_len_);
    capacity_ = size;

    bitmap_ = new BitMap(capacity_ / NODE_BASE_SIZE);
    last_allocate_ = START_ALLOCATOR_INDEX;
}

NVMNodeAllocator::~NVMNodeAllocator(){
    Sync();
    pmem_unmap(pmemaddr_, mapped_len_);
    delete bitmap_;
}

uint64_t NVMNodeAllocator::GetFreeIndex(uint64_t size){
    //mu_.Lock();
    MutexLock lock(&mu_);
    uint64_t i = last_allocate_;
    uint64_t max = capacity_ / NODE_BASE_SIZE;
    uint64_t need = size / NODE_BASE_SIZE;
    uint64_t ok = 0;
    for(; i < max;){
        if(!bitmap_->get(i)) {
            ok = 1;
            while(ok < need && (i + ok) < max && !bitmap_->get(i + ok)){
                ok++;
            }
            if(ok >= need){  //申请成功
                last_allocate_ = i + ok;
                for(uint64_t j = i; j < need; j++){
                    bitmap_->set(j);
                }
                //mu_.Unlock();
                return i;
            }
            else if((i + ok) > max){  //地址超过
                last_allocate_ = START_ALLOCATOR_INDEX;
                break;
            }
            else{  //申请失败
                i += ok;
            }
        }
        i++;
    }

    i = last_allocate_;

    for(; i < max;){
        if(!bitmap_->get(i)) {
            ok = 1;
            while(ok < need && (i + ok) < max && !bitmap_->get(i + ok)){
                ok++;
            }
            if(ok >= need){  //申请成功
                last_allocate_ = i + ok;
                for(uint64_t j = i; j < need; j++){
                    bitmap_->set(j);
                }
                //mu_.Unlock();
                return i;
            }
            else if((i + ok) > max){  //地址超过
                last_allocate_ = START_ALLOCATOR_INDEX;
                break;
            }
            else{  //申请失败
                i += ok;
            }
        }
        i++;
    }
    ERROR_PRINT("node allocate failed, no free space! size:%lu\n", size);
    exit(-1);
    return 0;

}

void NVMNodeAllocator::SetFreeIndex(uint64_t offset, uint64_t len){
    MutexLock lock(&mu_);
    uint64_t index = offset / NODE_BASE_SIZE;
    uint64_t num = len / NODE_BASE_SIZE;
    for(uint64_t i = 0; i < num; i++){
        bitmap_->set(index + i);
    }
    DBG_LOG("Node Free: offset:%llu, len:%llu", offset, len);
}

void *NVMNodeAllocator::Allocate(uint64_t size){  
    uint64_t allocated = (size + NODE_BASE_SIZE - 1) & (~(NODE_BASE_SIZE - 1));  //保证按照NODE_BASE_SIZE分配
    uint64_t index = GetFreeIndex(allocated);
    DBG_LOG("Node Allocate: size:%llu allocated:%llu index:%llu offset:%llu", size, allocated, index, index * NODE_BASE_SIZE);
    return static_cast<void *>(pmemaddr_ + index * NODE_BASE_SIZE);

}

void *NVMNodeAllocator::AllocateAndInit(uint64_t size, int c){
    void *addr = Allocate(size);
    nvm_memset_persist(addr, c, size);
    return addr;
}

void NVMNodeAllocator::Free(void *addr, uint64_t len){
    uint64_t allocated = (len + NODE_BASE_SIZE - 1) & (~(NODE_BASE_SIZE - 1));  //保证按照NODE_BASE_SIZE分配
    uint64_t offset = static_cast<char *>(addr) - pmemaddr_;
    assert(offset < capacity_);
    SetFreeIndex(offset, allocated);
}

void NVMNodeAllocator::Free(pointer_t addr, uint64_t len){
    uint64_t allocated = (len + NODE_BASE_SIZE - 1) & (~(NODE_BASE_SIZE - 1));  //保证按照NODE_BASE_SIZE分配
    uint64_t offset = addr;
    assert(offset < capacity_);
    SetFreeIndex(offset, allocated);
}

} // namespace name