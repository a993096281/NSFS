/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-13 15:01:35
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "inode_db.h"

namespace metadb {

InodeDB::InodeDB(const Option &option, uint64_t capacity) : option_(option), capacity_(capacity){
    zones_ = new InodeZone[capacity_];
    for(uint32_t i = 0; i < capacity; i++){
        zones_[i].InitInodeZone(option);
    }
}

InodeDB::~InodeDB(){
    delete[] zones_;
}

int InodeDB::InodePut(const inode_id_t key, const Slice &value){
    return zones_[hash_zone_id(key)].InodePut(key, value);
}

int InodeDB::InodeUpdate(const inode_id_t key, const Slice &new_value){
    return zones_[hash_zone_id(key)].InodeUpdate(key, new_value);
}

int InodeDB::InodeGet(const inode_id_t key, std::string &value){
    return zones_[hash_zone_id(key)].InodeGet(key, value);
}

int InodeDB::InodeDelete(const inode_id_t key){
    return zones_[hash_zone_id(key)].InodeDelete(key);
}

uint32_t InodeDB::hash_zone_id(const inode_id_t key){
    return key % capacity_;
}

} // namespace name