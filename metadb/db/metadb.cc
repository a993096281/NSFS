/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 20:00:16
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "metadb.h"

namespace metadb {

int DB::Open(const Option &option, const std::string &name, DB **dbptr){
    return 0;
}

MetaDB::MetaDB(const Option &option, const std::string &name){

}

MetaDB::~MetaDB(){

}
int MetaDB::DirPut(const inode_id_t key, const Slice &fname, const inode_id_t value){
    return dir_db_->DirPut(key, fname, value);
}

int MetaDB::DirGet(const inode_id_t key, const Slice &fname, inode_id_t &value){
    return dir_db_->DirGet(key, fname, value);
}

int MetaDB::DirDelete(const inode_id_t key, const Slice &fname){
    return dir_db_->DirDelete(key, fname);
}

Iterator* MetaDB::DirGetIterator(const inode_id_t target){
    return dir_db_->DirGetIterator(target);
}
    
int MetaDB::InodePut(const inode_id_t key, const Slice &value){
    return inode_db_->InodePut(key, value);
}

int MetaDB::InodeGet(const inode_id_t key, std::string &value){
    return inode_db_->InodeGet(key, value);
}

}