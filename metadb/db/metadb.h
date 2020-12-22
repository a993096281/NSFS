/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 15:41:15
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_METADB_H_
#define _METADB_METADB_H_

#include "metadb/db.h"
#include "inode_db.h"
#include "dir_db.h"

namespace metadb {

class MetaDB : public DB {
public:
    MetaDB(const Option &option, const std::string &name);
    virtual ~MetaDB();

    virtual int DirPut(const inode_id_t key, const Slice &fname, const inode_id_t value);
    virtual int DirGet(const inode_id_t key, const Slice &fname, inode_id_t &value);
    virtual Iterator* DirGetIterator(const inode_id_t target);

    virtual int InodePut(const inode_id_t key, const Slice &value);
    virtual int InodeGet(const inode_id_t key, std::string &value);
private:
    DirDB *dir_db_;
    InodeDB *inode_db_;
};


}








#endif