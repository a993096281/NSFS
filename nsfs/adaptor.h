/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-02-22 16:04:55
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _NSFS_ADAPTOR_H_
#define _NSFS_ADAPTOR_H_


#include "metadb/db.h"
#include <string>
#include "inode_format.h"

namespace nsfs {

using namespace metadb;

class DBAdaptor {
public:
    DBAdaptor();
    ~DBAdaptor();
    int Init();

    void Cleanup();

    int DirPut(const inode_id_t key, const Slice &fname, const inode_id_t value);
    int DirGet(const inode_id_t key, const Slice &fname, inode_id_t &value);
    int DirDelete(const inode_id_t key, const Slice &fname);
    Iterator* DirGetIterator(const inode_id_t target);

    int InodePut(const inode_id_t key, const Slice &value);
    int InodeGet(const inode_id_t key, std::string &value);
    int InodeDelete(const inode_id_t key);

    int Sync();

    int Init(const std::string & path = "");


protected:
    bool inited_;
    DB * db_;
    Option options_;
    static const std::string default_DBpath;

};

}







#endif