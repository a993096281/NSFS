/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-02-22 16:04:55
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _TABLEFS_ADAPTOR_H_
#define _TABLEFS_ADAPTOR_H_


#include "leveldb/db.h"
#include <string>
#include "inode_format.h"

namespace tablefs {

using namespace leveldb;

class DBAdaptor {
public:
    DBAdaptor();
    ~DBAdaptor();
    int Init();

    void Cleanup();

    int Get(const leveldb::Slice &key, std::string &result);
    int Put(const leveldb::Slice &key, const leveldb::Slice &values);
    int Delete(const leveldb::Slice &key);

    int Sync();

    int Init(const std::string & path = "");

    inline Iterator * NewIterator() { return db_->NewIterator(ReadOptions()); }

protected:
    bool inited_;
    DB * db_;
    Options options_;
    static const std::string default_DBpath;

};

}







#endif