/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 20:01:46
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_INODE_DB_H_
#define _METADB_INODE_DB_H_


#include <string>
#include "metadb/option.h"
#include "metadb/slice.h"
#include "metadb/inode.h"
#include "metadb/iterator.h"

namespace metadb {

class InodeDB {
public:
    InodeDB(const Option &option);
    virtual ~InodeDB();

    virtual int InodePut(const inode_id_t key, const Slice &value);
    virtual int InodeGet(const inode_id_t key, std::string &value);
    virtual int InodeDelete(const inode_id_t key);
private:
    
};


}








#endif