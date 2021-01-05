/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-08 20:16:16
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_OPTION_H_
#define _METADB_OPTION_H_

#include <stdint.h>

namespace metadb {

class Option {
public:
    uint64_t FIRST_HASH_CAPACITY;   //dir 存在的一级hash的容量

    uint64_t INODE_MAX_ZONE_NUM;   //inode 存储的一级hash最大数，inode id通过hash到一个一个zone里面

    uint64_t INODE_HASHTABLE_INIT_SIZE;  //inode存储的hashtable的初始大小
    double INODE_HASHTABLE_TRIG_REHASH_TIMES;  //inode存储的hashtable的node num 已经是capacity的INODE_HASHTABLE_TRIG_REHASH_TIMES倍，触发rehash


    Option() ：FIRST_HASH_CAPACITY(4096),
            INODE_MAX_ZONE_NUM(1024),
            INODE_HASHTABLE_INIT_SIZE(64),
            INODE_HASHTABLE_TRIG_REHASH_TIMES(1.5) {
        
    }
    ~Option() {}
    
};

}








#endif