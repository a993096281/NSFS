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
    uint64_t FIRST_HASH_CAPACITY;

    Option() ：FIRST_HASH_CAPACITY(4096) {
        
    }
    ~Option() {}
    
};

}








#endif