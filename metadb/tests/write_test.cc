#include <stdio.h>
#include <stdlib.h>

#include "../include/metadb/all_header.h"

using namespace metadb;

static inline uint64_t Random64(uint32_t *seed){
    return (((uint64_t)rand_r(seed)) << 32 ) | (uint64_t)rand_r(seed);
}

void DirRandomWrite(DB* db){
    uint32_t seed = 1000;
    uint64_t nums = 1000;

    inode_id_t key;
    char *fname = new char[8 + 1];
    inode_id_t value;
    uint64_t id = 0;
    uint64_t bytes = 0;
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % nums;
        key = 1;
        value = id;
        snprintf(fname, 8 + 1, "%0*llx", 8, id);
        DBG_LOG("put:%d key:%lu fname:%.*s value:%lx", i, key, 8, fname, value);
        ret = db->DirPut(key, Slice(fname, 8), value);
        if(ret != 0){
            fprintf(stderr, "dir put error! key:%lu fname:%.*s value:%llx\n", key, 8, fname, value);
            fflush(stderr);
            exit(1);
        }
        //db->PrintDir();
    }
    db->PrintDir();
    delete fname;
}

int main(int argc, char *argv[])
{
    DB *db;
    Option option;
    option.DIR_FIRST_HASH_MAX_CAPACITY = 1;
    option.INODE_MAX_ZONE_NUM = 1;
    int ret = DB::Open(option, "testdb", &db);
    if(ret != 0){
        fprintf(stderr, "open db error, Test stop\n");
        return -1;
    }

    DirRandomWrite(db);

    delete db;
    return 0;
}