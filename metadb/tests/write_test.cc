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
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % nums;
        key = id;
        value = id;
        snprintf(fname, 8 + 1, "%0*llx", 8, id);
        DBG_LOG("put:%d key:%lu fname:%.*s value:%lx hash_fname:%lx", i, key, 8, fname, value, MurmurHash64(fname, 8));
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

void DirRandomRead(DB* db){
    uint32_t seed = 1000;
    uint64_t nums = 1000;

    inode_id_t key;
    char *fname = new char[8 + 1]; 
    inode_id_t value;
    uint64_t id = 0;
    uint64_t found = 0;
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % nums;
        key = id;
        value = 0;
        snprintf(fname, 8 + 1, "%0*llx", 8, id);

        ret = db->DirGet(key, Slice(fname, 8), value);
        if(ret != 0 && ret != 2){
            fprintf(stderr, "dir get error! key:%lu fname:%.*s\n", key, 8, fname);
            fflush(stderr);
            exit(1);
        }
        if(ret == 0) {
            if(value != key) fprintf(stdout, "dir get wrong value! key:%lu fname:%.*s value:%lu\n", key, 8, fname, value);
            found++;
        }
        else{
            fprintf(stdout, "dir read no found! key:%lu fname:%.*s\n", key, 8, fname);
        }
    }

    delete fname;
    printf("(%lu of %lu found)", found, nums);
}

void DirRandomDelete(DB* db){
    uint32_t seed = 1000;
    uint64_t nums = 1000;

    inode_id_t key;
    char *fname = new char[8 + 1]; 
    uint64_t id = 0;
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % nums;
        key = id;
        snprintf(fname, 8 + 1, "%0*llx", 8, id);

        ret = db->DirDelete(key, Slice(fname, 8));
        if(ret != 0 && ret != 1 && ret != 2){
            fprintf(stderr, "dir delete error!key:%lu fname:%.*s\n", key, 8, fname);
            fflush(stderr);
            exit(1);
        }
    }
    db->PrintDir();
    delete fname;
}
void DirRandomRange(DB* db){
    uint32_t seed = 1000;
    uint64_t nums = 1000;

    inode_id_t key;
    string fname;
    inode_id_t value;
    uint64_t id = 0;
    uint64_t found = 0;
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % nums;
        key = id;
        Iterator *it = db->DirGetIterator(key);
        if(it != nullptr){
            for(it->SeekToFirst(); it->Valid(); it->Next()){
                fname = it->fname();
                value = it->value();
                uint64_t hash_fname = it->hash_fname();
                DBG_LOG("range:%d key:%lu fname:%.*s value:%lx hash_fname:%lx %lx", i, key, 8, fname.c_str(), value, MurmurHash64(fname.c_str(), 8), hash_fname);
            }
            delete it;
            found++;
        }
    }
    printf("(%lu of %lu found)", found, nums);
}

void InodeRandomWrite(DB* db){
    uint32_t seed = 1000;
    uint64_t nums = 100;

    inode_id_t key;
    char *value = new char[16 + 1];
    uint64_t id = 0;
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % nums;
        key = id;
        snprintf(value, 16 + 1, "%0*llx", 16, id);
        DBG_LOG("inode put:%d key:%lu value:%.*s key:%lx", i, key, 16, value, key);
        ret = db->InodePut(key, Slice(value, 16));
        if(ret != 0 && ret != 2){
            fprintf(stderr, "inode put error! key:%lu value:%.*s \n", key, 16, value);
            fflush(stderr);
            exit(1);
        }
        db->PrintInode();
    }
    delete value;
    db->PrintInode();
}

void InodeRandomRead(DB* db){
    uint32_t seed = 1000;
    uint64_t nums = 1000;

    inode_id_t key;
    string value;
    uint64_t id = 0;
    uint64_t found = 0;
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % nums;
        key = id;
        ret = db->InodeGet(key, value);
        if(ret != 0 && ret != 2){
            fprintf(stderr, "inode get error! key:%lu \n", key);
            fflush(stderr);
            exit(1);
        }
        if(ret == 0) {
            found++;
        }
        else{
            fprintf(stdout, "inode read no found! key:%lu \n", key);
        }
    }
    printf("(%lu of %lu found)", found, nums);
}

void InodeRandomDelete(DB* db){
    uint32_t seed = 1000;
    uint64_t nums = 1000;

    inode_id_t key;
    uint64_t id = 0;
    int ret = 0;
    for(int i = 0; i < nums; i++){
        //id = Random64(&seed);
        id = Random64(&seed) % nums;
        key = id;

        ret = db->InodeDelete(key);
        if(ret != 0 && ret != 2){
            fprintf(stderr, "inode delete error!key:%lu \n", key);
            fflush(stderr);
            exit(1);
        }
        db->PrintInode();
    }
}


int main(int argc, char *argv[])
{
    DB *db;
    Option option;
    option.DIR_FIRST_HASH_MAX_CAPACITY = 1;
    option.INODE_MAX_ZONE_NUM = 1;
    option.INODE_HASHTABLE_INIT_SIZE = 16;
    option.file_allocator_path = "/home/lzw/test/file.pool";
    option.node_allocator_path = "/home/lzw/test/node.pool";
    option.file_allocator_size = 1 * 1024 * 1024;
    option.node_allocator_path = 1 * 1024 * 1024;
    int ret = DB::Open(option, "testdb", &db);
    if(ret != 0){
        fprintf(stderr, "open db error, Test stop\n");
        return -1;
    }

    DirRandomWrite(db);
    //DirRandomRead(db);
    //DirRandomDelete(db);
    //DirRandomRange(db);

    //InodeRandomWrite(db);

    delete db;
    return 0;
}