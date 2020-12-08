/*************************************************************************
* Author: Kai Ren
* Created Time: 2011-11-27 13:38:34
* File Name: ./ldb_test.cpp
* Description: 
 ************************************************************************/

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/iterator.h"
#include "leveldb/status.h"
#include "leveldb/filter_policy.h"
#include "util/hash.h"
#include "fs/tfs_inode.h"
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <set>

using namespace leveldb;
using namespace tablefs;

void print_slice(Slice s) {
  printf("size: %d, content: ", s.size());
  for (int i = 0; i < s.size(); ++i) {
    printf("%c", s[i]);
  }
  printf("\n");
}

void check_stat(struct stat statbuf) {
  if (statbuf.st_uid != 0) {
    printf("Wrong UID %d\n", statbuf.st_uid);
  }
  if ((statbuf.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO | S_IFREG)) == 0) {
    printf("WrongMode\n");
  }
}

/*
typedef uint32_t tfs_inode_t;
typedef uint32_t tfs_hash_t;
typedef struct stat tfs_stat_t;

struct tfs_meta_key_t {
  char str[17];
  tfs_inode_t inode_id;
  tfs_hash_t hash_id;

  leveldb::Slice ToSlice() const {
    return leveldb::Slice(str, 16);
  }
};

struct tfs_inode_header {
  tfs_stat_t fstat;
  uint32_t namelen;
  char buffer[1];
};
*/

inline static void BuildMetaKey(const tfs_inode_t inode_id,
                                const tfs_hash_t hash_id,
                                tfs_meta_key_t &key) {
  key.inode_id = inode_id;
  key.hash_id = hash_id;
  //snprintf(key.str, sizeof(key.str), "%08x%08x", inode_id, hash_id);
}

inline static void BuildMetaKey(const char *path,
                                const int len,
                                const tfs_inode_t inode_id,
                                tfs_meta_key_t &key) {
  BuildMetaKey(inode_id, leveldb::Hash(path, len, 0), key);
}

void TestBloomfilter(std::string dbname) {
  DB* db_;
  Options options;
  options.create_if_missing = true;
  options.filter_policy = NewBloomFilterPolicy(15);
  Status s = DB::Open(options, dbname, &db_);
  WriteOptions wopt;
  FILE* f = fopen("/users/kair/code/tablefs/traces/twom.txt", "r");
  char st1[200], st2[200];
  fscanf(f, "%s %s", st1, st2);
  for (int i = 0; i < 200000; ++i) {
    fscanf(f, "%s %s", st1, st2);
    tfs_meta_key_t key;
    BuildMetaKey(st1+1, strlen(st1+1), 0, key);
    db_->Put(wopt, key.ToSlice(), std::string(""));
  }
  fclose(f);
  delete db_;

  s = DB::Open(options, dbname, &db_);
  ReadOptions ropt;
  f = fopen("/users/kair/code/tablefs/traces/twom.txt", "r");
  fscanf(f, "%s %s", st1, st2);
  for (int i = 0; i < 200; ++i) {
    fscanf(f, "%s %s", st1, st2);
    tfs_meta_key_t key;
    BuildMetaKey(st1+1, strlen(st1+1), 0, key);
    std::string result;
    Status s = db_->Get(ropt, key.ToSlice(), &result);
    if (!s.ok() || s.IsNotFound()) {
      printf("Wrong %d\n", i);
    }
  }
  fclose(f);
  delete db_;
}

void TestKeys(std::string dbname) {
  Options options;
  options.create_if_missing = true;

  DB* db_;
  options.filter_policy = NewBloomFilterPolicy(10);
  Status s = DB::Open(options, dbname, &db_);
  std::set<int> inode_counter;

  if (s.ok()) {
    ReadOptions ropt;
    Iterator* it = db_->NewIterator(ropt);
    int i = 0;
    int size = 0;
    printf("hello!\n");
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
       ++i;
       size += it->value().size();
       tfs_inode_header* header = (tfs_inode_header*) it->value().data();
       check_stat(header->fstat);
       if (inode_counter.find(header->fstat.st_ino) != inode_counter.end()) {
         printf("Duplicated inode!\n");
       } else {
         inode_counter.insert(header->fstat.st_ino);
       }
       char str[17];
       snprintf(str, 17, "%08x%08x", 0,
                Hash(it->value().data() + TFS_INODE_HEADER_SIZE, header->namelen, 0));
       if (strncmp(it->key().data(), str, 16) != 0) {
         printf("Hash corrupted! %d%s %s\n", it->key().data(), str);
       }
    }
    printf("Total entries: %d, Total size: %d, Avg size: %lf\n", i, size, float(size) / i);
  } else {
    printf("bug...\n");
    printf("Fail to open db: %s\n", s.ToString().c_str());
  }

  delete db_;
}

void PrintKeys(std::string dbname) {
  Options options;
  options.create_if_missing = true;

  DB* db_;
  options.filter_policy = NewBloomFilterPolicy(10);
  Status s = DB::Open(options, dbname, &db_);

  if (s.ok()) {
    ReadOptions ropt;
    Iterator* it = db_->NewIterator(ropt);
    int i = 0;
    int size = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      tfs_meta_key_t* key = (tfs_meta_key_t*) it->key().data();
      printf("key=[%08x, %08x]\n", key->inode_id, key->hash_id);
      printf("value=[size:%d]\n", it->value().size());
      tfs_inode_header* header = (tfs_inode_header*) it->value().data();
      printf("value=[fstat:%d]\n", sizeof(header->fstat));
      printf("value=[namelen:%d]\n", header->namelen);
      printf("header_size=%d\n", TFS_INODE_HEADER_SIZE);
    }
  }
}

int main(int nargs, char* args[]) {
  if (nargs < 2) {
    fprintf(stderr, "Not enough arguments.\n");
    return 1;
  }

  std::string dbname = std::string(args[1]);
  //TestBloomfilter(dbname);
  //TestKeys(dbname);
  PrintKeys(dbname);
  return 0;
}
