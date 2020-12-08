#define FUSE_USE_VERSION 26

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cstdarg>
#include <error.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <time.h>
#include <vector>
#include <algorithm>
#include "fs/tablefs.h"
#include "util/hash.h"
#include "leveldb/env.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/write_batch.h"

namespace tablefs {

inline static void BuildMetaKey(const char *path,
                                const int len,
                                const tfs_inode_t inode_id,
                                tfs_meta_key_t &key) {
  key.inode_id = inode_id;
  key.hash_id = leveldb::Hash(path, len, 0);
}

inline static bool IsKeyInDir(const leveldb::Slice &key,
                              const tfs_meta_key_t &dirkey) {
  const tfs_meta_key_t* rkey = (const tfs_meta_key_t *) key.data();
  return rkey->inode_id == dirkey.inode_id;
}

const tfs_inode_header *GetInodeHeader(InodeHandle* ihandle) {
  return reinterpret_cast<const tfs_inode_header*> (ihandle->value_.data());
}

const tfs_stat_t *GetAttribute(InodeHandle* ihandle) {
  return reinterpret_cast<const tfs_stat_t*> (ihandle->value_.data());
}

size_t GetInlineData(InodeHandle* ihandle, char* buf, size_t offset, size_t size) {
  const tfs_inode_header* header = GetInodeHeader(ihandle);
  size_t realoffset = TFS_INODE_HEADER_SIZE + header->namelen + 1 + offset;
  if (realoffset < ihandle->value_.size()) {
    if (realoffset + size > ihandle->value_.size()) {
      size = ihandle->value_.size() - realoffset;
    }
    memcpy(buf, ihandle->value_.c_str() + realoffset , size);
    return size;
  } else {
    return 0;
  }
}

inline void UpdateIhandleValue(InodeHandle* ihandle,
                        const char* buf, size_t offset, size_t size) {
  if (offset > ihandle->value_.size()) {
    ihandle->value_.resize(offset);
  }
  ihandle->value_.replace(offset, size, buf, size);
}

void UpdateInodeHeader(InodeHandle* ihandle,
                       tfs_inode_header &new_header) {
  UpdateIhandleValue(ihandle, (const char *) &new_header,
                     0, TFS_INODE_HEADER_SIZE);
}

void UpdateAttribute(InodeHandle* ihandle,
                     const tfs_stat_t &new_fstat) {
  UpdateIhandleValue(ihandle, (const char *) &new_fstat,
                     0, TFS_INODE_ATTR_SIZE);
}

void UpdateInlineData(InodeHandle* ihandle,
                      const char* buf, size_t offset, size_t size) {
  const tfs_inode_header* header = GetInodeHeader(ihandle);
  size_t realoffset = TFS_INODE_HEADER_SIZE + header->namelen + 1 + offset;
  UpdateIhandleValue(ihandle, buf, realoffset, size);
}

void TruncateInlineData(InodeHandle* ihandle, size_t new_size) {
  const tfs_inode_header* header = GetInodeHeader(ihandle);
  size_t target_size = TFS_INODE_HEADER_SIZE + header->namelen + new_size + 1;
  ihandle->value_.resize(target_size);
}

void DropInlineData(InodeHandle* ihandle) {
  const tfs_inode_header* header = GetInodeHeader(ihandle);
  size_t target_size = TFS_INODE_HEADER_SIZE + header->namelen + 1;
  ihandle->value_.resize(target_size);
}

void TableFS::SetState(FileSystemState* state) {
  state_ = state;
}

int TableFS::FSError(const char *err_msg) {
  int retv = -errno;
#ifdef TABLEFS_DEBUG
  state_->GetLog()->LogMsg(err_msg);
#endif
  return retv;
}

void TableFS::InitStat(tfs_stat_t &statbuf,
                       tfs_inode_t inode,
                       mode_t mode,
                       dev_t dev) {
  statbuf.st_ino = inode;
  statbuf.st_mode = mode;
  statbuf.st_dev = dev;
  if (flag_fuse_enabled) {
    statbuf.st_gid = fuse_get_context()->gid;
    statbuf.st_uid = fuse_get_context()->uid;
  }

  statbuf.st_size = 0;
  statbuf.st_blksize = 0;
  statbuf.st_blocks = 0;
  if S_ISREG(mode) {
    statbuf.st_nlink = 1;
  } else {
    statbuf.st_nlink = 2;
  }
  time_t now = time(NULL);
  statbuf.st_atim.tv_sec = now;
  statbuf.st_ctim.tv_sec = now;
  statbuf.st_mtim.tv_sec = now;
}

//TODO: Locking problem
bool TableFS::ParentPathLookup(const char *path,
                               tfs_meta_key_t &key,
                               tfs_inode_t &inode_in_search,
                               const char* &lastdelimiter) {
  const char* lpos = path;
  const char* rpos;
  std::string item;
  inode_in_search = ROOT_INODE_ID;
  LevelDBAdaptor *metadb = state_->GetMetaDB();
  while ((rpos = strchr(lpos+1, PATH_DELIMITER)) != NULL) {
    if (rpos - lpos > 0) {
      BuildMetaKey(lpos+1, rpos-lpos-1, inode_in_search, key);
      if (!dentry_cache->Find(key, inode_in_search)) {
        InodeCacheHandle* handle =
          inode_cache->GetInodeHandle(key, INODE_READ);
        if (handle != NULL) {
          const tfs_inode_header* value =
            inode_cache->GetInodeHeader(handle);
          inode_in_search = value->fstat.st_ino;
          dentry_cache->Insert(key, inode_in_search);
          inode_cache->ReleaseInodeHandle(handle);
//TODO:          inode_cache->EvictInodeHandle(key);
        } else {
          errno = ENOENT;
          return false;
        }
      }
    }
    lpos = rpos;
  }
  if (lpos == path) {
    BuildMetaKey(NULL, 0, ROOT_INODE_ID, key);
  }
  lastdelimiter = lpos;
  return true;
}

bool TableFS::PathLookup(const char *path,
                         tfs_meta_key_t &key) {
  const char* lpos;
  tfs_inode_t inode_in_search;
  if (ParentPathLookup(path, key, inode_in_search, lpos)) {
    const char* rpos = strchr(lpos, '\0');
    if (rpos != NULL && rpos-lpos > 1) {
      BuildMetaKey(lpos+1, rpos-lpos-1, inode_in_search, key);
    }
    return true;
  } else {
    errno = ENOENT;
    return false;
  }
}

bool TableFS::PathLookup(const char *path,
                         tfs_meta_key_t &key,
                         leveldb::Slice &filename) {
  const char* lpos;
  tfs_inode_t inode_in_search;
  if (ParentPathLookup(path, key, inode_in_search, lpos)) {
    const char* rpos = strchr(lpos, '\0');
    if (rpos != NULL && rpos-lpos > 1) {
      BuildMetaKey(lpos+1, rpos-lpos-1, inode_in_search, key);
      filename = leveldb::Slice(lpos+1, rpos-lpos-1);
    } else {
      filename = leveldb::Slice(lpos, 1);
    }
    return true;
  } else {
    errno = ENOENT;
    return false;
  }
}

void* TableFS::Init(struct fuse_conn_info *conn) {
  state_->GetLog()->LogMsg("TableFS initialized.\n");
  if (conn != NULL) {
    flag_fuse_enabled = true;
  } else {
    flag_fuse_enabled = false;
  }
  if (state_->IsEmpty()) {
    //TODO:
    LevelDBAdaptor* metadb = state_->GetMetaDB();
    tfs_meta_key_t key;
    BuildMetaKey(NULL, 0, ROOT_INODE_ID, key);
    tfs_inode_header value;
    value.namelen = 0;
    value.buffer[0] = '\0';
    //TODO: how to init the root directory
    lstat(ROOT_INODE_STAT, &value.fstat);
    if (metadb->Put(key.ToSlice(), value.ToSlice()) < 0) {
      state_->GetLog()->LogMsg("TableFS create root directory failed.\n");
    } else {
      state_->GetLog()->LogMsg("TableFS create root directory %d %d %08x.\n",
                                sizeof(value), key.inode_id, key.hash_id);
      state_->GetLog()->LogMsg("Root Stat %08x %d %d.\n",
                               value.fstat.st_mode,
                               value.fstat.st_uid,
                               value.fstat.st_gid);
    }
  }
  inode_cache = new InodeCache(state_->metadb);
  dentry_cache = new DentryCache(16384);
  return state_;
}

void TableFS::Destroy(void * data) {
  if (dentry_cache != NULL)
    delete dentry_cache;
  if (inode_cache != NULL) {
    inode_cache->Destroy();
    delete inode_cache;
  }
  state_->GetLog()->LogMsg("Clean write back cache.\n");
  state_->Destroy();
  delete state_;
}

int TableFS::GetAttr(const char *path, struct stat *statbuf) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("GetAttr: %s\n", path);
#endif

  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
     return FSError("GetAttr Path Lookup: No such file or directory: %s\n");
  }
  InodeCacheHandle* handle = inode_cache->GetInodeHandle(key, INODE_READ);
  if (handle != NULL ) {
    InodeHandle* ihandle = inode_cache->Value(handle);
    if (ihandle->mode_ != INODE_DELETE) {
      *statbuf = *GetAttribute(ihandle);
//      lstat("/tmp/test", statbuf);
#ifdef  TABLEFS_DEBUG
      state_->GetLog()->LogMsg("GetAttr: %08x %08x \nGetAttr File Size: %d\n",
                              key.inode_id, key.hash_id, statbuf->st_size);
#endif
      inode_cache->ReleaseInodeHandle(handle);
//TODO:      inode_cache->EvictInodeHandle(key);
      return 0;
    }
  }
  errno = ENOENT;
  return FSError("GetAttr At End: No such file or directory\n");
}

void TableFS::GetDiskFilePath(char *path, tfs_inode_t inode_id) {
  sprintf(path, "%s/%d/%d",
          state_->GetDataDir().data(),
          (int) inode_id >> NUM_FILES_IN_DATADIR_BITS,
          (int) inode_id % (NUM_FILES_IN_DATADIR));
}

int TableFS::OpenDiskFile(tfs_inode_t inode_id, mode_t mode, int flags) {
  char fpath[128];
  GetDiskFilePath(fpath, inode_id);
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("OpenDiskFile: %s InodeID: %d\n", fpath, inode_id);
#endif
  int fd = open(fpath, flags | O_CREAT, mode);
  return fd;
}

void TableFS::TruncateDiskFile(tfs_inode_t inode_id, off_t new_size) {
  char fpath[128];
  GetDiskFilePath(fpath, inode_id);
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("TruncateDiskFile: %s, InodeID: %d, NewSize: %d\n",
                          fpath, inode_id, new_size);
#endif
  truncate(fpath, new_size);
}

ssize_t TableFS::MigrateDiskFileToBuffer(tfs_inode_t inode_id,
                                         char* buffer,
                                         size_t size) {
  char fpath[128];
  GetDiskFilePath(fpath, inode_id);
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("MigrateDiskFile: %s InodeID: %d\n",
                           fpath, inode_id);
#endif
  int fd = open(fpath, O_RDONLY);
  ssize_t ret = pread(fd, buffer, size, 0);
  close(fd);
  unlink(fpath);
  return ret;
}

void TableFS::MigrateToDiskFile(InodeHandle* ihandle,
                                const tfs_inode_header* value,
                                int flags) {
  ihandle->fd_ = OpenDiskFile(value->fstat.st_ino,
                              value->fstat.st_mode,
                              flags);
  if (ihandle->fd_ < 0) {
    return;
  }
  if (value->fstat.st_size > 0 ) {
    pwrite(ihandle->fd_, value->buffer+value->namelen+1,
           value->fstat.st_size, 0);
    DropInlineData(ihandle);
  }
}

void TableFS::CloseDiskFile(int& fd_) {
  close(fd_);
  fd_ = -1;
}

int TableFS::Open(const char *path,struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Open: %s, Flags: %d\n", path, fi->flags);
#endif

  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    return FSError("Open: No such file or directory\n");
  }
  //  inode_cache->Lock(key);
  InodeCacheHandle* handle = NULL;
  if ((fi->flags & O_RDWR) > 0 ||
      (fi->flags & O_WRONLY) > 0 ||
      (fi->flags & O_TRUNC) > 0) {
    handle = inode_cache->GetInodeHandle(key, INODE_WRITE);
  } else {
    handle = inode_cache->GetInodeHandle(key, INODE_READ);
  }
  int ret = 0;
  //Per Handle Lock?
  if (handle != NULL) {
    InodeHandle* ihandle = inode_cache->Value(handle);
    const tfs_stat_t *value = GetAttribute(ihandle);
    if (value->st_size > state_->GetThreshold() &&
        ihandle->fd_ == -1) {
      ihandle->fd_ = OpenDiskFile(value->st_ino,
                    value->st_mode, fi->flags);
      if (ihandle->fd_ < 0) {
        inode_cache->ReleaseInodeHandle(handle);
        ret = -1;
      }
    }
    fi->fh = (uint64_t) handle;
    return 0;
  } else {
    errno = -1;
    return FSError("Cannot open file\n");
  }
}

int TableFS::Read(const char* path, char *buf, size_t size,
                  off_t offset, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Read: %s\n", path);
#endif

  InodeCacheHandle* handle = reinterpret_cast<InodeCacheHandle*>(fi->fh);
  InodeHandle* ihandle = inode_cache->Value(handle);
  const tfs_inode_header* value = GetInodeHeader(ihandle);

  if (value->fstat.st_size > state_->GetThreshold()) {
    if (ihandle->fd_ < 0) {
      ihandle->fd_ = OpenDiskFile(value->fstat.st_ino,
                    value->fstat.st_mode, fi->flags);
      if (ihandle->fd_ < 0)
        return FSError("Read file error\n");
    }
    int ret = pread(ihandle->fd_, buf, size, offset);
    if (ret < 0) {
      return FSError("Read file error\n");
    } else {
      return ret;
    }
  } else {
    int ret = GetInlineData(ihandle, buf, offset, size);
    return ret;
  }
}

int TableFS::Write(const char* path, const char *buf, size_t size,
                   off_t offset, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Write: %s %lld %d\n", path, offset, size);
#endif

  InodeCacheHandle* handle = reinterpret_cast<InodeCacheHandle*>(fi->fh);
  InodeHandle* ihandle = inode_cache->Value(handle);
  const tfs_inode_header* value = GetInodeHeader(ihandle);

  int ret;
  if (value->fstat.st_size > state_->GetThreshold()) {
    if (ihandle->fd_ < 0) {
      ihandle->fd_ = OpenDiskFile(value->fstat.st_ino,
                    value->fstat.st_mode, fi->flags);
      if (ihandle->fd_ < 0)
        return FSError("Write file error\n");
    }
    ret = pwrite(ihandle->fd_, buf, size, offset);
  } else {
    if (offset + size > state_->GetThreshold()) {
      MigrateToDiskFile(ihandle, value, fi->flags);
      ret = pwrite(ihandle->fd_, buf, size, offset);
    } else {
      UpdateInlineData(ihandle, buf, offset, size);
      ret = size;
    }
  }
  if (ret < 0) {
    return FSError("Write file error\n");
  }
  if (offset + ret > value->fstat.st_size) {
    tfs_inode_header new_value = *value;
    new_value.fstat.st_size = offset + ret;
    UpdateInodeHeader(ihandle, new_value);
  }
  return ret;
}

int TableFS::Release(const char *path, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Release: %s\n", path);
#endif

  InodeCacheHandle* handle = reinterpret_cast<InodeCacheHandle*>(fi->fh);
  InodeHandle* ihandle = inode_cache->Value(handle);

  int ret = 0;
  //TODO: How to change time stamp ?
  if (ihandle->mode_ == INODE_WRITE) {
    const tfs_stat_t *value = GetAttribute(ihandle);
    tfs_stat_t new_value = *value;
    new_value.st_atim.tv_sec = time(NULL);
    new_value.st_mtim.tv_sec = time(NULL);
    UpdateAttribute(ihandle, new_value);
  }

#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Release File Size: %d\n", GetAttribute(ihandle)->st_size);
#endif

  //TODO: Release Order?
  inode_cache->ReleaseInodeHandle(handle);
  inode_cache->EvictInodeHandle(ihandle->key_);

  if (ret < 0) {
    return FSError("Release file error\n");
  }
  return 0;
}

int TableFS::Truncate(const char *path, off_t new_size) {
  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    return FSError("Open: No such file or directory\n");
  }
  InodeCacheHandle* handle =
    inode_cache->GetInodeHandle(key, INODE_WRITE);
  if (handle != NULL) {
    InodeHandle* ihandle = inode_cache->Value(handle);
    const tfs_inode_header *value = GetInodeHeader(ihandle);

    if (value->fstat.st_size > state_->GetThreshold()) {
      if (new_size > state_->GetThreshold()) {
        TruncateDiskFile(value->fstat.st_ino, new_size);
      } else {
        char* buffer = new char[new_size];
        MigrateDiskFileToBuffer(value->fstat.st_ino, buffer, new_size);
        UpdateInlineData(ihandle, buffer, 0, new_size);
        delete [] buffer;
      }
    } else {
      if (new_size > state_->GetThreshold()) {
        MigrateToDiskFile(ihandle, value, O_TRUNC|O_WRONLY);
        ftruncate(ihandle->fd_, new_size);
        CloseDiskFile(ihandle->fd_);
      } else {
        TruncateInlineData(ihandle, new_size);
      }
    }
    if (new_size != value->fstat.st_size) {
      tfs_inode_header new_value = *value;
      new_value.fstat.st_size = new_size;
      UpdateInodeHeader(ihandle, new_value);
    }
    inode_cache->ReleaseInodeHandle(handle);
    inode_cache->EvictInodeHandle(ihandle->key_);
    return 0;
  }
  errno = ENOENT;
  return FSError("Open: No such file or directory\n");
}

int TableFS::Readlink(const char *path, char *buf, size_t size) {
  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    return FSError("Open: No such file or directory\n");
  }
  InodeCacheHandle* handle =
    inode_cache->GetInodeHandle(key, INODE_READ);
  if (handle != NULL) {
    InodeHandle* ihandle = inode_cache->Value(handle);
    int ret = GetInlineData(ihandle, buf, 0, size-1);
    inode_cache->ReleaseInodeHandle(handle);
//TODO:    inode_cache->EvictInodeHandle(key);
    if (ret > 0) {
      buf[ret] = '\0';
      return 0;
    }
  }
  errno = ENOENT;
  return FSError("Open: No such file or directory\n");
}

int TableFS::Symlink(const char *target, const char *path) {
  tfs_meta_key_t key;
  leveldb::Slice filename;
  if (!PathLookup(path, key, filename)) {
#ifdef  TABLEFS_DEBUG
    state_->GetLog()->LogMsg("Symlink: %s %s\n", path, target);
#endif
    return FSError("Symlink: No such parent file or directory\n");
  }

  int val_size = TFS_INODE_HEADER_SIZE+filename.size()+1+strlen(target);
  char* value = new char[val_size];
  tfs_inode_header* header = reinterpret_cast<tfs_inode_header*>(value);
  InitStat(header->fstat, state_->NewInode(), S_IFLNK, 0);
  header->namelen = filename.size();
  strncpy(header->buffer, filename.data(), filename.size());
  header->buffer[filename.size()] = '\0';
  strcpy(header->buffer+filename.size()+1, target);
  InodeCacheHandle* handle = inode_cache->InsertInodeHandle(key,
                         leveldb::Slice(value, val_size));
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Symlink: %08x %08x %08x %s %s\n",
                          key.inode_id, key.hash_id, handle,
                          header->buffer, header->buffer+filename.size()+1);
#endif
  if (handle != NULL) {
    inode_cache->ReleaseInodeHandle(handle);
    inode_cache->EvictInodeHandle(key);
  }
  delete [] value;
  return 0;
}

int TableFS::Unlink(const char *path) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Unlink: %s\n", path);
#endif

  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    return FSError("Unlink: No such file or directory\n");
  }

  InodeCacheHandle* handle = inode_cache->GetInodeHandle(key, INODE_DELETE);

  if (handle != NULL) {
    const tfs_inode_header *value = inode_cache->GetInodeHeader(handle);
    if (value->fstat.st_size > state_->GetThreshold()) {
      char fpath[128];
      GetDiskFilePath(fpath, value->fstat.st_ino);
      unlink(fpath);
      //TODO: if it fails to delete the file, do we need to delete the item in DB?
    }
    inode_cache->ReleaseInodeHandle(handle);
// TODO: How to avoid eviction?
    inode_cache->EvictInodeHandle(key);
    return 0;
  } else {
    return FSError("No such file or directory\n");
  }
}

//TODO: check file mode
int TableFS::MakeNode(const char *path, mode_t mode, dev_t dev) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("MakeNode: %s\n", path);
#endif

  tfs_meta_key_t key;
  leveldb::Slice filename;
  if (!PathLookup(path, key, filename)) {
    return FSError("MakeNode: No such parent file or directory\n");
  }

#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("MakeNode: %08x %08x %s\n", key.inode_id, key.hash_id, filename.data());
#endif

  int val_size = TFS_INODE_HEADER_SIZE+filename.size()+1;
  char* value = new char[val_size];
  tfs_inode_header* header = reinterpret_cast<tfs_inode_header*>(value);
  InitStat(header->fstat, state_->NewInode(), mode | S_IFREG, dev);
  header->namelen = filename.size();
  strncpy(header->buffer, filename.data(), filename.size());
  header->buffer[filename.size()] = '\0';

  InodeCacheHandle* handle = inode_cache->InsertInodeHandle(key,
                         leveldb::Slice(value, val_size));
  if (handle != NULL) {
    inode_cache->ReleaseInodeHandle(handle);
    inode_cache->EvictInodeHandle(key);
  }
  delete [] value;

  if (handle == NULL) {
    state_->GetLog()->LogMsg("MakeNode Error: %s Fail to Insert: %08x %08x\n",
                             path, key.inode_id, key.hash_id);
    return 1; //TODO: return the correct errno
  }
  return 0;
}

int TableFS::MakeDir(const char *path, mode_t mode) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("MakeDir: %s\n", path);
#endif

  tfs_meta_key_t key;
  leveldb::Slice filename;
  if (!PathLookup(path, key, filename)) {
    return FSError("MakeDir: No such parent file or directory\n");
  }

  int val_size = TFS_INODE_HEADER_SIZE+filename.size()+1;
  char* value = new char[val_size];
  tfs_inode_header* header = reinterpret_cast<tfs_inode_header*>(value);
  InitStat(header->fstat, state_->NewInode(), mode | S_IFDIR, 0);
  header->namelen = filename.size();
  strncpy(header->buffer, filename.data(), filename.size());
  header->buffer[filename.size()] = '\0';

//  TODO: Must lock to guaratee new file is written back to disk
  InodeCacheHandle* handle =
    inode_cache->InsertInodeHandle(key, leveldb::Slice(value, val_size));
  if (handle != NULL) {
    inode_cache->ReleaseInodeHandle(handle);
    inode_cache->EvictInodeHandle(key);
  }
  delete [] value;

  if (handle == NULL) {
    state_->GetLog()->LogMsg("MakeDir Error: %s Fail to Insert: %08x %08x\n",
                             path, key.inode_id, key.hash_id);
    return 1; //TODO: return the correct errno
  }
  return 0;
}

int TableFS::OpenDir(const char *path, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("OpenDir: %s\n", path);
#endif

  tfs_meta_key_t key;
  std::string inode;
  if (!PathLookup(path, key)) {
    return FSError("OpenDir: No such file or directory\n");
  }
  InodeCacheHandle* handle =
    inode_cache->GetInodeHandle(key, INODE_READ);
  if (handle != NULL) {
    fi->fh = (uint64_t) handle;
    return 0;
  } else {
    return FSError("OpenDir: No such file or directory\n");
  }
}

int TableFS::ReadDir(const char *path, void *buf, fuse_fill_dir_t filler,
                     off_t offset, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("ReadDir: %s\n", path);
#endif

  InodeCacheHandle* phandle = (InodeCacheHandle *) fi->fh;
  InodeHandle* pihandle = inode_cache->Value(phandle);
  tfs_meta_key_t childkey;
  childkey.inode_id = GetInodeHeader(pihandle)->fstat.st_ino;
  if (childkey.inode_id == ROOT_INODE_ID) {
    childkey.hash_id = 1;
  } else {
    childkey.hash_id = 0;
  }
  LevelDBAdaptor* metadb = state_->metadb;
  LevelDBIterator* iter = metadb->GetNewIterator();
  //TODO: Is it necessary to pass stat info?
  //TODO: May return deleted or old items?

  if (filler(buf, ".", NULL, 0) < 0) {
    return FSError("Cannot read a directory");
  }
  if (filler(buf, "..", NULL, 0) < 0) {
    return FSError("Cannot read a directory");
  }

  for (iter->Seek(childkey.ToSlice());
       iter->Valid() && IsKeyInDir(iter->key(), childkey);
       iter->Next()) {
    InodeCacheHandle *handle =
      inode_cache->GetInodeHandle(*((const tfs_meta_key_t *) iter->key().data()),
                                  INODE_READ);
    InodeHandle* ihandle = inode_cache->Value(handle);
    if (ihandle->mode_ == INODE_DELETE)
      continue;

    int ret = -1;
    if (handle != NULL) {
      const tfs_inode_header *value = inode_cache->GetInodeHeader(handle);
#ifdef  TABLEFS_DEBUG
      state_->GetLog()->LogMsg("ListDir: %s\n", value->buffer);
#endif
      ret = filler(buf, value->buffer, NULL, 0);
    }
    inode_cache->ReleaseInodeHandle(handle);
    if (ret < 0) {
      delete iter;
      return FSError("Cannot read a directory");
    }
  }
  delete iter;
  return 0;
}

int TableFS::ReleaseDir(const char *path, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("ReleaseDir: %s\n", path);
#endif

  InodeCacheHandle* handle = (InodeCacheHandle *) fi->fh;
  inode_cache->ReleaseInodeHandle(handle);
  return 0;
}

int TableFS::RemoveDir(const char *path) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("RemoveDir: %s\n", path);
#endif

  //TODO: Do we need to scan the whole table and delete the item?
  tfs_meta_key_t key;
  std::string inode;
  if (!PathLookup(path, key)) {
    return FSError("RemoveDir: No such file or directory\n");
  }
  InodeCacheHandle* handle =
      inode_cache->GetInodeHandle(key, INODE_DELETE);
  if (handle != NULL) {
    inode_cache->ReleaseInodeHandle(handle);
    inode_cache->EvictInodeHandle(key);
    return 0;
  } else {
    return FSError("RemoveDir: No such file or directory\n");
  }
}

int TableFS::Rename(const char *old_path, const char *new_path) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Rename: %s %s\n", old_path, new_path);
#endif

  tfs_meta_key_t old_key;
  if (!PathLookup(old_path, old_key)) {
    return FSError("No such file or directory\n");
  }
  tfs_meta_key_t new_key;
  leveldb::Slice filename;
  if (!PathLookup(new_path, new_key, filename)) {
    return FSError("No such file or directory\n");
  }
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Rename old_key: %08x %08x\n", old_key.inode_id, old_key.hash_id);
  state_->GetLog()->LogMsg("Rename new_key: %08x %08x\n", new_key.inode_id, new_key.hash_id);
#endif

  InodeCacheHandle* old_handle =
    inode_cache->GetInodeHandle(old_key, INODE_DELETE);
  InodeHandle* old_ihandle = inode_cache->Value(old_handle);

  std::string new_value;
  new_value.append(old_ihandle->value_, 0, TFS_INODE_ATTR_SIZE);
  uint32_t namelen = filename.size();
  new_value.append((char *) &namelen, sizeof(uint32_t));
  new_value.append(filename.data(), filename.size());
  new_value.push_back('\0');
  const tfs_inode_header *old_value = GetInodeHeader(old_ihandle);
  if (old_value->fstat.st_size > 0) {
    new_value.append(old_value->buffer+old_value->namelen+1, old_value->fstat.st_size);
  }

  InodeCacheHandle* new_handle =
    inode_cache->InsertInodeHandle(new_key, new_value);
  inode_cache->ReleaseInodeHandle(new_handle);
  inode_cache->EvictInodeHandle(new_key);
  inode_cache->ReleaseInodeHandle(old_handle);
  inode_cache->EvictInodeHandle(old_key);
  dentry_cache->Evict(old_key);
  dentry_cache->Evict(new_key);
  return 0;
}

int TableFS::Access(const char *path, int mask) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Access: %s %08x\n", path, mask);
#endif
  //TODO: Implement Access
  return 0;
}

int TableFS::UpdateTimens(const char *path, const struct timespec tv[2]) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("UpdateTimens: %s\n", path);
#endif

  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    return FSError("No such file or directory\n");
  }
  InodeCacheHandle* handle = inode_cache->GetInodeHandle(key, INODE_WRITE);
  if (handle != NULL) {
    InodeHandle* ihandle = inode_cache->Value(handle);
    const tfs_stat_t *value = GetAttribute(ihandle);
    tfs_stat_t new_value = *value;
    new_value.st_atim.tv_sec = tv[0].tv_sec;
    new_value.st_mtim.tv_sec = tv[1].tv_sec;
    UpdateAttribute(ihandle, new_value);
    inode_cache->ReleaseInodeHandle(handle);
    inode_cache->EvictInodeHandle(ihandle->key_);
    return 0;
  } else {
    return FSError("No such file or directory\n");
  }
}

int TableFS::Chmod(const char *path, mode_t mode) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Chmod: %s\n", path);
#endif

  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    return FSError("No such file or directory\n");
  }

  InodeCacheHandle* handle = inode_cache->GetInodeHandle(key, INODE_WRITE);
  if (handle != NULL) {
    InodeHandle* ihandle = inode_cache->Value(handle);
    const tfs_stat_t *value = GetAttribute(ihandle);
    tfs_stat_t new_value = *value;
    new_value.st_mode = mode;
    UpdateAttribute(ihandle, new_value);
    inode_cache->ReleaseInodeHandle(handle);
    inode_cache->EvictInodeHandle(ihandle->key_);
    return 0;
  } else {
    return FSError("No such file or directory\n");
  }
}

int TableFS::Chown(const char *path, uid_t uid, gid_t gid) {
#ifdef  TABLEFS_DEBUG
  state_->GetLog()->LogMsg("Chown: %s\n", path);
#endif

  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    return FSError("No such file or directory\n");
  }
  InodeCacheHandle* handle = inode_cache->GetInodeHandle(key, INODE_WRITE);
  if (handle != NULL) {
    InodeHandle* ihandle = inode_cache->Value(handle);
    const tfs_stat_t *value = GetAttribute(ihandle);
    tfs_stat_t new_value = *value;
    new_value.st_uid = uid;
    new_value.st_gid = gid;
    UpdateAttribute(ihandle, new_value);
    inode_cache->ReleaseInodeHandle(handle);
    inode_cache->EvictInodeHandle(ihandle->key_);
    return 0;
  } else {
    return FSError("No such file or directory\n");
  }
}

bool TableFS::GetStat(std::string stat, std::string* value) {
  return state_->GetMetaDB()->GetStat(stat, value);
}

}
