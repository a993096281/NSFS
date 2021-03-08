/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-02-22 15:29:12
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _TABLEFS_TABLEFS_H_
#define _TABLEFS_TABLEFS_H_

#define FUSE_USE_VERSION 35

#include <fuse3/fuse.h>
#include "inode_format.h"
#include "adaptor.h"
#include "config.h"
#include "leveldb/db.h"



namespace tablefs {

using namespace std;
class TableFS {
public :
    TableFS(const kvfs_args & arg);
    virtual ~TableFS();

    void * Init(struct fuse_conn_info * conn, struct fuse_config *cfg);

    void Destroy(void * data);

    int GetAttr(const char * path ,struct stat * statbuff, struct fuse_file_info *fi);

    int Open(const char * path ,struct fuse_file_info * fi);

    int Read(const char * path,char * buf , size_t size ,off_t offset ,struct fuse_file_info * fi);

    int Write(const char * path , const char * buf,size_t size ,off_t offset ,struct fuse_file_info * fi);

    int Truncate(const char * path ,off_t offset, struct fuse_file_info *fi);

    int Fsync(const char * path,int datasync ,struct fuse_file_info * fi);

    int Release(const char * path ,struct fuse_file_info * fi);

    int Readlink(const char * path ,char * buf,size_t size);

    int Symlink(const char * target , const char * path);

    int Unlink(const char * path);

    int MakeNode(const char * path,mode_t mode ,dev_t dev);

    int MakeDir(const char * path,mode_t mode);

    int OpenDir(const char * path,struct fuse_file_info *fi);

    int ReadDir(const char * path,void * buf ,fuse_fill_dir_t filler,
            off_t offset ,struct fuse_file_info * fi, enum fuse_readdir_flags flag);

    int ReleaseDir(const char * path,struct fuse_file_info * fi);

    int RemoveDir(const char * path);

    int Rename(const char *new_path,const char * old_path, unsigned int flags);
    
    int Access(const char * path,int mask);

    int Chmod(const char * path , mode_t mode, struct fuse_file_info *fi);
    
    int Chown(const char * path, uid_t uid,gid_t gid, struct fuse_file_info *fi);

    int UpdateTimens(const char * path ,const struct timespec tv[2], struct fuse_file_info *fi);

protected:
    void FreeInodeValue(tfs_inode_val_t &ival);
    inline void InitStat(struct stat &statbuf, tfs_inode_t inode, mode_t mode, dev_t dev);

    tfs_inode_val_t InitInodeValue(tfs_inode_t inum, mode_t mode, dev_t dev, leveldb::Slice filename);
    std::string InitInodeValue(const std::string& old_value, leveldb::Slice filename);
                
    bool ParentPathLookup(const char* path, tfs_meta_key_t &key, tfs_inode_t &inode_in_search, const char* &lastdelimiter);

    inline bool PathLookup(const char *path, tfs_meta_key_t &key, leveldb::Slice &filename);

    inline bool PathLookup(const char *path, tfs_meta_key_t &key);

    kvfs_file_handle * InitFileHandle(const char * path, struct fuse_file_info * fi, const tfs_meta_key_t & key , const std::string & value );
    string GetDiskFilePath(const tfs_meta_key_t &key, tfs_inode_t inode_id);
    int OpenDiskFile(const tfs_meta_key_t &key, const tfs_inode_header* iheader, int flags);
    int TruncateDiskFile(const tfs_meta_key_t &key, tfs_inode_t inode_id, off_t new_size);
    ssize_t MigrateDiskFileToBuffer(const tfs_meta_key_t &key, tfs_inode_t inode_id, char* buffer,size_t size);
    int MigrateToDiskFile(const tfs_meta_key_t &key, string &value, int &fd, int flags);


    DBAdaptor * db_;
    KVFSConfig * config_;
    kvfs_args args_;
    bool use_fuse;

    struct fuse_config *cfg_;

};


} // namespace name







#endif