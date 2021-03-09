/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-02-22 15:55:03
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#ifndef _NSFS_CONFIG_H_
#define _NSFS_CONFIG_H_

#include <stdint.h>
#include <string>
#include <unordered_map>
#include <sys/stat.h>

namespace nsfs{

using kvfs_args = std::unordered_map <std::string, std::string >;

class KVFSConfig {

public :
    KVFSConfig();
    void Init(const kvfs_args & args);
    inline bool IsEmpty() { 
        return 0 == current_inode_size;  }
    inline std::string GetDataDir()
    {
        return data_dir;
    }
    inline std::string GetMetaDir()
    {
        return meta_dir;
    }
    inline std::string GetMountDir()
    {
        return mount_dir;
    }
    inline uint64_t GetThreshold() 
    {
        return threshold;
    }
    inline uint64_t NewInode(){
        ++current_inode_size;
        return current_inode_size;
    }

protected:

    // const path
    static const std::string config_file_name;

    // dirs 
    std::string meta_dir;
    std::string data_dir;
    std::string mount_dir;
    uint64_t current_inode_size;

    uint64_t threshold;



};

}

#endif // KVFS_CONFIG_HPP
