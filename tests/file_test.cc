#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h> 
#include <unistd.h>
#include <sys/time.h>
#include <stdint.h>


#define DIR_PATH "/pmem0/fs/mnt"

#define FILE_SIZE (16 * 1024)
#define OP_COUNT 100000
#define DIR_DEPTH 1

#define ALIGN_SIZE 4096
#define MOD_NUM 10000000

using namespace std;

static inline uint64_t Random64(uint32_t *seed){
    return (((uint64_t)rand_r(seed)) << 32 ) | (uint64_t)rand_r(seed);
}

static inline uint64_t get_now_micros(){
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec) * 1000000 + tv.tv_usec;
}

string GetDirPath(){
    if(DIR_DEPTH <= 1){
        return string(DIR_PATH).append("/");
    }
    string dpath(DIR_PATH);
    for(int i = 1; i < DIR_DEPTH; i++){
        dpath.append("/").append("000001");
        if(access(dpath.c_str(), F_OK) != 0){
            if( mkdir(dpath.c_str(), 0755) != 0){
                printf("mkdir error! dpath:%s",dpath.c_str());
                exit(1);
            }
        }
    }
    dpath.append("/");
    return dpath;
}

void creatfiles(){
    char *buf;
    int buf_size = FILE_SIZE >= ALIGN_SIZE ? FILE_SIZE : ALIGN_SIZE;
    int ret = posix_memalign((void **)&buf, ALIGN_SIZE, buf_size);
    if(ret != 0){
        printf("alloc buf error! ret:%d",ret);
        exit(1);
    }
    memset(buf, '1', buf_size);
    string dpath = GetDirPath();
    uint32_t seed = 123;
    uint64_t id;
    char file_name[100];
    for(int i = 0; i < OP_COUNT; i++){
        //id = Random64(&seed) % MOD_NUM;
        id = i;
        snprintf(file_name, sizeof(file_name), "%06u", id);
        string fpath = dpath;
        fpath.append(file_name);

        int fd = open(fpath.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_DIRECT);
        if(fd < 0){
            printf("open file:%s error fd:%d", fpath.c_str(), fd);
            exit(1);
        }
        ret = pwrite(fd, buf, FILE_SIZE, 0);
        if(ret != FILE_SIZE){
            printf("write file:%s error ret:%d filesize:%d", fpath.c_str(), ret, FILE_SIZE);
            exit(1);
        }
        close(fd);
    }

}

void deletefiles(){
    string dpath = GetDirPath();
    uint32_t seed = 123;
    uint64_t id;
    char file_name[100];
    for(int i = 0; i < OP_COUNT; i++){
        //id = Random64(&seed) % MOD_NUM;
        id = i;
        snprintf(file_name, sizeof(file_name), "%06u", id);
        string fpath = dpath;
        fpath.append(file_name);

        if(unlink(fpath.c_str()) != 0){
            printf("delete file:%s error!", fpath.c_str());
            exit(1);
        }
        
    }
}

void createfilesbylen(int len){
    char *buf;
    int buf_size = len >= ALIGN_SIZE ? len : ALIGN_SIZE;
    int ret = posix_memalign((void **)&buf, ALIGN_SIZE, buf_size);
    if(ret != 0){
        printf("alloc buf error! ret:%d",ret);
        exit(1);
    }
    memset(buf, '1', buf_size);
    string dpath = GetDirPath();
    uint32_t seed = 123;
    uint64_t id;
    char file_name[100];
    for(int i = 0; i < OP_COUNT; i++){
        //id = Random64(&seed) % MOD_NUM;
        id = i;
        snprintf(file_name, sizeof(file_name), "%06u", id);
        string fpath = dpath;
        fpath.append(file_name);

        int fd = open(fpath.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_DIRECT);
        if(fd < 0){
            printf("open file:%s error fd:%d", fpath.c_str(), fd);
            exit(1);
        }
        if(len > 0){
            ret = pwrite(fd, buf, len, 0);
            if(ret != len){
                printf("write file:%s error ret:%d filesize:%d", fpath.c_str(), ret, len);
                exit(1);
            }
        }
        
        close(fd);
    }
}

void read_files(){
    char *buf;
    int buf_size = FILE_SIZE >= ALIGN_SIZE ? FILE_SIZE : ALIGN_SIZE;
    int ret = posix_memalign((void **)&buf, ALIGN_SIZE, buf_size);
    if(ret != 0){
        printf("alloc buf error! ret:%d",ret);
        exit(1);
    }
    string dpath = GetDirPath();
    uint32_t seed = 123;
    uint64_t id;
    char file_name[100];
    for(int i = 0; i < OP_COUNT; i++){
        //id = Random64(&seed) % MOD_NUM;
        id = i;
        snprintf(file_name, sizeof(file_name), "%06u", id);
        string fpath = dpath;
        fpath.append(file_name);

        int fd = open(fpath.c_str(), O_RDONLY | O_DIRECT);
        if(fd < 0){
            printf("open file:%s error fd:%d", fpath.c_str(), fd);
            exit(1);
        }
        
        ret = pread(fd, buf, FILE_SIZE, 0);
        if(ret != FILE_SIZE){
            printf("read file:%s error ret:%d filesize:%d", fpath.c_str(), ret, FILE_SIZE);
            exit(1);
        }
        
        close(fd);
    }
}

void write_files(){
    char *buf;
    int buf_size = FILE_SIZE >= ALIGN_SIZE ? FILE_SIZE : ALIGN_SIZE;
    int ret = posix_memalign((void **)&buf, ALIGN_SIZE, buf_size);
    if(ret != 0){
        printf("alloc buf error! ret:%d",ret);
        exit(1);
    }
    memset(buf, '1', buf_size);
    string dpath = GetDirPath();
    uint32_t seed = 123;
    uint64_t id;
    char file_name[100];
    for(int i = 0; i < OP_COUNT; i++){
        //id = Random64(&seed) % MOD_NUM;
        id = i;
        snprintf(file_name, sizeof(file_name), "%06u", id);
        string fpath = dpath;
        fpath.append(file_name);

        int fd = open(fpath.c_str(), O_TRUNC | O_WRONLY | O_DIRECT);
        if(fd < 0){
            printf("open file:%s error fd:%d", fpath.c_str(), fd);
            exit(1);
        }
        
        ret = pwrite(fd, buf, FILE_SIZE, 0);
        if(ret != FILE_SIZE){
            printf("write file:%s error ret:%d filesize:%d", fpath.c_str(), ret, FILE_SIZE);
            exit(1);
        }
        
        close(fd);
    }
}

void open_files(){
    string dpath = GetDirPath();
    uint32_t seed = 123;
    uint64_t id;
    char file_name[100];
    for(int i = 0; i < OP_COUNT; i++){
        //id = Random64(&seed) % MOD_NUM;
        id = i;
        snprintf(file_name, sizeof(file_name), "%06u", id);
        string fpath = dpath;
        fpath.append(file_name);

        int fd = open(fpath.c_str(), O_RDONLY | O_DIRECT);
        if(fd < 0){
            printf("open file:%s error fd:%d", fpath.c_str(), fd);
            exit(1);
        }
        close(fd);
    }
}

int main(int argc, char *argv[])
{
    createfilesbylen(16384);
    //createfilesbylen(0);

    uint64_t start_time = get_now_micros();

    //creatfiles();
    deletefiles();
    //read_files();
    //write_files();
    //open_files();

    uint64_t end_time = get_now_micros();

    uint64_t use_time = end_time - start_time;

    printf("------Config------\n");
    printf("DIR_PATH:%s FILE_SIZE:%d OP_COUNT:%d DIR_DEPTH:%d\n", DIR_PATH, FILE_SIZE, OP_COUNT, DIR_DEPTH);
    printf("------------------\n");
    printf("use_time:%u us iops:%.2f\n", use_time, 1.0 * OP_COUNT * 1e6 / use_time);
    
    return 0;
}