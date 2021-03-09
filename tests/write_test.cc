#include <stdio.h>
#include <stdlib.h>
#include <string>
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h> 

#define FILE_PATH "/home/lzw/fs/mnt/aa/abcd"

#define BLOCK_SIZE 1024
#define WRITE_COUNT 10

using namespace std;

int main(int argc, char *argv[])
{
    int fd = open(FILE_PATH, O_CREAT | O_RDWR);
    if(fd < 0){
        printf("open error:%s fd:%d\n",FILE_PATH, ret);
        return 0;
    }
    char buf[BLOCK_SIZE];
    memset(buf, '1', BLOCK_SIZE);
    int ret = 0;
    int offset = 0;
    for(int i = 0; i < WRITE_COUNT; i++){
        ret = pwrite(fd, buf, BLOCK_SIZE, offset);
        if(ret < 0){
            printf("write error:%d %d ret:%d\n",i, offset, ret);
        }
        offset += BLOCK_SIZE;
    }
    return 0;
}