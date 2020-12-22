/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 15:06:00
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_DEBUG_H_
#define _METADB_DEBUG_H_

#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace metadb {


#define ERROR_PRINT(format, a...) \
do{ \
    printf("ERROR:[%s][%s][%d]: " #format"\n", __FILE__, __FUNCTION__, __LINE__, ##a); \
    DBG_LOG(format, a...) \
}while(0)


#ifdef NDEBUG

#define DBG_LOG(format, a...)
#define DBG_PRINT(format, a...)

#else

#define LOG_FILE_PATH "log.log"

static inline unsigned int get_tid(){
#ifdef __APPLE__
    return (unsigned int) pthread_self();
#else
    return (unsigned int) syscall(__NR_gettid);
#endif

}

class DebugLogger{
public:
    DebugLogger(char *log_file_path) {
        fp = fopen(log_file_path,"w+");
        if(fp == nullptr){
            ERROR_PRINT("can't create log file!");
            exit(-1);
        }
    }

    static DebugLogger* GetInstance(){
        if(log_ == nullptr)
        {
            log_ = new DebugLogger(LOG_FILE_PATH);
        }
        return log_;
    }

    FILE* GetFp() { return fp; }

protected:
    static DebugLogger* log_;
    FILE *fp;
    
};

DebugLogger* DebugLogger::log_ = nullptr;

#define DBG_LOG(format, a...) \
do{ \
    fprint(metadb::DebugLogger::GetInstance()->GetFp, "[%-18s][%4d][%5d]: " #format"\n", __FUNCTION__, __LINE__, get_tid(), ##a); \
}while(0)

#define DBG_PRINT(format, a...) \
do{ \
    printf("[%-18s][%4d][%5d]: " #format"\n", __FUNCTION__, __LINE__, get_tid(), ##a); \
}while(0)

#endif

}

#endif