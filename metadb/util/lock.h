/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-14 14:31:40
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_LOCK_H_
#define _METADB_LOCK_H_

#include <pthread.h>

namespace metadb {

class Mutex {
public:
    Mutex(){
        pthread_mutex_init(&mu_, NULL);
    };

    ~Mutex(){
        pthread_mutex_destroy(&mu_);
    };

    void Lock(){
        pthread_mutex_lock(&mu_); 

    };

    void Unlock(){
        pthread_mutex_unlock(&mu_);
    };

private:
      pthread_mutex_t mu_;

};


// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class MutexLock {
 public:
  explicit MutexLock(Mutex *mu)
      : mu_(mu)  {
    this->mu_->Lock();
  }

  ~MutexLock() { this->mu_->Unlock(); }

 private:
  Mutex *const mu_;
  // No copying allowed
  MutexLock(const MutexLock&) = delete;
  void operator=(const MutexLock&) = delete;
};


} // namespace name








#endif