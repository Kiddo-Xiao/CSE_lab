// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
using namespace std;

class lock_server {

 protected:
  int nacquire;
  pthread_mutex_t mtx;
  map<lock_protocol::lockid_t, bool> locks;//文档：考虑使用 C++ STL（标准模板库）std::map 类来保存锁状态表。
  map<lock_protocol::lockid_t, pthread_cond_t> cond_variables;//条件变量
  //文档：Use a simple mutex scheme: a single pthreads mutex for all of lock_server. 

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 