// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  mtx = PTHREAD_MUTEX_INITIALIZER;
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mtx);//锁住
  if(locks.find(lid) == locks.end()){
      printf("acquire for the first time! %lld",lid);
      cond_variables[lid] = PTHREAD_COND_INITIALIZER;
  }else if(locks[lid]==true){
    while(locks[lid]==true)pthread_cond_wait(&cond_variables[lid], &mtx);
  }
  locks[lid] = true;//更新锁状态
  pthread_mutex_unlock(&mtx);//解锁
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mtx);//锁住
  locks[lid] = false;//更新锁状态
  pthread_cond_signal(&cond_variables[lid]);
  pthread_mutex_unlock(&mtx);//解锁
  return ret;
}