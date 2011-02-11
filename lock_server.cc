// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <iostream>
#include <pthread.h>
#include <utility>
#include <assert.h>
using namespace std;

lock_server::lock_server(): lock_table()
{
  pthread_mutex_init(&table_mutex, NULL);
}

lock_server::~lock_server(){
  lock_table.~map();
  pthread_mutex_destroy(&table_mutex);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  ScopedLock sl(&table_mutex);
  table_entry_t& lock = lock_table[lid];
  r = lock.nacquire;
  return lock_protocol::OK;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  ScopedLock sl(&table_mutex); 
  table_entry_t& lock = lock_table[lid];

  while(lock.locked){
    pthread_cond_wait(&lock.cond_var, &table_mutex);
  }

    lock.nacquire++;
    lock.locked = true;
    lock.clientid = clt;
  
    return lock_protocol::OK;
 }
 
lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  ScopedLock sl(&table_mutex);
  table_entry_t& lock = lock_table[lid];

  assert(lock.locked && lock.clientid == clt);

  lock.locked = false;
  pthread_cond_signal(&lock.cond_var);
  
  return lock_protocol::OK;
}
