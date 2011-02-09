// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <map>
#include <pthread.h>

using namespace std;

class lock_server {

 protected:
  typedef struct lock_table_entry {
    int nacquire;
    bool locked;
    int clientid;
    pthread_cond_t cond_var;

    lock_table_entry(){
      nacquire = 0;
      locked = false;
      pthread_cond_init(&cond_var, NULL);
    }

    ~lock_table_entry(){
      pthread_cond_destroy(&cond_var);
    }

  } table_entry_t;
  

  std::map<lock_protocol::lockid_t, table_entry_t> lock_table;
  pthread_mutex_t table_mutex;

 public:
  lock_server();
  ~lock_server();
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 
