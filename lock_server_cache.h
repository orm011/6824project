#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <deque>

#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include "handle.h"
#include <deque>

using namespace std;

class lock_server_cache {

 private:
  enum st {FREE, LOCKED, REVOKING, WAITING};
  typedef struct lock {
    st state;
    string owner;
    deque<string> waiting_clients;
    pthread_cond_t cond;
    pthread_mutex_t mutex;

  lock(): state(FREE),owner(""),waiting_clients(){
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
  }

  } lock_t;

  map<lock_protocol::lockid_t, lock_t> lock_table;
  
  pthread_mutex_t lock_table_mutex;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
