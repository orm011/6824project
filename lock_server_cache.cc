// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

lock_server_cache::lock_server_cache(): lock_table()
{
  tprintf("starting server\n");
  VERIFY(pthread_mutex_init(&lock_table_mutex, NULL) == 0);
  
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  tprintf("acquire of lock %llx by %s\n", lid, id.c_str());
  
  pthread_mutex_lock(&lock_table_mutex);
  lock_t &mylock = lock_table[lid];
  pthread_mutex_unlock(&lock_table_mutex);

  pthread_mutex_lock(&mylock.mutex);

  ///TODO: do i need 2? make sure to bind correctly
  handle::handle h1(mylock.owner);
  rpcc *cl1 = h1.safebind();

  handle::handle h2(*mylock.waiting_clients.begin());
  rpcc *cl2 = h2.safebind();

  
 switch (mylock.state){
 case FREE:
   assert(mylock.owner == "" && mylock.waiting_clients.empty());
   mylock.owner = id;
   mylock.state = LOCKED;
   pthread_mutex_unlock(&mylock.mutex);
   return lock_protocol::OK;
 case LOCKED:
   assert(mylock.owner != "" && mylock.waiting_clients.empty());
   mylock.waiting_clients.push_back(id);
   mylock.state = REVOKING;
   pthread_mutex_unlock(&mylock.mutex);
    
   if (cl1){
     int r, ret;
     ret = cl1->call(rlock_protocol::revoke, lid, r);
     VERIFY(ret == rlock_protocol::OK);
   } else {
     VERIFY(0);
   }
   return lock_protocol::RETRY;

 case REVOKING:
   assert(mylock.owner != "" && !mylock.waiting_clients.empty());
   mylock.waiting_clients.push_back(id);
   mylock.state = REVOKING;
   pthread_mutex_unlock(&mylock.mutex);
   return lock_protocol::RETRY;

 case WAITING:
   assert(mylock.owner == "" && mylock.waiting_clients.size() > 0);
   if (*mylock.waiting_clients.begin() == id){
     mylock.waiting_clients.pop_front();
     mylock.owner = id;
     mylock.state = REVOKING;
     pthread_mutex_unlock(&mylock.mutex);

     if (cl2){
       //TODO: make sure to send to correct client
       int r, ret;
       ret = cl2->call(rlock_protocol::revoke, lid, r);
       VERIFY(ret == rlock_protocol::OK);
     } else {
       VERIFY(0);
     }
     return lock_protocol::OK;
   } else {
     //assert only one acquire exists from client
     deque<string>::iterator it;
     for(it = mylock.waiting_clients.begin(); it != mylock.waiting_clients.end(); it++){
       assert(*it != id);
     }
     mylock.waiting_clients.push_back(id);
     pthread_mutex_unlock(&mylock.mutex);
     return lock_protocol::RETRY;
   }

 default:
      VERIFY(0);   
 }

}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{


  tprintf("release of lock %llx by %s\n", lid, id.c_str());
  pthread_mutex_lock(&lock_table_mutex);
  lock_t &mylock = lock_table[lid];
  pthread_mutex_unlock(&lock_table_mutex);

  pthread_mutex_lock(&mylock.mutex);

  //may need to send messages to head of queue.
  //make sure this is well done
  handle::handle h(*mylock.waiting_clients.begin());
  rpcc *cl = h.safebind();

  switch(mylock.state){
  case FREE:
    VERIFY(0);

  case LOCKED:
    assert(mylock.waiting_clients.empty());
    VERIFY(id == mylock.owner);
    mylock.state = FREE;
    mylock.owner = "";
    pthread_mutex_unlock(&mylock.mutex);
    return lock_protocol::OK;

  case REVOKING:
    assert(!mylock.waiting_clients.empty());
    VERIFY(id == mylock.owner);
    mylock.state = WAITING;
    mylock.owner = "";
    pthread_mutex_unlock(&mylock.mutex);

    //TODO: WARNING. too tired when i wrote this
    if (cl){
      int r, ret;
      ret = cl->call(rlock_protocol::retry, lid, r);
      VERIFY(ret == rlock_protocol::OK);
    } else {
      VERIFY(0);
    }
    
    return lock_protocol::OK;

  case WAITING:
    VERIFY(0);

  default:
    VERIFY(0);
  }
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = 0;
  return lock_protocol::OK;
}
