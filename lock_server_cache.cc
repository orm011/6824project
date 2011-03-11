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
  printf("FREE %d, LOCKED %d, REVOKING %d, WAITING %d\n",FREE, LOCKED, REVOKING, WAITING);
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
 switch (mylock.state){
 case FREE:
   assert(mylock.owner == "" && mylock.waiting_clients.empty());
   mylock.owner = id;
   mylock.state = LOCKED;
   pthread_mutex_unlock(&mylock.mutex);
   return lock_protocol::OK;
 case LOCKED:

   {
   assert(mylock.owner != "" && mylock.waiting_clients.empty());
   mylock.waiting_clients.push_back(id);
   mylock.state = REVOKING;

   string current_owner = mylock.owner;
   handle::handle h(mylock.owner);
   pthread_mutex_unlock(&mylock.mutex);

   rpcc *cl = h.safebind();
    
     if (cl){
       int r, ret;
       printf("(server) calling revoke on client %s, lock %llu",current_owner.c_str(), lid);
       ret = cl->call(rlock_protocol::revoke, lid, r);
       VERIFY(ret == rlock_protocol::OK);
     } else {
       VERIFY(0);
     }

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
     //my turn 
     mylock.waiting_clients.pop_front();
     mylock.owner = id;
     if (mylock.waiting_clients.size() == 0)
       {
	 mylock.state = LOCKED;
	 pthread_mutex_unlock(&mylock.mutex);
	 return lock_protocol::OK;
       }
     else  
       {
	 mylock.state = REVOKING;
	 //must send revoke to calling client (same who is to be granted the lock)
	 handle::handle h(id);
     
	 string owner = id;
	 pthread_mutex_unlock(&mylock.mutex);

	 rpcc *cl = h.safebind();
	 if (cl){
	   int r, ret;
	   printf("(server) calling revoke on client %s, lock %llu",owner.c_str(), lid);
	   ret = cl->call(rlock_protocol::revoke, lid, r);
	   VERIFY(ret == rlock_protocol::OK);
	 } else {
	   VERIFY(0);
	 }
	 return lock_protocol::OK;
       }

   } else 
     {
       //not my turn yet so assert only one acquire exists from client
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

  tprintf("state at release: %d\n", mylock.state);
  assert(id == mylock.owner);
  switch(mylock.state){
  case FREE:
    VERIFY(mylock.owner == "");
    VERIFY(0);

  case LOCKED:
    VERIFY(mylock.waiting_clients.empty()); 
    mylock.state = FREE;
    mylock.owner = "";
    pthread_mutex_unlock(&mylock.mutex);
    return lock_protocol::OK;

  case REVOKING:
    {
      assert(!mylock.waiting_clients.empty());

      tprintf("queue state:\n");
      deque<string>::iterator it;
      for (it = mylock.waiting_clients.begin(); it != mylock.waiting_clients.end(); it++){
	printf("%s\n", it->c_str());
      }
      printf("\n");

      VERIFY(id == mylock.owner);
      mylock.state = WAITING;
      mylock.owner = "";

      //call retry() on next in queue
      handle::handle h(*mylock.waiting_clients.begin());
      pthread_mutex_unlock(&mylock.mutex);

      rpcc *cl = h.safebind();

      //TODO: WARNING. too tired when i wrote this
      if (cl){
	int r, ret;
	printf("about to call...\n");
	ret = cl->call(rlock_protocol::retry, lid, r);
	printf("retry called\n");
	VERIFY(ret == rlock_protocol::OK);
      } else {
	VERIFY(0);
      }
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
