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
  VERIFY(pthread_mutex_init(&lock_table_mutex, NULL) == 0);
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  //  tprintf("(server) %s wishes to acquire lock %llx\n", id.c_str(), lid);
  
  pthread_mutex_lock(&lock_table_mutex);
  lock_t &mylock = lock_table[lid];
  pthread_mutex_unlock(&lock_table_mutex);

  pthread_mutex_lock(&mylock.mutex);
 switch (mylock.state){
 case FREE:
   //   tprintf("(server) %s, %llu. acquire() case FREE\n",  id.c_str(), lid);
   assert(mylock.owner == "" && mylock.waiting_clients.empty());
   mylock.owner = id;
   mylock.state = LOCKED;
   pthread_mutex_unlock(&mylock.mutex);
   return lock_protocol::OK;
 case LOCKED:
   {
     //tprintf("(server) %s, %llu. acquire() case LOCKED\n", id.c_str(), lid);
     assert(mylock.owner != "" && mylock.waiting_clients.empty());
     mylock.waiting_clients.push_back(id);
     mylock.state = REVOKING;

     string current_owner = mylock.owner;
     handle::handle h(mylock.owner);
     pthread_mutex_unlock(&mylock.mutex);

     rpcc *cl = h.safebind();
    
     if (cl){
       int r;
       //tprintf("(server) at acquire(), calling revoke on client %s, lock %llu\n", current_owner.c_str(), lid);
       VERIFY(cl->call(rlock_protocol::revoke, lid, r) == rlock_protocol::OK);
     } else {
       VERIFY(0);
     }

   }
   return lock_protocol::RETRY;
 case REVOKING:
   //   tprintf("(server) %s, %llu. acquire() case REVOKING\n", id.c_str(), lid);
   assert(mylock.owner != "" && !mylock.waiting_clients.empty());
   mylock.waiting_clients.push_back(id);
   mylock.state = REVOKING;
   pthread_mutex_unlock(&mylock.mutex);
   return lock_protocol::RETRY;

 case WAITING:
   //   tprintf("(server) %s, %llu. acquire() case WAITING\n", id.c_str(), lid);
   assert(mylock.owner == "" && mylock.waiting_clients.size() > 0);
   if (*mylock.waiting_clients.begin() == id){
     //my turn 
     mylock.waiting_clients.pop_front();
     mylock.owner = id;
     if (mylock.waiting_clients.size() == 0)
       {
	 //	 tprintf("(server) %s, %llu. acquire() case WAITING -- first with no other waiters\n", id.c_str(), lid);
	 mylock.state = LOCKED;
	 pthread_mutex_unlock(&mylock.mutex);
	 return lock_protocol::OK;
       }
     else  
       {
	 //	 tprintf("(server) %s, %llu. acquire() case WAITING -- first in the list but other waiters pending\n", id.c_str(), lid);
	 mylock.state = REVOKING;
	 //must send revoke to calling client (same who is to be granted the lock)
	 handle::handle h(id);
     
	 //string owner = id;
	 pthread_mutex_unlock(&mylock.mutex);

	 rpcc *cl = h.safebind();
	 if (cl){
	   int r;
	   //tprintf("(server) calling revoke on  client %s, lock %llu right at its own  acquire()\n", owner.c_str(), lid);
	   VERIFY(cl->call(rlock_protocol::revoke, lid, r) == rlock_protocol::OK);
	 } else {
	   VERIFY(0);
	 }
	 return lock_protocol::OK;
       }

   } else 
     {
       //       tprintf("(server) %s, %llu. acquire() case WAITING -- not first in the list\n", id.c_str(), lid);
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
  // tprintf("(server) %s wishes to release lock %llx\n", id.c_str(), lid);

  pthread_mutex_lock(&lock_table_mutex);
  lock_t &mylock = lock_table[lid];
  pthread_mutex_unlock(&lock_table_mutex);

  pthread_mutex_lock(&mylock.mutex);
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
      
      //tprintf("queue size: %u, state:\n", mylock.waiting_clients.size());
      // deque<string>::iterator it;
      // for (it = mylock.waiting_clients.begin(); it != mylock.waiting_clients.end(); it++){
      // 	printf("%s --- ", it->c_str());
      // }
      // printf("\n");

      VERIFY(id == mylock.owner);
      mylock.state = WAITING;
      mylock.owner = "";

      //call retry() on next in queue
      handle::handle h(*mylock.waiting_clients.begin());
      string tocall = *mylock.waiting_clients.begin();
      pthread_mutex_unlock(&mylock.mutex);

      rpcc *cl = h.safebind();

      if (cl){
	int r;
	//tprintf("(server) release() at case REVOKING to call retry() on client %s for lid: %llu\n", tocall.c_str(), lid);
	VERIFY(cl->call(rlock_protocol::retry, lid, r) == rlock_protocol::OK);
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
