// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{

  pthread_mutex_lock(&local_table_mutex);
  local_lock_t  &lock_delegate = local_lock_table[lid];
  pthread_mutex_unlock(&local_table_mutex);

  pthread_mutex_lock(&lock_delegate.protecting_mutex);

  switch(lock_delegate.state){
  case NONE:
    assert(lock_delegate.waiting == 0);
    lock_delegate.state = ACQUIRING;
    pthread_mutex_unlock(&lock_delegate.protecting_mutex);
    
    int r, answer;
    answer = cl->call(lock_protocol::acquire, lid, id, r);
    
    if (answer == lock_protocol::RETRY){
      //retry() method must set this up in case ACQUIRING.
      while (!lock_delegate.retry_call_received){
	pthread_mutex_lock(&lock_delegate.protecting_mutex);
	pthread_cond_wait(&lock_delegate.cond, &lock_delegate.protecting_mutex);
	pthread_mutex_unlock(&lock_delegate.protecting_mutex);
      }
      answer = cl->call(lock_protocol::acquire, lid, id, r);
    }
    
    VERIFY(answer == lock_protocol::OK);    
    pthread_mutex_lock(&lock_delegate.protecting_mutex); 
    if (lock_delegate.state == ACQUIRING)
      lock_delegate.state = LOCKED;
    else if (lock_delegate.state != RELEASING)
      VERIFY(0);

    pthread_mutex_unlock(&lock_delegate.protecting_mutex);
    return lock_protocol::OK; 

  case ACQUIRING:
  case LOCKED:
  case RELEASING: 
    lock_delegate.waiting++;

    while (lock_delegate.state != FREE){
      pthread_cond_wait(&lock_delegate.cond, &lock_delegate.protecting_mutex);
    }
   
    lock_delegate.state = LOCKED;
    //    lock_delegate.holder = pthread_self();
    lock_delegate.waiting--;
    pthread_mutex_unlock(&lock_delegate.protecting_mutex);
    return lock_protocol::OK;

  case FREE:
    lock_delegate.state = LOCKED;
    //    lock_delegate.holder = pthread_self();
    pthread_mutex_unlock(&lock_delegate.protecting_mutex);
    return lock_protocol::OK;

  default:
    VERIFY(0); 
  }

}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&local_table_mutex);
  local_lock_t  &lock_delegate = local_lock_table[lid];
  pthread_mutex_unlock(&local_table_mutex);

  pthread_mutex_lock(&lock_delegate.protecting_mutex);

  switch (lock_delegate.state) {
  case NONE:
    VERIFY(0);
  case ACQUIRING:
    VERIFY(0);
  case LOCKED:
    lock_delegate.state = FREE;
    pthread_cond_signal(&lock_delegate.cond);
    pthread_mutex_unlock(&lock_delegate.protecting_mutex);
    return lock_protocol::OK;
  case FREE:
    VERIFY(0);
  case RELEASING:
    if (lock_delegate.waiting == 0){
      lock_delegate.state = NONE;
    } else if (lock_delegate.waiting > 0){
      lock_delegate.state = ACQUIRING;
      pthread_mutex_unlock(&lock_delegate.protecting_mutex);

      int r, ans;

      ans = cl->call(lock_protocol::release, lid, id, r);
      VERIFY(ans == lock_protocol::OK);
     
      ans = cl->call(lock_protocol::acquire, lid, id, r);
      if (ans == lock_protocol::RETRY){
	while (!lock_delegate.retry_call_received){
	pthread_mutex_lock(&lock_delegate.protecting_mutex);
	pthread_cond_wait(&lock_delegate.cond, &lock_delegate.protecting_mutex);
	pthread_mutex_unlock(&lock_delegate.protecting_mutex);
      }
      ans = cl->call(lock_protocol::acquire, lid, id, r);
    }
      VERIFY(ans == lock_protocol::OK);    
      
      pthread_mutex_lock(&lock_delegate.protecting_mutex);
      lock_delegate.state = FREE;
      pthread_cond_signal(&lock_delegate.cond);
      pthread_mutex_unlock(&lock_delegate.protecting_mutex);

      return lock_protocol::OK;
    } else {
      VERIFY(0);
    }
  default:
    VERIFY(0);
  }

}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  pthread_mutex_lock(&local_table_mutex);
  local_lock_t  &lock_delegate = local_lock_table[lid];
  pthread_mutex_unlock(&local_table_mutex);


  pthread_mutex_lock(&lock_delegate.protecting_mutex);
  switch(lock_delegate.state){
  case NONE:
    VERIFY(0);
  case FREE:
    lock_delegate.state = NONE; // means the server may get an 'acquire()' before it gets the previous release
    pthread_mutex_unlock(&lock_delegate.protecting_mutex);
    
    int r, answer;
    answer = cl->call(lock_protocol::release, lid, id, r);
    VERIFY(answer == lock_protocol::OK);
    return lock_protocol::OK;
    
  case ACQUIRING:
  case LOCKED:
    lock_delegate.state = RELEASING;
    pthread_mutex_unlock(&lock_delegate.protecting_mutex);
    return lock_protocol::OK;
  case RELEASING:
    VERIFY(0);

  default:
    VERIFY(0);

  }
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  pthread_mutex_lock(&local_table_mutex);
  local_lock_t  &lock_delegate = local_lock_table[lid];
  pthread_mutex_unlock(&local_table_mutex);

  pthread_mutex_lock(&lock_delegate.protecting_mutex);
  switch(lock_delegate.state){
  case ACQUIRING:
    lock_delegate.retry_call_received = true;
    pthread_cond_signal(&lock_delegate.cond);
    pthread_mutex_lock(&lock_delegate.protecting_mutex);
    return lock_protocol::OK;
  case NONE:
  case LOCKED:
  case FREE:
  case RELEASING:
  default:
    VERIFY(0);

  }
}
