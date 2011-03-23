// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>



// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  pthread_mutex_init(&cache_table_mutex, NULL);
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  fprintf(stderr, "entering get...\n");
  extent_protocol::status ret = extent_protocol::OK;
  bool row_present;

  pthread_mutex_lock(&cache_table_mutex);  
  row_present = (cache_table.find(eid) != cache_table.end());
  cache_row_t & row = cache_table[eid];
  pthread_mutex_unlock(&cache_table_mutex);

  if (row_present){
    buf = row.buf;
    goto ret;
  } 

  ret = cl->call(extent_protocol::get, eid, buf); 

  if (ret == extent_protocol::OK){
    VERIFY((ret = cl->call(extent_protocol::getattr, eid, row.attr)) == extent_protocol::OK);
    row.buf = buf;
  } else{
    VERIFY(ret == extent_protocol::NOENT);
    pthread_mutex_lock(&cache_table_mutex);  
    cache_table.erase(eid);
    pthread_mutex_unlock(&cache_table_mutex);  
  }

 ret:
  fprintf(stderr, "leaving get...\n");
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  fprintf(stderr, "entering getattr...\n");
  pthread_mutex_lock(&cache_table_mutex);  
  bool found = (cache_table.find(eid) != cache_table.end());
  pthread_mutex_unlock(&cache_table_mutex);

  if (!found){
    string buf;
    ret = get(eid,buf);
  } 

  if (ret == extent_protocol::OK){
    pthread_mutex_lock(&cache_table_mutex);  
    VERIFY(cache_table.find(eid) != cache_table.end());
    cache_row & row = cache_table[eid];
    pthread_mutex_unlock(&cache_table_mutex);
    attr = row.attr;
  } else {
    VERIFY(ret == extent_protocol::NOENT);
  }

  fprintf(stderr, "leaving getattr...\n");
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  fprintf(stderr, "entering put...\n");
  extent_protocol::status ret = extent_protocol::OK;
  //  int r;
  pthread_mutex_lock(&cache_table_mutex);
  cache_row_t & row = cache_table[eid];
  pthread_mutex_unlock(&cache_table_mutex);

  extent_protocol::attr & attri = row.attr;
  row.dirty = (buf != row.buf);
  attri.size = buf.size();
  attri.ctime = attri.mtime = time(NULL);
  row.buf = buf;

  fprintf(stderr, "leaving put...\n");
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&cache_table_mutex);
  ret = (cache_table.erase(eid) == 1)? extent_protocol::OK : extent_protocol::NOENT;
  pthread_mutex_unlock(&cache_table_mutex);
  return ret;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  cache_row_t row;
  fprintf(stderr, "extent_client: in flush method\n");
  pthread_mutex_lock(&cache_table_mutex);  
  bool deleted = (cache_table.find(eid) == cache_table.end());
  if (!deleted){
    row = cache_table[eid];
    cache_table.erase(eid);
  }
  pthread_mutex_unlock(&cache_table_mutex);  

  int r;
  if (deleted){
    ret = cl->call(extent_protocol::remove, eid, r);
    VERIFY(ret == extent_protocol::OK || ret == extent_protocol::NOENT);
  } else if (row.dirty) {
    VERIFY((ret = cl->call(extent_protocol::put, eid, row.buf, r)) == extent_protocol::OK);
  }

  return ret;
}
