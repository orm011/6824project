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

  extent_protocol::status ret = extent_protocol::OK;
  
  ScopedLock sl(&cache_table_mutex);  
  cache_row_t & row = cache_table[eid];

  if (row.current){
    buf = row.buf;
  } else {
    ret = cl->call(extent_protocol::get, eid, buf);
    row.current = true;
    row.buf = buf;
  }
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  //  int r;
  cache_row_t & row = cache_table[eid];
  assert(row.current);
  assert(buf != row.buf);
  row.buf = buf;
  row.dirty = true;
  
  //  ret = cl->call(extent_protocol::put, eid, buf, r);

  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  //  int r;
  
  cache_table.erase(eid);
  //  ret = cl->call(extent_protocol::remove, eid, r);
  return ret;
}
