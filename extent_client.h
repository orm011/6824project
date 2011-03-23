// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "rpc.h"


using namespace std;
class extent_client {
  
  typedef struct cache_row {
    bool dirty;
    extent_protocol::attr attr;
    string buf;
  cache_row(): dirty(true), buf("INVALID"){}
  } cache_row_t;

 private:
  rpcc *cl;
  map<extent_protocol::extentid_t, cache_row_t> cache_table;
  pthread_mutex_t cache_table_mutex;
  
 public:
  extent_client(std::string dst);

  extent_protocol::status get(extent_protocol::extentid_t eid, 
			      std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);

  extent_protocol::status flush(extent_protocol::extentid_t eid);
};

#endif 

