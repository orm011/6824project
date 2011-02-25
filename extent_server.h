// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include <pthread.h>

using namespace std;

class extent_server {
  
  struct datapiece {
    std::string str;
    extent_protocol::attr attr;
  };

  map<extent_protocol::extentid_t, datapiece> extentmap;
  pthread_mutex_t maplatch;
  

 public:
  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif 
