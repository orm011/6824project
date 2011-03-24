// extent wire protocol

#ifndef extent_protocol_h
#define extent_protocol_h

#include "rpc.h"
#include <ctime>

using namespace std;

class extent_protocol {
 public:
  typedef int status;
  typedef unsigned long long extentid_t;
  enum xxstatus { OK, RPCERR, NOENT, IOERR };
  enum rpc_numbers {
    put = 0x6001,
    get,
    getattr,
    remove
  };

  struct attr {
    unsigned int atime;
    unsigned int mtime;
    unsigned int ctime;
    unsigned int size;

    void init(string buf){
      size = buf.size();
      atime =  mtime = ctime = time(NULL);
    }

    void put(string buf){
      size = buf.size(); 
      ctime = mtime = time(NULL);
    }

    void get(){
      atime = time(NULL);
    }
  };
};

inline unmarshall &
operator>>(unmarshall &u, extent_protocol::attr &a)
{
  u >> a.atime;
  u >> a.mtime;
  u >> a.ctime;
  u >> a.size;
  return u;
}

inline marshall &
operator<<(marshall &m, extent_protocol::attr a)
{
  m << a.atime;
  m << a.mtime;
  m << a.ctime;
  m << a.size;
  return m;
}

#endif 
