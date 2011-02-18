// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <ctime>

extent_server::extent_server(): extentmap()
{
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  bool newid = (extentmap.find(id) == extentmap.end());
  extent_protocol::attr& attributes =  extentmap[id].attr;
  
  attributes.size = buf.size();
  attributes.ctime = attributes.mtime = time(NULL);

  //atime gets set in put() only at creation time.
  if (newid) attributes.atime = attributes.mtime;

  //check this piece of code: it might not be storing anything after function returns.
  extentmap[id].str = buf;
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  if (extentmap.find(id) != extentmap.end()){
    buf = extentmap[id].str;
    extentmap[id].attr.atime = time(NULL);
    return extent_protocol::OK;
  } else {
    return extent_protocol::NOENT;
  }
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.

  if (extentmap.find(id) != extentmap.end()){

    extent_protocol::attr& extentattr = extentmap[id].attr;

    a.size = extentattr.size;
    a.atime = extentattr.atime;
    a.mtime = extentattr.mtime;
    a.ctime = extentattr.ctime;

    return extent_protocol::OK;

  } else {
    return extent_protocol::NOENT;
  }
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  map<extent_protocol::extentid_t, datapiece>::iterator it = extentmap.find(id);

  if (it != extentmap.end()){
    extentmap.erase(it);
    return extent_protocol::OK;
  } else {
    return extent_protocol::NOENT;
  }
}
