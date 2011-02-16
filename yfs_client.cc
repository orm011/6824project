// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#define ROOTINUM 0x00000001
#include <cstdlib>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  
  //add root to extent server
  ec->put(ROOTINUM, std::string());
  
  //create generator
  gen = new generator();
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{

  //TODO: is this the inum convention we follow, then?
  // where to we make sure the 1 and 0s get set? fuse.cc?
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}

int
yfs_client::lookup(std::string name, inum parent, dirent& ent){
  assert(isdir(parent) && parent == (parent & 0xFFFF));

  string buf;
  assert(ec->get(parent, buf) == extent_protocol::OK);

  Directory parentDir(buf);

  list<dirent>::iterator  it;
  for  (it = parentDir.begin(); it != parentDir.end() && it -> name != name; it++){}

  if (it != parentDir.end()){
    //TODO: make sure this is actually making a copy.
    ent = *it;
    return yfs_client::OK;
  } else {
    return yfs_client::NOENT;
  }
 
}

int 
yfs_client::mkfile(std::string name, inum parent, inum& ret){
  assert(isdir(parent) && parent == (parent & 0xFFFF));
    
  string buf;

  assert(ec->get(parent, buf) == extent_protocol::OK);
  
  Directory parentDir(buf);
  inum inu = gen->fileinum();
  dirent newfile(name, inu );
  

  std::list<dirent>::iterator it;
  for  (it = parentDir.begin(); it != parentDir.end() && it -> name != name; it++){}
  
  if (it != parentDir.end()){
      return yfs_client::EXIST;
  } 

  parentDir.insert_entry(newfile);
  ret = inu;
  assert(ec->put(parent, parentDir.serialize()) == extent_protocol::OK);

  return yfs_client::OK;
}

int 
yfs_client::readdir(inum inumber, list<dirent>& dirlist){
  assert(isdir(inumber) && inumber == (inumber & 0xFFFF));
  
  string buf;

  assert(ec->get(inumber,buf) == extent_protocol::OK);
  Directory dir(buf);

  dirlist = dir.entries;

  return yfs_client::OK;
}

/* apparently not for lab2*/
int 
yfs_client::mkdir(std::string name, inum parent){
  assert(isdir(parent) && parent == (parent & 0xFFFF));
  
  string buf; 
  assert(ec->get(parent, buf) == extent_protocol::OK);

  Directory parentDir(buf);
  dirent newdir(name, gen->dirinum());

  parentDir.insert_entry(newdir);
  assert(ec->put(parent, parentDir.serialize()) == extent_protocol::OK);

  return yfs_client::OK;
}


/* ----------Directory data structure methods ---------------------------*/

yfs_client::Directory::Directory(std::string& serial){
  //parse string and generate structures
  unsigned int pos = 0;

  while (pos < serial.size()){
    //read in the name
    unsigned int len;
    sscanf(serial.substr(pos, NAMELENBYTES).c_str(), NAMELENFORMAT, &len);
    pos += NAMELENBYTES;
    string filename = serial.substr(pos, len);
      
    //read in the inum
    pos += len;
    unsigned long long int in;
    sscanf(serial.substr(pos, INUMBYTES).c_str(), INUMFORMAT, &in);

    pos += INUMBYTES;
    dirent entry(filename, (inum)in);
    entries.push_back(entry);
  }

  assert(pos = serial.size());

}

yfs_client::Directory::~Directory(){
  entries.~list();
}

std::string yfs_client::Directory::serialize(){
  list<dirent>::iterator it = entries.begin();
  char sizestring[NAMELENBYTES + 1];
  char inumstring[INUMBYTES + 1];

  std::string total;

  while (it != entries.end()){	

    //4bytes
    sprintf(sizestring, NAMELENFORMAT, it->name.size());
    total.append(sizestring);

    //namestring
    total.append(it->name);

    //64bit inum 8 bytes 
    sprintf(inumstring, INUMFORMAT, it->inum);
    total.append(inumstring);
  }

  return total;
}  

void yfs_client::Directory::insert_entry(dirent&  entry){
  entries.push_back(entry);
}

  //returns iterator to internal list
  //(don't have time to think of how to make a proper iterator right now)
list<yfs_client::dirent>::iterator  yfs_client::Directory::begin(){
  return entries.begin();
}

list<yfs_client::dirent>::iterator yfs_client::Directory::end(){
  return entries.end();
}

/*--------- inum generator methods ------------------*/


yfs_client::generator::generator(){
  srand(0);
}

yfs_client::inum yfs_client::generator::fileinum(){
  return (yfs_client::inum) ((unsigned int)rand() | 0x8000u);
}

yfs_client::inum yfs_client::generator::dirinum(){
  return (yfs_client::inum) ((unsigned int)rand() & 0x7FFFu);
}
