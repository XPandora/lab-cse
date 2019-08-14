#ifndef yfs_client_h
#define yfs_client_h

#include <string>

#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"

//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>

class yfs_client
{
  extent_client *ec;
  lock_client_cache *lc;

public:
  typedef unsigned long long inum;
  enum xxstatus
  {
    OK,
    RPCERR,
    NOENT,
    IOERR,
    EXIST
  };
  typedef int status;

  struct fileinfo
  {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo
  {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct symlinkinfo
  {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent
  {
    std::string name;
    yfs_client::inum inum;
  };

private:
  static std::string filename(inum);
  static inum n2i(std::string);

  void acquirelock(inum);
  void releaselock(inum);
  void acquireBitmap();
  void releaseBitmap();

public:
  yfs_client();
  yfs_client(std::string, std::string);

  bool isfile_l(inum);
  bool isdir_l(inum);
  bool issymlink_l(inum);

  int getfile_l(inum, fileinfo &);
  int getdir_l(inum, dirinfo &);
  int getsymlink_l(inum, symlinkinfo &);

  int setattr_l(inum, size_t);
  int lookup_l(inum, const char *, bool &, inum &);
  int addDirent_l(inum, dirent);
  int deleteDirent_l(inum, const char *);
  int create_l(inum, const char *, mode_t, inum &);
  int readdir_l(inum, std::list<dirent> &);
  int write_l(inum, size_t, off_t, const char *, size_t &);
  int read_l(inum, size_t, off_t, std::string &);
  int unlink_l(inum, const char *);
  int mkdir_l(inum, const char *, mode_t, inum &);
  int symlink_l(inum, const char *, const char *name, inum &);
  int readlink_l(inum, std::string &);

  bool isfile(inum);
  bool isdir(inum);
  bool issymlink(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  int getsymlink(inum, symlinkinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int addDirent(inum, dirent);
  int deleteDirent(inum, const char *);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum, const char *);
  int mkdir(inum, const char *, mode_t, inum &);
  int symlink(inum, const char *, const char *name, inum &);
  int readlink(inum, std::string &);
  /** you may need to add symbolic link related methods here.*/
};

#endif





