#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

class lock_server_cache
{
private:
  struct lock_info
  {
    bool is_hold;
    std::string owner;
    std::queue<std::string> waiting_queue;
    pthread_cond_t revoke_cv;
    lock_info() : is_hold(false), owner("")
    {
      pthread_cond_init(&revoke_cv, NULL);
    }
  };
  int nacquire;
  pthread_mutex_t mutex;
  std::map<lock_protocol::lockid_t, lock_info> lock_admin;

public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
