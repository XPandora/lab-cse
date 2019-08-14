// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

// Classes that inherit lock_release_user can override dorelease so that
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user
{
public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user(){};
};

class lock_client_cache : public lock_client
{
private:
  enum State
  {
    NONE,
    FREE,
    LOCKED,
    ACQUIRING,
    RELEASING
  };

  struct lock_info
  {
    State st;
    bool revoked;
    bool retry;
    int acquire_num;

    pthread_cond_t retry_mutex;
    pthread_cond_t revoke_mutex;
    pthread_cond_t local_wait_mutex;
    lock_info() : st(NONE), revoked(false), retry(false), acquire_num(0)
    {
      pthread_cond_init(&retry_mutex, NULL);
      pthread_cond_init(&revoke_mutex, NULL);
      pthread_cond_init(&local_wait_mutex, NULL);
    }
  };
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  pthread_mutex_t mutex;
  std::map<lock_protocol::lockid_t, lock_info> lock_map;

public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache(){};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t,
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t,
                                       int &);
};

#endif
