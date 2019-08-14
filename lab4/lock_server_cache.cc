// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

lock_server_cache::lock_server_cache() : nacquire(0)
{
  pthread_mutex_init(&mutex, NULL);
}

bool inWaitingQueue(std::queue<std::string> waiting_queue, std::string id)
{
  while (!waiting_queue.empty())
  {
    if (waiting_queue.front() == id)
    {
      return true;
    }
    waiting_queue.pop();
  }

  return false;
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id,
                               int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if (lock_admin.count(lid) == 0)
  {
    lock_admin[lid] = lock_info();
  }

  lock_info *li = &lock_admin[lid];

  //tprintf("server: lock owner: %s\n", li->owner.c_str());
  //tprintf("server: %s call for aquire\n", id.c_str());
  if (li->is_hold)
  {
    if (li->owner == id)
    {
      tprintf("server: client %s had alread get the lock\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      return lock_protocol::OK;
    }

    if (inWaitingQueue(li->waiting_queue, id))
    {
      tprintf("server: client %s is already in waiting queue\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      return lock_protocol::RETRY;
    }
    int r;
    std::string revoke_cl;
    if (li->waiting_queue.empty())
    {
      revoke_cl = li->owner;
    }
    else
    {
      revoke_cl = li->waiting_queue.back();
    }

    li->waiting_queue.push(id);
    while (revoke_cl != li->owner)
    {
      pthread_cond_wait(&li->revoke_cv, &mutex);
    }
    pthread_mutex_unlock(&mutex);
    handle h(revoke_cl);

    if (h.safebind())
    {
      ret = h.safebind()->call(rlock_protocol::revoke, lid, r);
    }
    pthread_mutex_lock(&mutex);
    /*if (!h.safebind() || ret != lock_protocol::OK)
    {
      tprintf("server: call for client %s revoke error\n", li->owner.c_str());
      ret = lock_protocol::IOERR;
      pthread_mutex_unlock(&mutex);
      return ret;
    }*/
    ret = lock_protocol::RETRY;
  }
  else
  {
    li->is_hold = true;
    li->owner = id;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

int lock_server_cache::release(lock_protocol::lockid_t lid, std::string id,
                               int &r)
{
  lock_protocol::status ret = lock_protocol::OK;

  if (lock_admin.count(lid) == 0)
  {
    tprintf("server: no lock %llu exists in lock_admin\n", lid);
    return lock_protocol::NOENT;
  }

  pthread_mutex_lock(&mutex);
  lock_info *li = &lock_admin[lid];
  //tprintf("server: %s call for release\n", id.c_str());

  if (li->owner != id || !li->is_hold)
  {
    tprintf("server: %s has already released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return lock_protocol::OK;
  }

  if (!li->is_hold)
  {
    tprintf("server: lock %llu is not held\n", lid);
    ret = lock_protocol::IOERR;
  }
  else
  {
    if (li->waiting_queue.empty())
    {
      tprintf("server: %s call for release, waiting queue should not be empty\n", id.c_str());
      li->is_hold = false;
      li->owner = "";
      ret = lock_protocol::OK;
    }
    else
    {
      std::string retry_cl = li->waiting_queue.front();

      li->waiting_queue.pop();
      li->owner = retry_cl;
      //tprintf("server: lock:%llu new owner %s\n", lid, retry_cl.c_str());
      handle h(retry_cl);

      if (h.safebind())
      {
        pthread_mutex_unlock(&mutex);
        ret = h.safebind()->call(rlock_protocol::retry, lid, r);
        pthread_mutex_lock(&mutex);
      }
      pthread_cond_broadcast(&li->revoke_cv);
      /*if (!h.safebind() || ret != lock_protocol::OK)
      {
        tprintf("server: call for client %s retry error\n", retry_cl.c_str());
        ret = lock_protocol::RPCERR;
      }*/

      //tprintf("server: send retry succeed, new owner %s\n", retry_cl.c_str());
    }
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

