// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include <unistd.h>

#define RVOKEMAX_ACQUIRE 1

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user *_lu)
    : lock_client(xdst), lu(_lu)
{
  srand(time(NULL) ^ last_port);
  rlock_port = ((rand() % 32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  pthread_mutex_init(&mutex, NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  int r;
  pthread_mutex_lock(&mutex);

  if (lock_map.count(lid) == 0)
  {
    lock_map[lid] = lock_info();
  }

  lock_info *li = &lock_map[lid];
  li->acquire_num++;
  while (li->st == LOCKED || li->st == ACQUIRING || li->st == RELEASING)
  {
    pthread_cond_wait(&li->local_wait_mutex, &mutex);
  }

  if (li->st == FREE)
  {
    li->st = LOCKED;
    li->acquire_num--;
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  if (li->st == NONE)
  {
    li->st = ACQUIRING;
    li->retry = false;
    
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::acquire, lid, id, r);
    pthread_mutex_lock(&mutex);

    if (ret == lock_protocol::OK)
    {
      li->st = LOCKED;
      //li->retry = false;
    }
    else
    {
      while (!li->retry)
      {
        pthread_cond_wait(&li->retry_mutex, &mutex);
      }
      li->st = LOCKED;
    }
  }
  
  li->acquire_num--;
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  int r;

  if (lock_map.count(lid) == 0)
  {
    tprintf("client: no lock %llu exists\n", lid);
    return lock_protocol::NOENT;
  }

  pthread_mutex_lock(&mutex);
  
  lock_info *li = &lock_map[lid];

  if (li->revoked && li->acquire_num < RVOKEMAX_ACQUIRE)
  {
    li->st = RELEASING;
    li->revoked = false;
    
    pthread_mutex_unlock(&mutex);

    ret = cl->call(lock_protocol::release, lid, id, r);

    pthread_mutex_lock(&mutex);
    
    li->st = NONE;
    pthread_cond_signal(&li->local_wait_mutex);
  }
  else
  {
    li->st = FREE;
    pthread_cond_signal(&li->local_wait_mutex);
  }

  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &)
{
  usleep(500000);
  int ret = rlock_protocol::OK;
  int r;
  pthread_mutex_lock(&mutex);
  lock_info *li = &lock_map[lid];

  if (li->st == FREE)
  {
    li->st = RELEASING;
    
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);
    
    li->st = NONE;
    pthread_cond_signal(&li->local_wait_mutex);
  }
  else
  {
    li->revoked = true;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &)
{
  int ret = rlock_protocol::OK;
  if (lock_map.count(lid) == 0)
  {
    tprintf("client: no lock %llu exists\n", lid);
    return rlock_protocol::OK;
  }
  pthread_mutex_lock(&mutex);
  if (lock_map[lid].retry)
  {
    tprintf("client: %s aleady recevie retry request\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return rlock_protocol::OK;
  }
  lock_map[lid].retry = true;
  pthread_cond_signal(&lock_map[lid].retry_mutex);
  pthread_mutex_unlock(&mutex);
  return ret;
}
