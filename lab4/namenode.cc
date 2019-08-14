#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);

  /* Add your init logic here */
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino)
{
  printf("NameNode: begin GetBlockLocations");
  fflush(stdout);
  std::list<blockid_t> block_ids;
  list<NameNode::LocatedBlock> block_locs;
  extent_protocol::attr ino_attr;

  if (ec->get_block_ids(ino, block_ids) != extent_protocol::OK)
  {
    printf("GetBlockLocations: ec get_block_ids error\n");
    fflush(stdout);
    return list<LocatedBlock>();
  }

  if (ec->getattr(ino, ino_attr) != extent_protocol::OK)
  {
    printf("GetBlockLocations: ec getattr error\n");
    fflush(stdout);
    return list<LocatedBlock>();
  }

  int size = block_ids.size();
  unsigned int file_size = ino_attr.size;

  for (int i = 0; i < size; i++)
  {
    blockid_t block_id = block_ids.front();
    uint64_t offset = i * BLOCK_SIZE;
    if (i == size - 1)
    {
      if (file_size % BLOCK_SIZE != 0)
      {
        block_locs.push_back(LocatedBlock(block_id, offset, file_size - BLOCK_SIZE * i, GetDatanodes()));
        break;
      }
    }
    block_locs.push_back(LocatedBlock(block_id, offset, BLOCK_SIZE, GetDatanodes()));
    block_ids.pop_front();
  }

  return block_locs;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size)
{
  printf("NameNode: begin complete\n");
  fflush(stdout);
  if (ec->complete(ino, new_size) != extent_protocol::OK)
  {
    lc->release(ino);
    return false;
  }
  RecordWritten(ino);
  lc->release(ino);
  return true;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino)
{
  printf("NameNode: begin AppendBlock\n");
  fflush(stdout);
  blockid_t bid;
  extent_protocol::attr ino_attr;

  if (ec->getattr(ino, ino_attr) != extent_protocol::OK)
  {
    printf("AppendBlock: ec getattr error\n");
    fflush(stdout);
    return LocatedBlock(0, 0, 0, GetDatanodes());
  }

  if (ec->append_block(ino, bid) != extent_protocol::OK)
  {
    printf("AppendBlock: ec append_block error\n");
    fflush(stdout);
    return LocatedBlock(0, 0, 0, GetDatanodes());
  }

  unsigned int file_size = ino_attr.size;
  uint64_t offset = ((file_size - 1 + BLOCK_SIZE) / BLOCK_SIZE) * BLOCK_SIZE;

  return LocatedBlock(bid, offset, BLOCK_SIZE, GetDatanodes());
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name)
{
  printf("NameNode:: begin Rename\n");
  fflush(stdout);
  lc->acquire(src_dir_ino);
  bool found = false;
  yfs_client::inum ino;

  if (yfs->lookup_l(src_dir_ino, src_name.c_str(), found, ino) != yfs_client::OK)
  {
    printf("Rename: yfs lookup_l error\n");
    fflush(stdout);
    lc->release(src_dir_ino);
    return false;
  }

  if (!found)
  {
    printf("Rename: src file not found\n");
    fflush(stdout);
    lc->release(src_dir_ino);
    return false;
  }

  if (yfs->deleteDirent_l(src_dir_ino, src_name.c_str()) != yfs_client::OK)
  {
    printf("Rename: yfs deleteDirent_l error\n");
    fflush(stdout);
    lc->release(src_dir_ino);
    return false;
  }

  lc->release(src_dir_ino);

  yfs_client::dirent dst_entry;
  dst_entry.name = dst_name;
  dst_entry.inum = ino;

  lc->acquire(dst_dir_ino);
  if (yfs->addDirent_l(dst_dir_ino, dst_entry) != yfs_client::OK)
  {
    printf("Rename: yfs addDirent_l error\n");
    fflush(stdout);
    lc->release(dst_dir_ino);
    return false;
  }

  lc->release(dst_dir_ino);
  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out)
{
  printf("NameNode: begin mkdir\n");
  fflush(stdout);
  if (yfs->mkdir(parent, name.c_str(), mode, ino_out) != yfs_client::OK)
  {
    printf("Mkdir: yfs mkdir error\n");
    fflush(stdout);
    return false;
  }

  lc->acquire(parent);
  RecordWritten(parent);
  lc->release(parent);

  return true;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out)
{
  printf("NameNode: begin create\n");
  fflush(stdout);
  if (yfs->create(parent, name.c_str(), mode, ino_out) != yfs_client::OK)
  {
    printf("Create: yfs create error\n");
    fflush(stdout);
    return false;
  }
  printf("NameNode: finish create,acquire lock: %lld\n", ino_out);
  fflush(stdout);

  lc->acquire(parent);
  RecordWritten(parent);
  lc->release(parent);

  lc->acquire(ino_out);
  return true;
}

bool NameNode::Isfile(yfs_client::inum ino)
{
  printf("NameNode: begin Isfile\n");
  fflush(stdout);
  return yfs->isfile_l(ino);
}

bool NameNode::Isdir(yfs_client::inum ino)
{
  printf("NameNode: begin Isdir\n");
  fflush(stdout);
  return yfs->isdir_l(ino);
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info)
{
  printf("NameNode: begin Getfile\n");
  fflush(stdout);
  if (yfs->getfile_l(ino, info) != yfs_client::OK)
  {
    printf("Getfile: yfs getfile_l error\n");
    fflush(stdout);
    return false;
  }
  fflush(stdout);
  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info)
{
  printf("NameNode: begin Getdir\n");
  fflush(stdout);
  if (yfs->getdir_l(ino, info) != yfs_client::OK)
  {
    printf("Getdir: yfs getdir_l error\n");
    fflush(stdout);
    return false;
  }
  fflush(stdout);
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir)
{
  printf("NameNode: begin Readdir\n");
  fflush(stdout);
  if (yfs->readdir_l(ino, dir) != yfs_client::OK)
  {
    printf("Readdir: yfs readdir_l error\n");
    fflush(stdout);
    return false;
  }
  fflush(stdout);
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino)
{
  printf("NameNode: begin Unlink\n");
  fflush(stdout);
  if (yfs->unlink_l(parent, name.c_str()) != yfs_client::OK)
  {
    printf("Unlink: yfs unlink_l error\n");
    fflush(stdout);
    return false;
  }
  fflush(stdout);
  return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id)
{
  if (datanode_states.count(id) == 0)
  {
    printf("DatanodeHeartbeat: no id found\n");
    fflush(stdout);
    return;
  }
  struct DatanodeState *ds = &(datanode_states.find(id)->second);
  if (ds->state == live || ds->state == recover)
  {
    ds->lastHeartbeatTime = time(NULL);
  }
  else
  {
    // need recovery
    ds->lastHeartbeatTime = time(NULL);
    ds->state = recover;

    NewThread(this, &NameNode::CheckDataNodeState, id);
    NewThread(this, &NameNode::ReplicateData, id);
  }
}

void NameNode::RegisterDatanode(DatanodeIDProto id)
{
  bool found = false;

  std::list<DatanodeIDProto>::iterator id_iter = datanode_ids.begin();
  while (id_iter != datanode_ids.end())
  {
    if (*id_iter == id)
    {
      found = true;
      break;
    }
    id_iter++;
  }

  if (!found)
  {
    printf("NameNode: RegisterDatanode ok\n");
    fflush(stdout);
    datanode_ids.push_back(id);
    datanode_states.insert(std::pair<DatanodeIDProto, DatanodeState>(id, DatanodeState(recover, time(NULL))));

    NewThread(this, &NameNode::CheckDataNodeState, id);
    NewThread(this, &NameNode::ReplicateData, id);
  }
}

list<DatanodeIDProto> NameNode::GetDatanodes()
{
  std::list<DatanodeIDProto> live_nodes;
  std::list<DatanodeIDProto>::iterator id_iter = datanode_ids.begin();

  while (id_iter != datanode_ids.end())
  {
    DatanodeIDProto id = *id_iter;
    struct DatanodeState *ds = &(datanode_states.find(id)->second);
    if (ds->state == live)
    {
      live_nodes.push_back(*id_iter);
    }
    id_iter++;
  }

  return live_nodes;
}

void NameNode::CheckDataNodeState(DatanodeIDProto id)
{
  printf("NameNode: begin CheckDataNodeState\n");
  fflush(stdout);
  while (true)
  {
    unsigned int present_time = time(NULL);
    if (datanode_states.count(id) == 0)
    {
      printf("CheckDataNodeState: no id found\n");
      fflush(stdout);
      return;
    }
    struct DatanodeState *ds = &(datanode_states.find(id)->second);
    if (present_time - ds->lastHeartbeatTime >= 4)
    {
      printf("CheckDataNodeState: datanode is dead\n");
      fflush(stdout);
      ds->state = dead;
      return;
    }

    sleep(1);
  }
}

void NameNode::RecordWritten(yfs_client::inum ino)
{
  std::list<blockid_t> ino_blocks;
  if (ec->get_block_ids(ino, ino_blocks) != extent_protocol::OK)
  {
    printf("RecordWritten: ec get_block_ids error\n");
    fflush(stdout);
    return;
  }

  std::list<blockid_t>::iterator iter = ino_blocks.begin();
  while (iter != ino_blocks.end())
  {
    /*
    blockid_t id = *iter;
    bool found = false;

    std::list<blockid_t>::iterator written_iter = written_blocks.begin();
    while (written_iter != written_blocks.end())
    {
      if (*written_iter == id)
      {
        found = true;
        break;
      }
      written_iter++;
    }

    if (!found)
    {
      written_blocks.push_back(id);
    }
    */
    written_blocks.push_back(*iter);
    iter++;
  }
}

void NameNode::ReplicateData(DatanodeIDProto id)
{
  struct DatanodeState *ds = &(datanode_states.find(id)->second);
  if (datanode_ids.empty() || written_blocks.empty())
  {
    printf("ReplicateData: no need to ReplicateData\n");
    fflush(stdout);
    ds->state = live;
    return;
  }
  std::list<blockid_t>::iterator iter = written_blocks.begin();

  while (iter != written_blocks.end())
  {
    ReplicateBlock(*iter, master_datanode, id);
    iter++;
  }
  ds->state = live;
}
