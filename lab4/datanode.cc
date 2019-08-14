#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr)
{
  ec = new extent_client(extent_dst);

  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);
  if (!ConnectToNN())
  {
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode())
  {
    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }

  /* Add your initialization here */
  NewThread(this, &DataNode::KeepSend);

  return 0;
}

void DataNode::KeepSend()
{
  while (true)
  {
    if (!SendHeartbeat())
    {
      printf("DataNode: SendHeartbeat error\n");
      fflush(stdout);
    }
    sleep(1);
  }
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf)
{
  /* Your lab4 part 2 code */
  printf("DataNode: begin ReadBlock\n");
  fflush(stdout);
  std::string block_content;
  if (ec->read_block(bid, block_content) != extent_protocol::OK)
  {
    printf("ReadBlock: ec read_block error\n");
    return false;
  }

  buf = block_content.substr(offset, len);
  //printf("DataNode: read buf is %s \n", buf);
  //fflush(stdout);
  return true;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf)
{
  /* Your lab4 part 2 code */
  printf("DataNode: begin WriteBlock\n");
  fflush(stdout);
  std::string block_content;
  if (ec->read_block(bid, block_content) != extent_protocol::OK)
  {
    printf("WriteBlock: ec read_block error\n");
    return false;
  }

  block_content.replace(offset, len, buf);

  if (ec->write_block(bid, block_content) != extent_protocol::OK)
  {
    printf("WriteBlock: ec write_block error\n");
    return false;
  }

  return true;
}
