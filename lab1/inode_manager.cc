#include "inode_manager.h"
#ifndef TIP
#define TIP 0
#endif

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  if ((id <= 0) || (id > BLOCK_NUM))
  {
    printf("read block id out of range!\n");
    return;
  }

  if (buf == NULL)
  {
    printf("read block buf is NULL!\n");
    return;
  }

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf)
{

  if ((id <= 0) || (id > BLOCK_NUM))
  {
    printf("write block id out of range!\n");
    return;
  }

  if (buf == NULL)
  {
    printf("write block buf is NULL!\n");
    return;
  }

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  char bitmap_buf[BLOCK_SIZE];
  blockid_t first_block_id = IBLOCK(INODE_NUM, sb.nblocks) + 1;

  for (int block_id = BBLOCK(first_block_id); block_id <= BBLOCK(BLOCK_NUM); block_id++)
  {
    read_block(block_id, bitmap_buf);

    for (int bitmap_index = 0; bitmap_index < BPB; bitmap_index++)
    {
      char bitmap_byte = bitmap_buf[bitmap_index / 8];
      char freebit = bitmap_byte & ((char)1 << (7 - (bitmap_index % 8)));

      if (freebit == 0)
      {
        bitmap_buf[bitmap_index / 8] = bitmap_byte | ((char)1 << (7 - bitmap_index % 8));
        write_block(block_id, bitmap_buf);
        return ((block_id - BBLOCK(1)) * BPB + bitmap_index + 1);
      }
    }
  }

  printf("alloc_block: no free block!\n");
  return 0;
}
void block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  char bitmap_buf[BLOCK_SIZE];
  if (id <= 0 || id > BLOCK_NUM)
  {
    printf("free block id out of range!\n");
    return;
  }

  blockid_t block_id = BBLOCK(id);

  read_block(block_id, bitmap_buf);
  int bitmap_index = id - (block_id - BBLOCK(1)) * BPB - 1;

  char bitmap_byte = bitmap_buf[bitmap_index / 8];
  bitmap_byte = bitmap_byte & ~((char)1 << (7 - (bitmap_index % 8)));
  bitmap_buf[bitmap_index / 8] = bitmap_byte;

  write_block(block_id, bitmap_buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  char fill_buf[BLOCK_SIZE];
  memset(fill_buf, 1, BLOCK_SIZE);

  for (blockid_t bitmap_block_id = BBLOCK(1);
       bitmap_block_id <= BBLOCK(IBLOCK(INODE_NUM, sb.nblocks)); bitmap_block_id++)
  {
    if (bitmap_block_id == BBLOCK(IBLOCK(INODE_NUM, sb.nblocks)))
    {
      int left_blocks_num = IBLOCK(INODE_NUM, sb.nblocks) % BPB;

      if (left_blocks_num == 0)
      {
        break;
      }
      
      char left_buf[BLOCK_SIZE];
      memset(left_buf, 0, BLOCK_SIZE);
      for (int i = 0; i < left_blocks_num; i++)
      {
        char byte = left_buf[i/8];
        byte = byte | ((char)1 << (7 - i % 8));
        left_buf[i/8] = byte;
      }
      write_block(bitmap_block_id,left_buf);
    }
    else
    {
      write_block(bitmap_block_id, fill_buf);
    }
  }
}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1)
  {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  struct inode *ino_disk;
  char buf[BLOCK_SIZE];

  for (uint32_t inum = 1; inum <= INODE_NUM; inum++)
  {
    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);

    ino_disk = (struct inode *)buf + (inum - 1) % IPB;

    if (ino_disk->type == 0)
    {
      ino_disk->type = type;
      ino_disk->size = 0;
      ino_disk->atime = (unsigned int)time(NULL);
      ino_disk->mtime = (unsigned int)time(NULL);
      ino_disk->ctime = (unsigned int)time(NULL);
      bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
      return inum;
    }
  }

  printf("error!inode is full\n");
  exit(0);
}

void inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  if (inum <= 0 || inum > INODE_NUM)
  {
    printf("\tim: inum out of range\n");
    return;
  }

  struct inode *ino_disk = get_inode(inum);

  if (ino_disk->type == 0)
  {
    printf("inode is already a freed one!");
    return;
  }

  ino_disk->type = 0;
  ino_disk->size = 0;
  ino_disk->atime = 0;
  ino_disk->mtime = 0;
  ino_disk->ctime = 0;

  put_inode(inum, ino_disk);
  free(ino_disk);
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  if (inum <= 0 || inum > INODE_NUM)
  {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode *)buf + (inum - 1) % IPB;
  if (ino_disk->type == 0)
  {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode *)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  ino->ctime = (unsigned int)time(NULL);

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + (inum - 1) % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  char buf[BLOCK_SIZE];
  if (size == NULL)
  {
    printf("read file size is NULL\n");
    return;
  }

  if (buf_out == NULL)
  {
    printf("read file buf out is NULL\n");
    return;
  }

  struct inode *ino = get_inode(inum);
  if (ino == NULL)
  {
    printf("read file inode null\n");
    return;
  }

  *buf_out = (char *)malloc(ino->size);
  int block_num = (ino->size - 1 + BLOCK_SIZE) / BLOCK_SIZE;
  *size = ino->size;

  for (int i = 0; i < MIN(block_num, NDIRECT); i++)
  {
    blockid_t read_block_id = ino->blocks[i];
    bm->read_block(read_block_id, buf);
    if (i == block_num - 1)
    {
      memcpy(*buf_out + i * BLOCK_SIZE, buf, ino->size - i * BLOCK_SIZE);
    }
    else
    {
      memcpy(*buf_out + i * BLOCK_SIZE, buf, BLOCK_SIZE);
    }
  }

  if (block_num > NDIRECT)
  {
    blockid_t indirect_blocks[BLOCK_SIZE / sizeof(blockid_t)];
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect_blocks);
    for (int i = 0; i < block_num - NDIRECT; i++)
    {
      blockid_t read_block_id = indirect_blocks[i];
      bm->read_block(read_block_id, buf);
      if (i == block_num - NDIRECT - 1)
      {
        memcpy(*buf_out + (block_num - 1) * BLOCK_SIZE, buf, ino->size - (block_num - 1) * BLOCK_SIZE);
      }
      else
      {
        memcpy(*buf_out + (i + NDIRECT) * BLOCK_SIZE, buf, BLOCK_SIZE);
      }
    }
  }
  ino->atime = (unsigned int)time(NULL);
  put_inode(inum, ino);
  free(ino);

  return;
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  char block_buf[BLOCK_SIZE];
  if (size < 0 || (unsigned int)size > MAXFILE * BLOCK_SIZE)
  {
    printf("write file size error\n");
    return;
  }

  if (buf == NULL)
  {
    printf("write file buf out is NULL\n");
    return;
  }

  struct inode *ino = get_inode(inum);
  if (ino == NULL)
  {
    printf("write file inode null\n");
    return;
  }

  int prev_block_num = (ino->size - 1 + BLOCK_SIZE) / BLOCK_SIZE;
  int next_block_num = (size - 1 + BLOCK_SIZE) / BLOCK_SIZE;

  // new file is smaller or the same size
  if (next_block_num <= prev_block_num)
  {
    // direct block
    if (TIP)
    {
      printf("write a smaller file:%d\n", inum);
    }
    for (int i = 0; i < MIN(next_block_num, NDIRECT); i++)
    {
      blockid_t write_block_id = ino->blocks[i];
      if (i == next_block_num - 1)
      {
        //bzero(block_buf,sizeof(block_buf));
        memcpy(block_buf, buf + i * BLOCK_SIZE, size - i * BLOCK_SIZE);
      }
      else
      {
        memcpy(block_buf, buf + i * BLOCK_SIZE, BLOCK_SIZE);
      }

      bm->write_block(write_block_id, block_buf);
    }

    if (next_block_num > NDIRECT)
    {
      blockid_t indirect_blocks[BLOCK_SIZE / sizeof(blockid_t)];
      bm->read_block(ino->blocks[NDIRECT], (char *)indirect_blocks);
      for (int i = 0; i < next_block_num - NDIRECT; i++)
      {
        blockid_t write_block_id = indirect_blocks[i];

        if (i == next_block_num - NDIRECT - 1)
        {
          memcpy(block_buf, buf + (next_block_num - 1) * BLOCK_SIZE, size - (next_block_num - 1) * BLOCK_SIZE);
        }
        else
        {
          memcpy(block_buf, buf + (i + NDIRECT) * BLOCK_SIZE, BLOCK_SIZE);
        }

        bm->write_block(write_block_id, block_buf);
      }
    }
    for (int i = next_block_num; i < MIN(prev_block_num, NDIRECT); i++)
    {
      if (TIP)
      {
        printf("2:free block %d\n", ino->blocks[i]);
      }
      bm->free_block(ino->blocks[i]);
    }

    if (prev_block_num > NDIRECT)
    {
      blockid_t indirect_blocks[BLOCK_SIZE / sizeof(blockid_t)];
      bm->read_block(ino->blocks[NDIRECT], (char *)indirect_blocks);

      for (int i = (next_block_num > NDIRECT ? next_block_num - NDIRECT : 0); i < prev_block_num - NDIRECT; i++)
      {
        if (TIP)
        {
          printf("3:free block %d\n", indirect_blocks[i]);
        }

        bm->free_block(indirect_blocks[i]);
      }

      if (next_block_num <= NDIRECT)
      {
        bm->free_block(ino->blocks[NDIRECT]);
      }
    }
  }
  // new file is bigger
  else
  {
    if (TIP)
    {
      printf("write a bigger file!\n");
    }
    for (int i = 0; i < MIN(prev_block_num, NDIRECT); i++)
    {
      blockid_t write_block_id = ino->blocks[i];
      memcpy(block_buf, buf + i * BLOCK_SIZE, BLOCK_SIZE);

      bm->write_block(write_block_id, block_buf);
    }

    for (int i = prev_block_num; i < MIN(next_block_num, NDIRECT); i++)
    {
      blockid_t new_block = bm->alloc_block();
      ino->blocks[i] = new_block;

      if (i == next_block_num - 1)
      {
        memcpy(block_buf, buf + i * BLOCK_SIZE, size - i * BLOCK_SIZE);
      }
      else
      {
        memcpy(block_buf, buf + i * BLOCK_SIZE, BLOCK_SIZE);
      }
      bm->write_block(new_block, block_buf);
    }

    if (next_block_num > NDIRECT)
    {
      blockid_t indirect_blocks[BLOCK_SIZE / sizeof(blockid_t)];
      if (prev_block_num > NDIRECT)
      {
        bm->read_block(ino->blocks[NDIRECT], (char *)indirect_blocks);
      }
      else
      {
        blockid_t new_indirect_block = bm->alloc_block();
        ino->blocks[NDIRECT] = new_indirect_block;
        bm->read_block(ino->blocks[NDIRECT], (char *)indirect_blocks);
      }

      for (int i = 0; i < prev_block_num - NDIRECT; i++)
      {
        blockid_t write_block_id = indirect_blocks[i];
        memcpy(block_buf, buf + (i + NDIRECT) * BLOCK_SIZE, BLOCK_SIZE);
        bm->write_block(write_block_id, block_buf);
      }

      for (int i = prev_block_num > NDIRECT ? prev_block_num - NDIRECT : 0; i < next_block_num - NDIRECT; i++)
      {
        blockid_t new_block = bm->alloc_block();
        indirect_blocks[i] = new_block;
        if (i == next_block_num - NDIRECT - 1)
        {
          memcpy(block_buf, buf + (i + NDIRECT) * BLOCK_SIZE, size - (next_block_num - 1) * BLOCK_SIZE);
        }
        else
        {
          memcpy(block_buf, buf + (i + NDIRECT) * BLOCK_SIZE, BLOCK_SIZE);
        }
        bm->write_block(new_block, block_buf);
      }

      if (next_block_num > NDIRECT)
      {
        bm->write_block(ino->blocks[NDIRECT], (char *)indirect_blocks);
      }
    }
  }

  ino->size = size;
  ino->mtime = (unsigned int)time(NULL);
  ino->ctime = (unsigned int)time(NULL);
  put_inode(inum, ino);
  free(ino);
  return;
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  if (inum <= 0 || inum > INODE_NUM)
  {
    printf("getattr inum out of range\n");
    return;
  }
  struct inode *ino = get_inode(inum);

  if (ino == NULL)
  {
    printf("getattr ino is null\n");
    return;
  }
  a.type = ino->type;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.size = ino->size;

  free(ino);
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  if (inum <= 0 || inum > INODE_NUM)
  {
    printf("remove file inum out of range\n");
    return;
  }

  if (TIP)
  {
    printf("remove file:%d\n", inum);
  }

  blockid_t indirect_blocks[BLOCK_SIZE / sizeof(blockid_t)];
  struct inode *ino = get_inode(inum);

  if (ino == NULL)
  {
    printf("remove file ino is NULL\n");
    return;
  }
  int block_num = (ino->size - 1 + BLOCK_SIZE) / BLOCK_SIZE;

  for (int i = 0; i < MIN(block_num, NDIRECT); i++)
  {
    bm->free_block(ino->blocks[i]);
  }

  if (block_num > NDIRECT)
  {
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect_blocks);

    for (int i = 0; i < block_num - NDIRECT; i++)
    {
      bm->free_block(indirect_blocks[i]);
    }
  }

  free_inode(inum);
  free(ino);

  return;
}
