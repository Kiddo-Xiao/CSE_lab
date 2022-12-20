#include "inode_manager.h"
using namespace std;

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf,blocks[id],BLOCK_SIZE);//put context in blocks[id] into buf
}

void
disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id],buf,BLOCK_SIZE);//put context in buf into blocks[id]
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*从datablock起始block位置开始找到第一个未使用的block标记使用
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  int i = IBLOCK(INODE_NUM, sb.nblocks) + 1;//i=datablock起始block位置
  for(i;i<BLOCK_NUM;++i){
    if(using_blocks[i]==0){
      using_blocks[i]=1;
      return i;
    }
  }
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  using_blocks[id]=0;
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

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
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
  char buf[BLOCK_SIZE];
  struct inode *ino;
  for(uint32_t inum = 1; inum < INODE_NUM; ++inum){
      bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
      ino = (struct inode*)buf + inum%IPB;
      if(ino->type == 0){
        ino->type = type;
        ino->size = 0;
        ino->atime = time(NULL);
        ino->mtime = time(NULL);
        ino->ctime = time(NULL);
        bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
        return inum;
      }
  }
  return 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  char buf[BLOCK_SIZE];
  struct inode *ino;
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino = (struct inode*)buf + inum%IPB;
  if(ino->type != 0){
    ino->type = 0;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  }
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino = (struct inode*)malloc(sizeof(struct inode));
  // printf("\tim:get_inode %d\n", inum);

  /* 
   * your code goes here.
   */
  char buf[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  *ino = *((struct inode*)buf + inum%IPB);
  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  struct inode *ino = get_inode(inum);
  *size = ino->size;
  char buf[BLOCK_SIZE];
  char indirect_block[BLOCK_SIZE];
  int block_num = ino->size / BLOCK_SIZE;
  int block_remain = ino->size % BLOCK_SIZE;
  if(block_remain)block_num =block_num + 1;
  else block_num =block_num ;
  *buf_out = (char *)malloc(block_num * BLOCK_SIZE);

  if(block_num > NDIRECT) 
    bm->read_block(ino->blocks[NDIRECT], indirect_block);
  for(int i = 0; i < block_num; ++i){
    if(i < NDIRECT){
      blockid_t block_id = ino->blocks[i];
      bm->read_block(block_id, buf);
      memcpy(*buf_out + i * BLOCK_SIZE, buf, BLOCK_SIZE);
    }
    else{
      blockid_t block_id = ((blockid_t*)indirect_block)[i - NDIRECT];
      bm->read_block(block_id, buf);
      memcpy(*buf_out + i * BLOCK_SIZE, buf, BLOCK_SIZE);
    }
  }
  ino->atime = time(NULL);
  free(ino);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  struct inode *ino = get_inode(inum);

  char indirect_block[BLOCK_SIZE];

  int block_num = size / BLOCK_SIZE;
  int block_remain = size % BLOCK_SIZE;
  if(block_remain)block_num =block_num + 1;
  else block_num =block_num ;

  int old_block_num = ino->size / BLOCK_SIZE;
  int old_block_remain = ino->size % BLOCK_SIZE;
  old_block_num = old_block_remain == 0 ? old_block_num : old_block_num + 1;
  char buf_aligned[BLOCK_SIZE * block_num];
  memcpy(buf_aligned, buf, size);

  if(old_block_num > NDIRECT){
      blockid_t indirect_block_id = ino->blocks[NDIRECT];
      bm->read_block(indirect_block_id, indirect_block);
    }
  if(block_num <= old_block_num){
    for(int i = 0; i < block_num; ++i){
      if(i < NDIRECT){
        blockid_t block_id = ino->blocks[i];
        bm->write_block(block_id, buf_aligned + i * BLOCK_SIZE);
      }
      else{
        blockid_t block_id = ((blockid_t*)indirect_block)[i - NDIRECT];
        bm->write_block(block_id, buf_aligned + i * BLOCK_SIZE);
      }
    }

    for(int i = block_num; i < old_block_num; ++i){
      if(i < NDIRECT){
        blockid_t block_id = ino->blocks[i];
        bm->free_block(block_id);
      }
      else{
        blockid_t block_id = ((blockid_t*)indirect_block)[i - NDIRECT];
        bm->free_block(block_id);
      }
    }
    if(block_num <= NDIRECT && old_block_num > NDIRECT){
      bm->free_block(ino->blocks[NDIRECT]);
    }
  }
  else{
    for(int i = 0; i < old_block_num; ++i){
      if(i < NDIRECT){
        blockid_t block_id = ino->blocks[i];
        bm->write_block(block_id, buf_aligned + i * BLOCK_SIZE);
      }
      else{
        blockid_t block_id = ((blockid_t*)indirect_block)[i - NDIRECT];
        bm->write_block(block_id, buf_aligned + i * BLOCK_SIZE);
      }
    }
    for(int i = old_block_num; i < block_num; ++i){
      blockid_t block_id = bm->alloc_block();
      // printf("alloc block %d\n",block_id);
      bm->write_block(block_id, buf_aligned + i * BLOCK_SIZE);
      if(i < NDIRECT)ino->blocks[i] = block_id;
      else((blockid_t*)indirect_block)[i - NDIRECT] = block_id;
    }

    if(block_num > NDIRECT){
      blockid_t indirect_block_id = old_block_num <= NDIRECT ? bm->alloc_block() : ino->blocks[NDIRECT];
      ino->blocks[NDIRECT] = indirect_block_id;
      bm->write_block(indirect_block_id, indirect_block);
    }
  }

  ino->size = size;
  ino->atime = time(NULL);
  ino->mtime = time(NULL);
  ino->ctime = time(NULL);
  put_inode(inum, ino);
  free(ino);
  return;
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /* note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *ino = get_inode(inum);
  if(!ino)return;
  a.atime = ino->atime;
  a.ctime = ino->ctime;
  a.mtime = ino->mtime;
  a.size = ino->size;
  a.type = ino->type;
  free(ino);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  struct inode *ino = get_inode(inum);
  char indirect_block[BLOCK_SIZE];
  if(!ino){
    cerr << "remove inexist inode!" <<endl;
    assert(0);
  }
  

  int block_num = ino->size / BLOCK_SIZE;
  int block_remain = ino->size % BLOCK_SIZE;
  block_num = block_remain == 0 ? block_num : block_num + 1;
  if(block_num > NDIRECT){
    blockid_t indirect_block_id = ino->blocks[NDIRECT];
    bm->read_block(indirect_block_id, indirect_block);
  }
  
  for(int i = 0; i < block_num; ++i){
    if(i < NDIRECT)
      bm->free_block(ino->blocks[i]);
    else
      bm->free_block(((blockid_t*)indirect_block)[i-NDIRECT]);
  }
  if(block_num > NDIRECT)
    bm->free_block(ino->blocks[NDIRECT]);

  ino->type = 0;
  ino->size = 0;
  put_inode(inum, ino);
  free(ino);

  return;
}
