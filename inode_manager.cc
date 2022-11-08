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
  static int inum = 0;//全局变量!
  for(int i = 0;i<INODE_NUM;++i){
      inum = (inum+1)%INODE_NUM;//inode[0]:root inode!
      inode_t *ino = get_inode(inum);
      if(!ino){//直接分配
        ino = (inode_t*)malloc(sizeof(inode_t));
        bzero(ino,sizeof(inode_t));//set to 000...
        ino->type = type;
        ino->atime = (unsigned int)time(NULL);
        ino->mtime = (unsigned int)time(NULL);
        ino->ctime = (unsigned int)time(NULL);
        put_inode(inum,ino);
        free(ino);
        break;
      }
      //已分配空间不用重新分配
      free(ino);
  }
  return inum;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t* ino = get_inode(inum);
  if(ino){
    ino->type = 0;
    put_inode(inum,ino);
    free(ino);
  }
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino;

  char buf[BLOCK_SIZE];
  if(inum < 0 || inum >= INODE_NUM){
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  

  ino = (struct inode*)malloc(sizeof(struct inode));

  memcpy(ino, buf, sizeof(struct inode));

  if (ino->type == 0) {
    return NULL;
  }

  return ino;
  // struct inode *ino, *ino_disk;
  // char buf[BLOCK_SIZE];

  // printf("\tim: get_inode %d\n", inum);

  // if (inum < 0 || inum >= INODE_NUM) {
  //   printf("\tim: inum out of range\n");
  //   return NULL;
  // }

  // bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // // printf("%s:%d\n", __FILE__, __LINE__);

  // ino_disk = (struct inode*)buf + inum%IPB;
  // if (ino_disk->type == 0) {
  //   printf("\tim: inode not exist\n");
  //   return NULL;
  // }

  // ino = (struct inode*)malloc(sizeof(struct inode));
  // *ino = *ino_disk;

  // return ino;
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

blockid_t
inode_manager::get_inode_to_datablock(inode_t* ino, uint32_t n){
    blockid_t ans;

    if(n < NDIRECT){
      ans = ino->blocks[n];
    }else if(n >= NDIRECT && n <MAXFILE){
      char buf[BLOCK_SIZE];
      bm->read_block(ino->blocks[NDIRECT],buf);
      ans = ((blockid_t*)buf)[n-NDIRECT];
    }else {
      cerr << "block num overflow (>= MAXFILE) !" << endl;
      assert(0);
    }
    return ans;
}

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
  int block_num = 0, block_remain = 0;
  char buf[BLOCK_SIZE];

  inode_t* ino = get_inode(inum);
  if(ino){
    *size = ino->size;//ino所表示的文件的size
    *buf_out = (char*)malloc(ino->size);
    block_num = *size / BLOCK_SIZE;
    block_remain = *size % BLOCK_SIZE;

    int i=0;
    for(i;i<block_num;++i){//文件完全包含的block
      bm->read_block(get_inode_to_datablock(ino,i),buf);
      memcpy(*buf_out+BLOCK_SIZE*i, buf, BLOCK_SIZE);
    }
    if(block_remain){
      bm->read_block(get_inode_to_datablock(ino,i),buf);
      memcpy(*buf_out+i*BLOCK_SIZE, buf, block_remain);
    }
    free(ino);
  }
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
  int block_num = 0, block_remain = 0;
  inode_t* ino = get_inode(inum);
  if(!ino){
    cerr << "get inode wrong!" << endl;
    assert(0);
  }
  if(ino->size>MAXFILE*BLOCK_SIZE || (unsigned int)size>MAXFILE*BLOCK_SIZE){
    cerr <<"size of buf to write overflow!"<< endl;
    assert(0);
  }
  int old_nblock = ino->size==0 ? 0:(ino->size-1)/BLOCK_SIZE+1;
  int new_nblock = size==0 ? 0:(size-1)/BLOCK_SIZE+1;
  if(old_nblock > new_nblock){//space redundancy:release
    for(int i = new_nblock ; i < old_nblock ; i++){
      bm->free_block(get_inode_to_datablock(ino,i));
    }
  }else if(old_nblock < new_nblock){//space lack:alloc
    for(int i=old_nblock;i<new_nblock;i++){
      if(i<NDIRECT)ino->blocks[i] = bm->alloc_block();
      else if(i>=NDIRECT&&i<MAXFILE){
        if(!ino->blocks[NDIRECT])ino->blocks[NDIRECT]=bm->alloc_block();
        char buf[BLOCK_SIZE];
        bm->read_block(ino->blocks[NDIRECT],buf);
        ((blockid_t *)buf)[i - NDIRECT] = bm->alloc_block();
        bm->write_block(ino->blocks[NDIRECT], buf); 
      }else {
        cerr << "alloc block overflow!" << endl;
        assert(0);
      }
    }
  }
  ino->size = size;
  ino->atime = (unsigned int)time(NULL);
  ino->ctime = (unsigned int)time(NULL);
  ino->mtime = (unsigned int)time(NULL);
  block_num = size/BLOCK_SIZE,block_remain = size%BLOCK_SIZE;
  int i=0;
  for(i;i<block_num;++i) bm->write_block(get_inode_to_datablock(ino,i),buf+i*BLOCK_SIZE);
  if(block_remain){
    char remain[BLOCK_SIZE];
    memcpy(remain,buf+i*BLOCK_SIZE,block_remain);
    bm->write_block(get_inode_to_datablock(ino,i),remain);
  }
  put_inode(inum,ino);
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
  inode_t* ino = get_inode(inum);
  if(!ino){
    cerr << "remove inexist inode!" <<endl;
    assert(0);
  }
  int block_num = ino->size == 0? 0 : (ino->size - 1)/BLOCK_SIZE + 1;
  for(int i = 0; i < block_num; ++i)bm->free_block(get_inode_to_datablock(ino, i));
  if(block_num > NDIRECT)bm->free_block(ino->blocks[NDIRECT]);
  bzero(ino, sizeof(inode_t));
  free_inode(inum),free(ino);
  return;
}
