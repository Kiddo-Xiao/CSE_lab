// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"
using namespace std;

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here
  // Your code here for Lab2A: recover data on startup
  tx_id = 0;//事务编号
  _persister->restore_logdata();//读取旧数据
  vector<chfs_command> logs = _persister->log_entries;
  vector<chfs_command> cmd_list;
  int log_num = 0;
  printf("[extent_Server]初始化-------------------\n");
  cout<<"log_num = "<<logs.size()<<endl;
  // for(chfs_command cmd:logs){//只有create和put类 没有begin和commit类？？？？？
  //   cout<<"log_num : "<<++log_num<< "————";
  //   cout<<"log_type = "<<cmd.type<<"------";
  //   switch (cmd.type)//delete
  //   {
  //   case chfs_command::CMD_BEGIN:
  //     /* code */
  //     printf("begin txid %lld\n",cmd.id);
  //     break;
  //   case chfs_command::CMD_COMMIT:
  //     /* code */
  //     printf("commit txid %lld\n",cmd.id);
  //     break;
  //   case chfs_command::CMD_CREATE:
  //     /* code */
  //     printf("create txid %lld\n",cmd.id);
  //     break;
  //   case chfs_command::CMD_PUT:
  //     /* code */
  //     printf("put txid %lld\n",cmd.id);
  //     break;
  //   case chfs_command::CMD_REMOVE:
  //     /* code */
  //     printf("remove txid %lld\n",cmd.id);
  //     break;    
  //   default:
  //     break;
  //   }
  // }
  for(int i = 0;i < logs.size();++i){
    if(logs[i].type == chfs_command::CMD_BEGIN){
      vector<chfs_command> transaction_middle;
      while(i < logs.size() - 1){
        i++;
        if(logs[i].type == chfs_command::CMD_COMMIT)
          break;
        else if(logs[i].type == chfs_command::CMD_BEGIN){
          i--;
          break;
        }
        else transaction_middle.push_back(logs[i]);
      }
      if(logs[i].type == chfs_command::CMD_COMMIT){
        for(chfs_command cmd : transaction_middle){
          cmd_list.push_back(cmd);
        }
      }
    }
    else 
      printf("!!!error in extent_server ini!!!\n");
  }
  for (chfs_command cmd : cmd_list){
    tx_id = cmd.id;
    if(cmd.type == chfs_command::CMD_CREATE){
      uint32_t type = stoi(cmd.content);
      uint32_t inode = im->alloc_inode(type);
    }
    else if(cmd.type == chfs_command::CMD_PUT){
      const char * cbuf = cmd.content.c_str();
      int size = cmd.content.size();
      im->write_file(cmd.inode_id, cbuf, size);
    }
    else if(cmd.type == chfs_command::CMD_REMOVE){
      im->remove_file(cmd.inode_id);
    }
  }
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");

  string s = to_string(type);
  chfs_command cmd(chfs_command::CMD_CREATE, tx_id, 0, s);
  _persister->append_log(cmd);

  id = im->alloc_inode(type);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;

  chfs_command cmd(chfs_command::CMD_PUT, tx_id, id, buf);
  _persister->append_log(cmd);
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: write %lld\n", id);

  chfs_command cmd(chfs_command::CMD_REMOVE, tx_id, id, "");
  _persister->append_log(cmd);

  id &= 0x7fffffff;
  im->remove_file(id);
 
  return extent_protocol::OK;
}

int extent_server::begin(uint64_t &tx_id_){
  tx_id_ = ++tx_id;
  chfs_command cmd(chfs_command::CMD_BEGIN, tx_id, 0, "");
  _persister->append_log(cmd);
  return extent_protocol::OK;
}

int extent_server::commit(uint64_t tx_id_){
  chfs_command cmd(chfs_command::CMD_COMMIT, tx_id, 0, "");
  _persister->append_log(cmd);
  return extent_protocol::OK;
}