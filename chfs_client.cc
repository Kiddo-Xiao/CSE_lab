// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

/* 
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create', 
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log 
 * to achive all-or-nothing for these transactions.
 */

using namespace std;
chfs_client::chfs_client()
{
    ec = new extent_client();
}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    } 
    printf("issymlink: %lld is not a symlink\n", inum);
    return false;
}
bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("isdir: %lld is not a dir\n", inum);
    return false;
    // return ! isfile(inum);
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    uint64_t tx_id_;//code for lab2A
    ec->begin(tx_id_);//code for lab2A
    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    string buf;
    ec->get(ino,buf);
    buf.resize(size);
    //Number of characters the %string should contain. 
    // This function will resize the %string to the specified length. 
    // If the new size is smaller than the %string's current size the %string is truncated, 
    // otherwise the %string is extended and new characters are default-constructed. 
    // For basic types such as char, this means setting them to 0.
    ec->put(ino,buf);
    ec->commit(tx_id_);//code for lab2A
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    uint64_t tx_id_;//code for lab2A
    inum file_inum;//if exist, =file inum
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found ;
    string buf;
    lookup(parent, name, found, file_inum);
    if (found)return EXIST;

    ec->begin(tx_id_);//code for lab2A
    ec->create(extent_protocol::T_FILE, ino_out);
    ec->get(parent, buf);
    buf.append(string(name) + ":" + filename(ino_out) + "/");// add a dir pair into parent dir
    ec->put(parent, buf);
    ec->commit(tx_id_);//code for lab2A
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    uint64_t tx_id_;//code for lab2A
    ec->begin(tx_id_);//code for lab2A
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found;
    inum file_inum;
    string buf;
    lookup(parent, name, found, file_inum);
    if(found)return EXIST;
    ec->create(extent_protocol::T_DIR, ino_out);
    ec->get(parent, buf);
    buf.append(string(name) + ":" + filename(ino_out) + "/");
    ec->put(parent, buf);
    ec->commit(tx_id_);//code for lab2A
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    list<dirent> dir_pair;//名称/inum 对
    readdir(parent,dir_pair);
    if(dir_pair.empty()){
        found = false;
        return r;
    }
    for(auto i = dir_pair.begin();i!=dir_pair.end();i++){
        if(i->name==name){
            found = true;
            ino_out = i->inum;
            return r;
        }
    }
    found = false;
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*将目录内容解析为一系列名称/inum 对
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    string buf;
    ec->get(dir,buf);
    // cerr <<"???????????"<<buf.length()<<' '<<buf<<endl;
    // cout dir format:"name:inum/name:inum/..."
    int name_sta = 0, name_end = buf.find(':');
    while (name_end != string::npos) {
        string name = buf.substr(name_sta, name_end - name_sta);
        int inum_sta = name_end + 1, inum_end = buf.find('/', inum_sta);
        string inum = buf.substr(inum_sta, inum_end - inum_sta);
        name_sta = inum_end + 1, name_end = buf.find(':', name_sta);  

        struct dirent single_pair;
        single_pair.name = name;
        single_pair.inum = n2i(inum);
        list.push_back(single_pair);
    }
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    string buf;
    ec->get(ino,buf);
    //从某个偏移量off开始最多读size bytes , 当可用的字节数少于size时只返回可用的字节数
    if(off>buf.size())data="";//偏移量 overflow
    else if(off+size>buf.size())data=buf.substr(off);//可用的字节数<size
    else data = buf.substr(off,size);//可用的字节数>=size
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    uint64_t tx_id_;//code for lab2A
    ec->begin(tx_id_);//code for lab2A
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    string buf;
    ec->get(ino,buf);
    int write_size = off + size;
    if(write_size > buf.size())buf.resize(write_size);//file size < write_size
    for(int i=off;i<write_size;++i)buf[i]=data[i-off];
    bytes_written=size;
    ec->put(ino,buf);
    ec->commit(tx_id_);//code for lab2A
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    uint64_t tx_id_;//code for lab2A
    ec->begin(tx_id_);//code for lab2A
    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found;
    string buf;
    inum file_inum;
    lookup(parent, name, found, file_inum);
    ec->remove(file_inum);
    //update parent dir:
    ec->get(parent,buf);
    int sta = buf.find(name);
    int end = buf.find('/',sta);
    buf.erase(sta,end-sta+1);
    ec->put(parent,buf);
    ec->commit(tx_id_);//code for lab2A
    return r;
}

int
chfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
    int r = OK;
    uint64_t tx_id_;//code for lab2A
    ec->begin(tx_id_);//code for lab2A 
    bool found;
    string buf;
    inum file_inum;
    lookup(parent, name, found, file_inum);
    if (found)return EXIST;

    ec->create(extent_protocol::T_SYMLINK, ino_out);
    ec->put(ino_out, string(link));
    ec->get(parent, buf);
    buf.append(std::string(name) + ":" + filename(ino_out) + "/");
    ec->put(parent, buf);
    ec->commit(tx_id_);//code for lab2A
    return r;
}

int
chfs_client::readlink(inum ino, string& data)
{
    int r = OK;
    string buf;
    ec->get(ino, buf);
    data = buf;
    return r;
}