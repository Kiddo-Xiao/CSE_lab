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

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);
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
    string buf;
    ec->begin(tx_id_);//code for lab2A
    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    lc->acquire(ino);
    if (ec->get(ino, buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
    buf.resize(size);
    //Number of characters the %string should contain. 
    // This function will resize the %string to the specified length. 
    // If the new size is smaller than the %string's current size the %string is truncated, 
    // otherwise the %string is extended and new characters are default-constructed. 
    // For basic types such as char, this means setting them to 0.
    if (ec->put(ino, buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
    ec->commit(tx_id_);//code for lab2A
    lc->release(ino);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    //tips for lab2B:
    //chfs_client 应该在开始 CREATE 之前获取目录上的锁，
    //并且只有在完成将新信息写回范围服务器后才释放锁。
    //如果有并发操作，锁会强制两个操作之一延迟，直到另一个操作完成。
    //所有 chfs_client 必须从同一个锁服务器获取锁。
    int r = OK;
    uint64_t tx_id_;//code for lab2A
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found ;
    string buf;
    lc->acquire(parent);
    if (!isdir(parent)) {
        r = NOENT;
        lc->release(parent);
        return r;
    }
    lookup(parent, name, found, ino_out);
    if(found){
        r = EXIST;
        lc->release(parent);
        return r;
    }

    ec->begin(tx_id_);//code for lab2A
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }

    if (ec->get(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    buf.append(string(name) + ":" + filename(ino_out) + "/");// add a dir pair into parent dir
    if (ec->put(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    ec->commit(tx_id_);//code for lab2A
    lc->release(parent);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    uint64_t tx_id_;//code for lab2A
    bool found;
    string buf;
    ec->begin(tx_id_);//code for lab2A
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    lc->acquire(parent);
    if (!isdir(parent)) {
        r = NOENT;
        lc->release(parent);
        return r;
    }
    lookup(parent, name, found, ino_out);
    if(found){
        r = EXIST;
        lc->release(parent);
        return r;
    }
    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    if (ec->get(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    buf.append(string(name) + ":" + filename(ino_out) + "/");
    if (ec->put(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    ec->commit(tx_id_);//code for lab2A
    lc->release(parent);
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
    if (!isdir(parent)) {
        r = NOENT;
        return r;
    }
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
    if (!isdir(dir)) {
        r = NOENT;
        return r;
    }
    if (ec->get(dir, buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
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
    if (!isfile(ino)) {
        r = NOENT;
        return r;
    }
    if (ec->get(ino, buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
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
    string buf;
    ec->begin(tx_id_);//code for lab2A
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    lc->acquire(ino);
    if (!isfile(ino)) {
        r = NOENT;
        lc->release(ino);
        return r;
    }
    if (ec->get(ino, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(ino);
        return r;
    }
    int write_size = off + size;
    if(write_size > buf.size())buf.resize(write_size);//file size < write_size
    for(int i=off;i<write_size;++i)buf[i]=data[i-off];
    bytes_written=size;
    if (ec->put(ino, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(ino);
        return r;
    }
    ec->commit(tx_id_);//code for lab2A
    lc->release(ino);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    bool found;
    string buf;
    inum file_inum;
    uint64_t tx_id_;//code for lab2A
    ec->begin(tx_id_);//code for lab2A
    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    lc->acquire(parent);
    
    lookup(parent, name, found, file_inum);
    if(!found){
        r = NOENT;
        lc->release(parent);
        return r;
    }
    if(ec->remove(file_inum) != OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    //update parent dir:
    if (ec->get(parent, buf) != extent_protocol::OK){
        r = IOERR;
        lc->release(parent);
        return r;
    }
    int sta = buf.find(name);
    int end = buf.find('/',sta);
    buf.erase(sta,end-sta+1);
    if(ec->put(parent, buf) != OK){
        r = IOERR;
        lc->release(parent);
        return r;    
    }
    ec->commit(tx_id_);//code for lab2A
    lc->release(parent);
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