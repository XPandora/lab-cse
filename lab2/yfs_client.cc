// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client()
{
    // ec = new extent_client();
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

void yfs_client::acquirelock(inum inum)
{
    lc->acquire(inum);
}

void yfs_client::releaselock(inum inum)
{
    lc->release(inum);
}

void yfs_client::acquireBitmap()
{
    lc->acquire(0);
}

void yfs_client::releaseBitmap()
{
    lc->release(0);
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool yfs_client::isfile(inum inum)
{
    acquirelock(inum);
    bool r = isfile_l(inum);
    releaselock(inum);
    return r;
}

bool yfs_client::isfile_l(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE)
    {
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

bool yfs_client::isdir(inum inum)
{
    acquirelock(inum);
    bool r = isdir_l(inum);
    releaselock(inum);
    return r;
}

bool yfs_client::isdir_l(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR)
    {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    }
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

bool yfs_client::issymlink(inum inum)
{
    acquirelock(inum);
    bool r = issymlink_l(inum);
    releaselock(inum);
    return r;
}

bool yfs_client::issymlink_l(inum inum)
{
    return (!isfile_l(inum)) | (!isdir_l(inum));
}

int yfs_client::getfile(inum inum, fileinfo &fin)
{
    acquirelock(inum);
    int r = getfile_l(inum, fin);
    releaselock(inum);
    return r;
}

int yfs_client::getfile_l(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
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

int yfs_client::getdir(inum inum, dirinfo &din)
{
    acquirelock(inum);
    int r = getdir_l(inum, din);
    releaselock(inum);
    return r;
}

int yfs_client::getdir_l(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

int yfs_client::getsymlink(inum inum, symlinkinfo &sin)
{
    acquirelock(inum);
    int r = getsymlink_l(inum, sin);
    releaselock(inum);
    return r;
}

int yfs_client::getsymlink_l(inum inum, symlinkinfo &sin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    sin.atime = a.atime;
    sin.mtime = a.mtime;
    sin.ctime = a.ctime;
    sin.size = a.size;
    printf("getsymlink %016llx -> sz %llu\n", inum, sin.size);

release:
    return r;
}

#define EXT_RPC(xx)                                                \
    do                                                             \
    {                                                              \
        if ((xx) != extent_protocol::OK)                           \
        {                                                          \
            printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
            r = IOERR;                                             \
            goto release;                                          \
        }                                                          \
    } while (0)

// Only support set size of attr
int yfs_client::setattr(inum ino, size_t size)
{
    acquirelock(ino);
    int r = setattr_l(ino, size);
    releaselock(ino);
    return r;
}

int yfs_client::setattr_l(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string file_content;
    if (ec->get(ino, file_content) != extent_protocol::OK)
    {
        printf("\tsetattr:ec get error!\n");
        return IOERR;
    }

    file_content.resize(size);
    if (ec->put(ino, file_content) != extent_protocol::OK)
    {
        printf("\tsetattr:ec put error!\n");
        return IOERR;
    }

    return r;
}

int yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    acquirelock(parent);
    int r = create_l(parent, name, mode, ino_out);
    releaselock(parent);
    return r;
}

int yfs_client::create_l(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    if (lookup_l(parent, name, found, ino_out) != OK)
    {
        printf("\tcreate:lookup error!\n");
        return IOERR;
    }

    if (found)
    {
        printf("\tcreate:same dir name found!\n");
        return EXIST;
    }

    acquireBitmap();
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK)
    {
        printf("\tcreate::ec create error!\n");
        releaseBitmap();
        return IOERR;
    }
    releaseBitmap();

    dirent dir_pair;
    dir_pair.name = name;
    dir_pair.inum = ino_out;

    if (addDirent_l(parent, dir_pair) != OK)
    {
        printf("\tcreate::addDirent error!\n");
        return IOERR;
    }

    return r;
}
int yfs_client::addDirent(inum inode, dirent dir_pair)
{
    acquirelock(inode);
    int r = addDirent_l(inode, dir_pair);
    releaselock(inode);
    return r;
}

int yfs_client::addDirent_l(inum inode, dirent dir_pair)
{
    int r = OK;

    std::list<dirent> dir_list;
    if (readdir_l(inode, dir_list) != OK)
    {
        printf("\taddDirent:readdir error!\n");
        return IOERR;
    }
    dir_list.push_back(dir_pair);
    std::stringstream ss_dir;
    std::list<dirent>::iterator dir_iter = dir_list.begin();
    while (dir_iter != dir_list.end())
    {
        ss_dir << dir_iter->name;
        ss_dir << '\0';
        ss_dir << dir_iter->inum;
        //ss_dir << '\n';
        dir_iter++;
    }
    if (ec->put(inode, ss_dir.str()) != extent_protocol::OK)
    {
        printf("\taddDirent: ec put error!\n");
        return IOERR;
    }
    return r;
}

int yfs_client::deleteDirent(inum inode, const char *name)
{
    acquirelock(inode);
    int r = deleteDirent_l(inode, name);
    releaselock(inode);
    return r;
}

int yfs_client::deleteDirent_l(inum inode, const char *name)
{
    int r = OK;

    std::list<dirent> dir_list;
    if (readdir_l(inode, dir_list) != OK)
    {
        printf("\taddDirent:readdir error!\n");
        return IOERR;
    }
    std::stringstream ss_dir;
    std::list<dirent>::iterator dir_iter = dir_list.begin();
    while (dir_iter != dir_list.end())
    {
        if (dir_iter->name == name)
        {
            dir_iter++;
            continue;
        }
        ss_dir << dir_iter->name;
        ss_dir << '\0';
        ss_dir << dir_iter->inum;
        //ss_dir << '\n';
        dir_iter++;
    }
    if (ec->put(inode, ss_dir.str()) != extent_protocol::OK)
    {
        printf("\taddDirent: ec put error!\n");
        return IOERR;
    }
    return r;
}

int yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    acquirelock(parent);
    int r = mkdir_l(parent, name, mode, ino_out);
    releaselock(parent);
    return r;
}

int yfs_client::mkdir_l(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found = false;
    if (lookup_l(parent, name, found, ino_out) != OK)
    {
        printf("\tmkdir:lookup error!\n");
        return IOERR;
    }

    if (found)
    {
        printf("\tmkdir:same dir name found!\n");
        return EXIST;
    }

    acquireBitmap();
    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK)
    {
        printf("\tmkdir:ec create error!\n");
        releaseBitmap();
        return IOERR;
    }
    releaseBitmap();

    dirent dir_pair;
    dir_pair.name = name;
    dir_pair.inum = ino_out;

    if (addDirent_l(parent, dir_pair) != OK)
    {
        printf("\tmkdir:addDirent error!\n");
        return IOERR;
    }

    return r;
}

int yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    acquirelock(parent);
    int r = lookup_l(parent, name, found, ino_out);
    releaselock(parent);
    return r;
}

int yfs_client::lookup_l(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::list<dirent> list;
    if (readdir_l(parent, list) != OK)
    {
        printf("\tlookup:readdir error!\n");
        return IOERR;
    }
    found = false;

    std::list<dirent>::iterator dirent_iter = list.begin();
    while (dirent_iter != list.end())
    {
        dirent dir_pair = *dirent_iter;
        if (dir_pair.name == name)
        {
            found = true;
            ino_out = dir_pair.inum;
            break;
        }
        dirent_iter++;
    }

    return r;
}

int yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    acquirelock(dir);
    int r = readdir_l(dir, list);
    releaselock(dir);
    return r;
}

int yfs_client::readdir_l(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string all_dir_name;
    dirent entry;
    if (ec->get(dir, all_dir_name) != extent_protocol::OK)
    {
        return IOERR;
    }

    list.clear();
    std::istringstream ss_dir(all_dir_name);
    while (getline(ss_dir, entry.name, '\0'))
    {
        ss_dir >> entry.inum;
        list.push_back(entry);
    }

    return r;
}

int yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    acquirelock(ino);
    int r = read_l(ino, size, off, data);
    releaselock(ino);
    return r;
}

int yfs_client::read_l(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string file_content;
    if (ec->get(ino, file_content) != extent_protocol::OK)
    {
        printf("\tread:ec get error!\n");
        return IOERR;
    }
    data = file_content.substr(off, size);

    return r;
}

int yfs_client::write(inum ino, size_t size, off_t off, const char *data,
                      size_t &bytes_written)
{
    acquirelock(ino);
    int r = write_l(ino, size, off, data, bytes_written);
    releaselock(ino);
    return r;
}

int yfs_client::write_l(inum ino, size_t size, off_t off, const char *data,
                        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    std::string file_content;
    if (ec->get(ino, file_content) != extent_protocol::OK)
    {
        printf("\twrite:ec get error!\n");
        return IOERR;
    }

    if ((unsigned)off > file_content.size())
    {
        std::string replace_string = data;
        replace_string.resize(size);
        file_content.resize(off, '\0');
        file_content.replace(off, size, replace_string);
    }
    else
    {
        file_content.replace(off, size, data, size);
    }
    
    acquireBitmap();
    if (ec->put(ino, file_content) != extent_protocol::OK)
    {
        printf("\twrite:ec put error!\n");
        releaseBitmap();
        return IOERR;
    }
    releaseBitmap();

    return r;
}

int yfs_client::unlink(inum parent, const char *name)
{
    acquirelock(parent);
    int r = unlink_l(parent, name);
    releaselock(parent);
    return r;
}

int yfs_client::unlink_l(inum parent, const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found;
    inum remove_ino;
    if (lookup_l(parent, name, found, remove_ino) != OK)
    {
        printf("\tunlink:unlink error!\n");
        return IOERR;
    }

    if (!found)
    {
        printf("\tunlink:no such file found!\n");
        return NOENT;
    }

    acquireBitmap();
    if (ec->remove(remove_ino) != extent_protocol::OK)
    {
    
        printf("\tunlink:ec remove error!\n");
        releaseBitmap();
        return IOERR;
    }
    releaseBitmap();

    if (deleteDirent_l(parent, name) != OK)
    {
        printf("\tunlink:delete dirent error!\n");
        return IOERR;
    }

    return r;
}

int yfs_client::symlink(inum parent, const char *dir, const char *name, inum &ino)
{
    acquirelock(parent);
    int r = symlink_l(parent, dir, name, ino);
    releaselock(parent);
    return r;
}

int yfs_client::symlink_l(inum parent, const char *dir, const char *name, inum &ino)
{
    int r = OK;

    acquireBitmap();
    if (ec->create(extent_protocol::T_SYMLINK, ino) != extent_protocol::OK)
    {
        printf("\tsymlink:ec create error!\n");
        releaseBitmap();
        return IOERR;
    }

    if (ec->put(ino, dir) != extent_protocol::OK)
    {
        printf("\tsymlink:ec put error!\n");
        releaseBitmap();
        return IOERR;
    }
    releaseBitmap();
    
    dirent dir_pair;
    dir_pair.name = name;
    dir_pair.inum = ino;

    if (addDirent_l(parent, dir_pair) != OK)
    {
        printf("\tsymlink:addDirent error!\n");
        return IOERR;
    }

    return r;
}

int yfs_client::readlink(inum ino, std::string &file_path)
{
    acquirelock(ino);
    int r = readlink_l(ino, file_path);
    releaselock(ino);
    return r;
}

int yfs_client::readlink_l(inum ino, std::string &file_path)
{
    int r = OK;
    if (ec->get(ino, file_path) != extent_protocol::OK)
    {
        printf("\treadlink:ec get error!\n");
        return IOERR;
    }

    return r;
}
