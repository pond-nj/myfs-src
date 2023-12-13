#include "client.h"

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
// I don't fully understand the documentation above -- it doesn't
// match the documentation for the read() system call which says it
// can return with anything up to the amount of data requested. nor
// with the fusexmp code which returns the amount of data also
// returned by read.

int bb_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;

    log_msg("\nbb_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
            path, buf, size, offset, fi);
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);

    return log_syscall("client_read", client_read(path, fi->fh, buf, size, offset), 0);
}

/** Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 *
 * Changed in version 2.2
 */
// As  with read(), the documentation above is inconsistent with the
// documentation for the write() system call.
int bb_write(const char *path, const char *buf, size_t size, off_t offset,
             struct fuse_file_info *fi)
{
    int retstat = 0;

    log_msg("\nbb_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
            path, buf, size, offset, fi);
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);

    return log_syscall("client_write", client_write(path, fi->fh, buf, size, offset), 0);
}

int bb_open(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    int fd;

    log_msg("\nbb_open(path\"%s\", fi=0x%08x), open_flag=%o\n",
            path, fi, fi->flags);

    // if the open call succeeds, my retstat is the file descriptor,
    // else it's -errno.  I'm making sure that in that case the saved
    // file descriptor is exactly -1.
    fd = log_syscall("client_open", 0, 0);
    if (fd < 0)
        retstat = log_error("open");

    // save file descriptor to bbfs
    fi->fh = fd;

    log_fi(fi);

    return retstat;
}

int bb_getattr(const char *path, struct stat *statbuf)
{
    // for some reason it keeps calling "/.git**"
    if(path[1] != '.'){
        int retstat;

        log_msg("\nbb_getattr(path=\"%s\", statbuf=0x%08x)\n",
                path, statbuf);

        // char* tmp = "/home/csci5550/CSCI5550-project/example";
        // retstat = log_syscall("client_lstat", stat(tmp, statbuf), 0);

        retstat = log_syscall("client_lstat", client_lstat(path, statbuf), 0);

        log_stat(statbuf);

        return retstat;
    }else{
        log_msg("\nbb_getattr(path=\"%s\", statbuf=0x%08x)\n",
                path, statbuf);
        return -1;
    }
}

int bb_opendir(const char *path, struct fuse_file_info *fi)
{
    printf("path: %s\n", path);
    if(strcmp(path, "/") == 0){
        // Pond: Usually, opendir is called to get DIR* for subsequent readdir call.
        // In our case, it will be used to read mount_dir
        // We don't need this in myfs

        log_msg("\nbb_opendir(path=\"%s\", fi=0x%08x)\n",
                path, fi);

        int retstat = log_syscall("opendir", 0, 0);

        log_fi(fi);

        return retstat;
    }else{
        // sometime it calls to opendir /.git/HEAD, i dont know why
        // only allow open to "/" which is the mountdir??
        return -1;

    }
}

int bb_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
               struct fuse_file_info *fi)
{
    assert(strcmp(path, "/") == 0);
    log_msg("\nbb_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n",
            path, buf, filler, offset, fi);

    for(int i=0; i<BB_DATA->metadata.count_files; i++)
    {
        log_msg("calling filler with name %s\n", BB_DATA->metadata.files[i].name);
        if (filler(buf, BB_DATA->metadata.files[i].name, NULL, 0) != 0)
        {
            log_msg("    ERROR bb_readdir filler:  buffer full");
            return -ENOMEM;
        }
    }

    log_fi(fi);

    return 0;
}

int bb_releasedir(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;

    log_msg("\nbb_releasedir(path=\"%s\", fi=0x%08x)\n",
            path, fi);
    log_fi(fi);

    // DO NOTHING
    // closedir((DIR *)(uintptr_t)fi->fh);

    return retstat;
}

int bb_access(const char *path, int mask)
{
    int retstat = 0;

    log_msg("\nbb_access(path=\"%s\", mask=0%o)\n",
            path, mask);

    retstat = 0;

    if (retstat < 0)
        retstat = log_error("bb_access access");

    return retstat;
}


int bb_utime(const char *path, struct utimbuf *ubuf)
{
    log_msg("\nbb_utime(path=\"%s\", ubuf=0x%08x)\n",
            path, ubuf);

    // return log_syscall("utime", utime("/home/csci5550/CSCI5550-project/example", ubuf), 0);
    return log_syscall("utime", 0, 0);
}

/** Get extended attributes */
int bb_getxattr(const char *path, const char *name, char *value, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nbb_getxattr(path = \"%s\", name = \"%s\", value = 0x%08x, size = %d)\n",
	    path, name, value, size);
    bb_fullpath(fpath, path);

    retstat = log_syscall("lgetxattr", 0, 0);
    if (retstat >= 0)
	log_msg("    value = \"%s\"\n", value);
    
    return retstat;
}

int bb_release(const char *path, struct fuse_file_info *fi)
{
    log_msg("\nbb_release(path=\"%s\", fi=0x%08x)\n",
            path, fi);
    log_fi(fi);

    // We need to close the file.  Had we allocated any resources
    // (buffers etc) we'd need to free them here as well.
    return log_syscall("close", 0, 0);
}