#pragma once
#include "params.h"
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#include "log.h"
#include <assert.h>
// this file is for function that we dont use or dont need to modify

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
// Undocumented but extraordinarily useful fact:  the fuse_context is
// set up before this function is called, and
// fuse_get_context()->private_data returns the user_data passed to
// fuse_main().  Really seems like either it should be a third
// parameter coming in here, or else the fact should be documented
// (and this might as well return void, as it did in older versions of
// FUSE).
void *bb_init(struct fuse_conn_info *conn)
{
    log_msg("\nbb_init()\n");

    log_conn(conn);
    log_fuse_context(fuse_get_context());

    // return BB_DATA;
}

/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */
void bb_destroy(void *userdata)
{
    log_msg("\nbb_destroy(userdata=0x%08x)\n", userdata);
}


//  All the paths I see are relative to the root of the mounted
//  filesystem.  In order to get to the underlying filesystem, I need to
//  have the mountpoint.  I'll save it away early on in main(), and then
//  whenever I need a path for something I'll call this to construct
//  it.
static void bb_fullpath(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, BB_DATA->rootdir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will
                                    // break here

    log_msg("    bb_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
            BB_DATA->rootdir, path, fpath);
}

/** Read the target of a symbolic link
 *
 * The buffer should be filled with a null terminated string.  The
 * buffer size argument includes the space for the terminating
 * null character.  If the linkname is too long to fit in the
 * buffer, it should be truncated.  The return value should be 0
 * for success.
 */
// Note the system readlink() will truncate and lose the terminating
// null.  So, the size passed to to the system readlink() must be one
// less than the size passed to bb_readlink()
// bb_readlink() code by Bernardo F Costa (thanks!)
int bb_readlink(const char *path, char *link, size_t size)
{
    assert(0);
    // int retstat;
    // char fpath[PATH_MAX];

    // log_msg("\nbb_readlink(path=\"%s\", link=\"%s\", size=%d)\n",
    //         path, link, size);
    // bb_fullpath(fpath, path);

    // retstat = log_syscall("readlink", readlink(fpath, link, size - 1), 0);
    // if (retstat >= 0) {
    //     link[retstat] = '\0';
    //     retstat = 0;
    //     log_msg("    link=\"%s\"\n", link);
    // }

    // return retstat;
    return -1;
}

/** Create a directory */
int bb_mkdir(const char *path, mode_t mode)
{
    assert(0);
    // char fpath[PATH_MAX];

    // log_msg("\nbb_mkdir(path=\"%s\", mode=0%3o)\n",
    //         path, mode);
    // bb_fullpath(fpath, path);

    // return log_syscall("mkdir", mkdir(fpath, mode), 0);
    return -1;
}


/** Remove a directory */
int bb_rmdir(const char *path)
{
    assert(0);
    // char fpath[PATH_MAX];

    // log_msg("bb_rmdir(path=\"%s\")\n",
    //         path);
    // bb_fullpath(fpath, path);

    // return log_syscall("rmdir", rmdir(fpath), 0);

    return -1;
}


/** Create a symbolic link */
// The parameters here are a little bit confusing, but do correspond
// to the symlink() system call.  The 'path' is where the link points,
// while the 'link' is the link itself.  So we need to leave the path
// unaltered, but insert the link into the mounted directory.
int bb_symlink(const char *path, const char *link)
{
    assert(0);
    // char flink[PATH_MAX];

    // log_msg("\nbb_symlink(path=\"%s\", link=\"%s\")\n",
    //         path, link);
    // bb_fullpath(flink, link);

    // return log_syscall("symlink", symlink(path, flink), 0);
    return -1;
}

/** Rename a file */
// both path and newpath are fs-relative
int bb_rename(const char *path, const char *newpath)
{
    assert(0);
    // char fpath[PATH_MAX];
    // char fnewpath[PATH_MAX];

    // log_msg("\nbb_rename(fpath=\"%s\", newpath=\"%s\")\n",
    //         path, newpath);
    // bb_fullpath(fpath, path);
    // bb_fullpath(fnewpath, newpath);

    // return log_syscall("rename", rename(fpath, fnewpath), 0);
    return -1;
}

/** Create a hard link to a file */
int bb_link(const char *path, const char *newpath)
{
    assert(0);
    // char fpath[PATH_MAX], fnewpath[PATH_MAX];

    // log_msg("\nbb_link(path=\"%s\", newpath=\"%s\")\n",
    //         path, newpath);
    // bb_fullpath(fpath, path);
    // bb_fullpath(fnewpath, newpath);

    // return log_syscall("link", link(fpath, fnewpath), 0);
    return -1;
}

/** Change the permission bits of a file */
int bb_chmod(const char *path, mode_t mode)
{
    assert(0);
    // char fpath[PATH_MAX];

    // log_msg("\nbb_chmod(fpath=\"%s\", mode=0%03o)\n",
    //         path, mode);
    // bb_fullpath(fpath, path);

    // return log_syscall("chmod", chmod(fpath, mode), 0);
    return -1;
}

/** Change the owner and group of a file */
int bb_chown(const char *path, uid_t uid, gid_t gid)

{
    assert(0);
    // char fpath[PATH_MAX];

    // log_msg("\nbb_chown(path=\"%s\", uid=%d, gid=%d)\n",
    //         path, uid, gid);
    // bb_fullpath(fpath, path);

    // return log_syscall("chown", chown(fpath, uid, gid), 0);
    return -1;
}

/** Change the size of a file */
int bb_truncate(const char *path, off_t newsize)
{
    char fpath[PATH_MAX];

    log_msg("\nbb_truncate(path=\"%s\", newsize=%lld)\n",
            path, newsize);
    bb_fullpath(fpath, path);

    return log_syscall("truncate", truncate(fpath, newsize), 0);
}



/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 * Replaced 'struct statfs' parameter with 'struct statvfs' in
 * version 2.5
 */
int bb_statfs(const char *path, struct statvfs *statv)
{
    assert(0);
    // int retstat = 0;
    // char fpath[PATH_MAX];

    // log_msg("\nbb_statfs(path=\"%s\", statv=0x%08x)\n",
    //         path, statv);
    // bb_fullpath(fpath, path);

    // // get stats for underlying filesystem
    // retstat = log_syscall("statvfs", statvfs(fpath, statv), 0);

    // log_statvfs(statv);

    // return retstat;
    return -1;
}

/** Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * Changed in version 2.2
 */
int bb_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    assert(0);
//     log_msg("\nbb_fsync(path=\"%s\", datasync=%d, fi=0x%08x)\n",
//             path, datasync, fi);
//     log_fi(fi);

//     // some unix-like systems (notably freebsd) don't have a datasync call
// #ifdef HAVE_FDATASYNC
//     if (datasync)
//         return log_syscall("fdatasync", fdatasync(fi->fh), 0);
//     else
// #endif
//         return log_syscall("fsync", fsync(fi->fh), 0);
    return -1;
}


#ifdef HAVE_SYS_XATTR_H
/** Note that my implementations of the various xattr functions use
    the 'l-' versions of the functions (eg bb_setxattr() calls
    lsetxattr() not setxattr(), etc).  This is because it appears any
    symbolic links are resolved before the actual call takes place, so
    I only need to use the system-provided calls that don't follow
    them */

/** Set extended attributes */
int bb_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
    assert(0);
    // char fpath[PATH_MAX];

    // log_msg("\nbb_setxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d, flags=0x%08x)\n",
    //         path, name, value, size, flags);
    // bb_fullpath(fpath, path);

    // return log_syscall("lsetxattr", lsetxattr(fpath, name, value, size, flags), 0);
    return -1;
}

/** List extended attributes */
int bb_listxattr(const char *path, char *list, size_t size)
{
    assert(0);
    // int retstat = 0;
    // char fpath[PATH_MAX];
    // char *ptr;

    // log_msg("\nbb_listxattr(path=\"%s\", list=0x%08x, size=%d)\n",
    //         path, list, size);
    // bb_fullpath(fpath, path);

    // retstat = log_syscall("llistxattr", llistxattr(fpath, list, size), 0);
    // if (retstat >= 0)
    // {
    //     log_msg("    returned attributes (length %d):\n", retstat);
    //     if (list != NULL)
    //         for (ptr = list; ptr < list + retstat; ptr += strlen(ptr) + 1)
    //             log_msg("    \"%s\"\n", ptr);
    //     else
    //         log_msg("    (null)\n");
    // }

    // return retstat;
    return -1;
}

/** Remove extended attributes */
int bb_removexattr(const char *path, const char *name)
{
    assert(0);
    // char fpath[PATH_MAX];

    // log_msg("\nbb_removexattr(path=\"%s\", name=\"%s\")\n",
    //         path, name);
    // bb_fullpath(fpath, path);

    // return log_syscall("lremovexattr", lremovexattr(fpath, name), 0);
    return -1;
}
#endif

/** Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 *
 * Introduced in version 2.3
 */
// when exactly is this called?  when a user calls fsync and it
// happens to be a directory? ??? >>> I need to implement this...
int bb_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi)
{
    assert(0);
    // int retstat = 0;

    // log_msg("\nbb_fsyncdir(path=\"%s\", datasync=%d, fi=0x%08x)\n",
    //         path, datasync, fi);
    // log_fi(fi);

    // return retstat;
    return -1;
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
// Not implemented.  I had a version that used creat() to create and
// open the file, which it turned out opened the file write-only.

/**
 * Change the size of an open file
 *
 * This method is called instead of the truncate() method if the
 * truncation was invoked from an ftruncate() system call.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the truncate() method will be
 * called instead.
 *
 * Introduced in version 2.5
 */
int bb_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
    assert(0);
    // int retstat = 0;

    // log_msg("\nbb_ftruncate(path=\"%s\", offset=%lld, fi=0x%08x)\n",
    //         path, offset, fi);
    // log_fi(fi);

    // retstat = ftruncate(fi->fh, offset);
    // if (retstat < 0)
    //     retstat = log_error("bb_ftruncate ftruncate");

    // return retstat;
    return -1;
}

/**
 * Get attributes from an open file
 *
 * This method is called instead of the getattr() method if the
 * file information is available.
 *
 * Currently this is only called after the create() method if that
 * is implemented (see above).  Later it may be called for
 * invocations of fstat() too.
 *
 * Introduced in version 2.5
 */
int bb_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
    assert(0);
    // int retstat = 0;

    // log_msg("\nbb_fgetattr(path=\"%s\", statbuf=0x%08x, fi=0x%08x)\n",
    //         path, statbuf, fi);
    // log_fi(fi);

    // // On FreeBSD, trying to do anything with the mountpoint ends up
    // // opening it, and then using the FD for an fgetattr.  So in the
    // // special case of a path of "/", I need to do a getattr on the
    // // underlying root directory instead of doing the fgetattr().
    // if (!strcmp(path, "/"))
    //     return bb_getattr(path, statbuf);

    // retstat = fstat(fi->fh, statbuf);
    // if (retstat < 0)
    //     retstat = log_error("bb_fgetattr fstat");

    // log_stat(statbuf);

    // return retstat;
    return -1;
}

/** Possibly flush cached data
 *
 * BIG NOTE: This is not equivalent to fsync().  It's not a
 * request to sync dirty data.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 *
 * NOTE: The flush() method may be called more than once for each
 * open().  This happens if more than one file descriptor refers
 * to an opened file due to dup(), dup2() or fork() calls.  It is
 * not possible to determine if a flush is final, so each flush
 * should be treated equally.  Multiple write-flush sequences are
 * relatively rare, so this shouldn't be a problem.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * Changed in version 2.2
 */
// this is a no-op in BBFS.  It just logs the call and returns success
int bb_flush(const char *path, struct fuse_file_info *fi)
{
    log_msg("\nbb_flush(path=\"%s\", fi=0x%08x)\n", path, fi);
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);

    return 0;
}

void bb_usage()
{
    fprintf(stderr, "usage:  bbfs [FUSE and mount options] rootDir mountPoint\n");
    abort();
}


/** Create a file node
 *
 * There is no create() operation, mknod() will be called for
 * creation of all non-directory, non-symlink nodes.
 */
// shouldn't that comment be "if" there is no.... ?
int bb_mknod(const char *path, mode_t mode, dev_t dev)
{
    int retstat;
    char fpath[PATH_MAX];

    log_msg("\nbb_mknod(path=\"%s\", mode=0%3o, dev=%lld)\n",
            path, mode, dev);
    // bb_fullpath(fpath, path);

    // On Linux this could just be 'mknod(path, mode, dev)' but this
    // tries to be be more portable by honoring the quote in the Linux
    // mknod man page stating the only portable use of mknod() is to
    // make a fifo, but saying it should never actually be used for
    // that.
    if (S_ISREG(mode)) {
        retstat = log_syscall("open", open(fpath, O_CREAT | O_EXCL | O_WRONLY, mode), 0);
        if (retstat >= 0)
            retstat = log_syscall("close", close(retstat), 0);
    }
    else if (S_ISFIFO(mode))
        retstat = log_syscall("mkfifo", mkfifo(fpath, mode), 0);
    else
        retstat = log_syscall("mknod", mknod(fpath, mode, dev), 0);

    return retstat;
}

