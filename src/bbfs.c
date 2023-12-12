/*
  Big Brother File System
  Copyright (C) 2012 Joseph J. Pfeiffer, Jr., Ph.D. <pfeiffer@cs.nmsu.edu>

  This program can be distributed under the terms of the GNU GPLv3.
  See the file COPYING.

  This code is derived from function prototypes found /usr/include/fuse/fuse.h
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  His code is licensed under the LGPLv2.
  A copy of that code is included in the file fuse.h

  The point of this FUSE filesystem is to provide an introduction to
  FUSE.  It was my first FUSE filesystem as I got to know the
  software; hopefully, the comments in this code will help people who
  follow later to get a gentler introduction.

  This might be called a no-op filesystem:  it doesn't impose
  filesystem semantics on top of any other existing structure.  It
  simply reports the requests that come in, and passes them to an
  underlying filesystem.  The information is saved in a logfile named
  bbfs.log, in the directory from which you run bbfs.
*/
#include "config.h"
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

// old unmodified function from bbfs
#include "old_bbfs.h"

// new
#include "log.h"
#include "storage.h"
#include "client.h"
// #include "metadata.h"

// need to implement: (base on bbfs log simulating workload in fuse assignment)
// workload: read, write, list file in a directory
// bb_releasedir
// opendir
// getattr
// readdir
// bb_access
// bb_mknod?? what for
// bb_getxattr
// bb_write
// bb_flush
// bb_release
// bb_read
// bb_open

// TODO: fschk

// moved unused/unmodified function to this file
#include "old_bbfs.h"
// moved need to modify function to this file
#include "new_bbfs.h"


struct fuse_operations bb_oper = {
    .getattr = bb_getattr,
    .readlink = bb_readlink,
    // no .getdir -- that's deprecated
    .getdir = NULL,
    .mknod = bb_mknod,
    .mkdir = bb_mkdir,
    .unlink = bb_unlink,
    .rmdir = bb_rmdir,
    .symlink = bb_symlink,
    .rename = bb_rename,
    .link = bb_link,
    .chmod = bb_chmod,
    .chown = bb_chown,
    .truncate = bb_truncate,
    .utime = bb_utime,
    .open = bb_open,
    .read = bb_read,
    .write = bb_write,
    /** Just a placeholder, don't set */ // huh???
    .statfs = bb_statfs,
    .flush = bb_flush,
    .release = bb_release,
    .fsync = bb_fsync,

#ifdef HAVE_SYS_XATTR_H
    .setxattr = bb_setxattr,
    .getxattr = bb_getxattr,
    .listxattr = bb_listxattr,
    .removexattr = bb_removexattr,
#endif

    .opendir = bb_opendir,
    .readdir = bb_readdir,
    .releasedir = bb_releasedir,
    .fsyncdir = bb_fsyncdir,
    .init = bb_init,
    .destroy = bb_destroy,
    .access = bb_access,
    .ftruncate = bb_ftruncate,
    .fgetattr = bb_fgetattr};


// master mounts directory
int mount_fuse(int argc, char* argv[])
{
    printf("mount fuse\n");
    int fuse_stat;
    struct bb_state *bb_data;

    // See which version of fuse we're running
    fprintf(stderr, "Fuse library version %d.%d\n", FUSE_MAJOR_VERSION, FUSE_MINOR_VERSION);

    bb_data = malloc(sizeof(struct bb_state));
    if (bb_data == NULL)
    {
        perror("main calloc");
        abort();
    }

    // Initialize myfs
    // Step 1: master connect to storage servers
    // hcpuyang comment: if you save the sockets in the local data structure `bb_data`, 
    // the client.h won't be able to use the sockets.
    bb_data->server_sockets_des = client_connect_to_servers();
    bb_data->rootdir = realpath(argv[argc - 1], NULL);
    bb_data->logfile = log_open();
    bb_data->metadata = metadata_init();

    // Step 2: turn over control to fuse
    fprintf(stderr, "about to call fuse_main\n");
    fuse_stat = fuse_main(argc, argv, &bb_oper, bb_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);

    return fuse_stat;
}

int main(int argc, char *argv[])
{
    if (argc == 2 || argv[1][0] == '-')
    {
        // if master server
        // ./bbfs mount_dir
        mount_fuse(argc, argv);
    }
    else
    {
        printf("error invalid command line input\n");
        abort();
    }
}