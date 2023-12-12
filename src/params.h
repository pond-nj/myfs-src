/*
  Copyright (C) 2012 Joseph J. Pfeiffer, Jr., Ph.D. <pfeiffer@cs.nmsu.edu>

  This program can be distributed under the terms of the GNU GPLv3.
  See the file COPYING.

  There are a couple of symbols that need to be #defined before
  #including all the headers.
*/

#pragma once

// The FUSE API has been changed a number of times.  So, our code
// needs to define the version of the API that we assume.  As of this
// writing, the most current API version is 26
#define FUSE_USE_VERSION 26

// need this to get pwrite().  I have to use setvbuf() instead of
// setlinebuf() later in consequence.
#define _XOPEN_SOURCE 500

// maintain bbfs state in here
#include <limits.h>
#include <stdio.h>
#include <fuse.h>
#include <stdlib.h>
#define ss_num 4

typedef struct FileMetadata{
  char* name;
  size_t chunk_size;
  size_t file_size;
} FileMetadata;

// only support flat namespace
typedef struct metadata{
  // index by file descriptor
  FileMetadata* files;
  int count_files;
} Metadata;

struct bb_state {
  FILE *logfile;
  char *rootdir;
  int* server_sockets_des;
  Metadata metadata;
};


#define BB_DATA ((struct bb_state *) fuse_get_context()->private_data)

