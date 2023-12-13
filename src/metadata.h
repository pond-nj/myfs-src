#pragma once
#include <time.h>
#include <assert.h>
#include "params.h"
#include "log.h"
#include <fcntl.h>
#include <stdlib.h>
#define MAX_FILES 20
#define MAX_PATHNAME 100

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

Metadata bbfs_metadata;

Metadata metadata_init(){
  FileMetadata* fm = (FileMetadata *) malloc(sizeof(FileMetadata) * MAX_FILES);

  bbfs_metadata.files = fm;

  bbfs_metadata.files[0].name = ".";
  bbfs_metadata.files[1].name = "..";
  bbfs_metadata.count_files = 2;
}

// in case size = 10, ss_num-1 = 3, we need chunksize = 4
// chunksize = ceil(size/ss_num-1)
size_t get_chunk_size(size_t file_size){
  // return (file_size + ss_num-2) / (ss_num-1);
  // TODO
  return file_size / (ss_num-1);
}

// in case size = 10, ss_num-1 = 3, we need chunksize = 4
// last_chunk_size = 2
size_t get_last_chunk_size(size_t file_size){
  // size_t chunk_size = get_chunk_size(file_size);
  // size_t last_chunk_size = file_size % chunk_size;
  // if (last_chunk_size == 0)
  //   last_chunk_size = chunk_size;
  // return last_chunk_size;

  // TODO
  return file_size / (ss_num-1);
}

int add_file(const char *filename, size_t file_size) {
  log_msg("METADATA: adding file %s of size %zu\n", filename, file_size);
  int curr_num_file = bbfs_metadata.count_files;
  log_msg("METADATA: now count_files = %d\n", curr_num_file);
  assert(curr_num_file + 1 <= MAX_FILES);
  bbfs_metadata.files[curr_num_file].name = (char *)malloc(sizeof(char) * MAX_PATHNAME);
  strcpy(bbfs_metadata.files[curr_num_file].name, filename);

  bbfs_metadata.files[curr_num_file].file_size = file_size;
  bbfs_metadata.files[curr_num_file].chunk_size = get_chunk_size(file_size);

  bbfs_metadata.count_files++;
  return 0;
}

int find_file_idx(const char* filename){
  for (int i=0; i<bbfs_metadata.count_files; i++) {
    if (strcmp(bbfs_metadata.files[i].name, filename) == 0) {
      return i;
    }
  }
  return -1;
}

size_t query_filesize(const char *filename) {
  int file_index = find_file_idx(filename);

  if(file_index != -1)
    return bbfs_metadata.files[file_index].file_size;
  else
    return (size_t)(-1);
}

void change_file_size(const char *filename, size_t file_size){
  log_msg("METADATA: change file size of file %s to size %zu\n", filename, file_size);
  int file_index = find_file_idx(filename);
  assert(file_index != -1);

  bbfs_metadata.files[file_index].chunk_size = get_chunk_size(file_size);
  bbfs_metadata.files[file_index].file_size = file_size;
}

// abstraction of metadata in myfs
// acts like open
// return file descriptor
// int metadata_open(const char *path, int flag){
//   // TODO: change to depend on flag

  
//   // path: /test.txt => file: test.txt
//   assert(path[0] == '/');
//   const char* filename = &path[1];

//   // return existing file
//   for(int i=0; i<bbfs_metadata.count_files; i++){
//     if( strcmp(bbfs_metadata.files[i].name, filename) == 0){
//       return i;
//     }
//   }

//   log_msg("add_file( %s)\n", filename);
//   add_file(filename);
//   return 0;
// }


