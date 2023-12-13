#pragma once
#include <time.h>
#include <assert.h>
#include "params.h"
#include "log.h"
#include <fcntl.h>
#include <stdlib.h>
#define MAX_FILES 20
#define MAX_PATHNAME 100

Metadata metadata_init(){
  FileMetadata* fm = (FileMetadata *) malloc(sizeof(FileMetadata) * MAX_FILES);
  Metadata m;
  m.files = fm;

  m.files[0].name = ".";
  m.files[1].name = "..";
  m.count_files = 2;
  return m;
}

// in case size = 10, ss_num-1 = 3, we need chunksize = 4
// chunksize = ceil(size/ss_num-1)
size_t get_chunk_size(size_t file_size){
  return (file_size + ss_num-2) / (ss_num-1);
}

// in case size = 10, ss_num-1 = 3, we need chunksize = 4
// last_chunk_size = 2
size_t get_last_chunk_size(size_t file_size){
  size_t chunk_size = get_chunk_size(file_size);
  size_t last_chunk_size = file_size % chunk_size;
  if(last_chunk_size == 0)
      last_chunk_size = chunk_size;
  return last_chunk_size;
}

int add_file(const char *filename, size_t file_size) {
  int curr_num_file = BB_DATA->metadata.count_files;
  assert(curr_num_file + 1 <= MAX_FILES);
  BB_DATA->metadata.files[curr_num_file].name = (char *)malloc(sizeof(char) * MAX_PATHNAME);
  strcpy(BB_DATA->metadata.files[curr_num_file].name, filename);

  BB_DATA->metadata.files[curr_num_file].file_size = file_size;
  BB_DATA->metadata.files[curr_num_file].chunk_size = get_chunk_size(file_size);

  BB_DATA->metadata.count_files++;
  return 0;
}

size_t query_filesize(const char *filename) {
  for (int i=0; i<BB_DATA->metadata.count_files; i++) {
    if (strcmp(BB_DATA->metadata.files[i].name, filename) == 0) {
      return BB_DATA->metadata.files[i].file_size;
    }
  }
  return (size_t)(-1);
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
//   for(int i=0; i<BB_DATA->metadata.count_files; i++){
//     if( strcmp(BB_DATA->metadata.files[i].name, filename) == 0){
//       return i;
//     }
//   }

//   log_msg("add_file( %s)\n", filename);
//   add_file(filename);
//   return 0;
// }


