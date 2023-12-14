#pragma once

#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdio.h>
#include "log.h"
#include "metadata.h"
#include "params.h"
#include "storage.h"
#include <string.h>
#include <unistd.h>

#define READ_OPERATION '1'
#define WRITE_OPERATION '2'
#define BUFFER_SIZE 1024

int* ss_sockets;

// perform handshake and return int array of socket descriptor
int* client_connect_to_servers(){
    ss_sockets = (int *) malloc(sizeof(int) * 4);

    Server* all_storages = get_all_storages_info();

    for(int i=0 ; i<4; i++){
        struct sockaddr_in address;
        memset(&address, 0, sizeof(address));
        address.sin_family = AF_INET;

        printf("connecting to storage %d at %s:%d ...\n", i, all_storages[i].addr, all_storages[i].port);

        address.sin_addr.s_addr = inet_addr(all_storages[i].addr);
        address.sin_port = htons(all_storages[i].port);
        int sd = socket(AF_INET, SOCK_STREAM, 0);

        while (connect(sd, (struct sockaddr*) &address, sizeof(address)) < 0);

        printf("connected to %s:%d at socket des %d\n\n", all_storages[i].addr, all_storages[i].port, sd);

        ss_sockets[i] = sd;
    }

    // array of socket desciption
    return ss_sockets;
}

// Helper functions 

// Send requests to storage servers and wait for the confirmed response
uint64_t send_recv_operation(int* ss_socket, char operation, uint64_t chunk_size, const char* filename, uint64_t offset) {
    printf("CLIENT: call send_recv_operation( %d, %c, %lu, %s, %lu)\n", *ss_socket, operation, chunk_size, filename, offset);

    char* client_resp_buffer = (char*)malloc(BUFFER_SIZE);    

    // operation | chunk_size | filename\0 | offset
    size_t total_size = sizeof(char) + sizeof(uint64_t) + sizeof(char) * (strlen(filename) + 1) + sizeof(uint64_t);

    // create buffer
    printf("CLIENT: total_size is %ld\n", total_size);
    char* req_buffer = (char*)malloc(total_size);
    if (req_buffer == NULL) {
        perror("send read requests malloc failed");
        return -1;
    }

    // fill in buffer contents
    req_buffer[0] = operation;
    memcpy(req_buffer+sizeof(char), &chunk_size, sizeof(uint64_t));
    strcpy(req_buffer+sizeof(char)+sizeof(uint64_t), filename);
    memcpy(req_buffer+sizeof(char) + sizeof(uint64_t) + sizeof(char) * (strlen(filename) + 1), &offset, sizeof(uint64_t));

    // TODO: test server connection failed & return failed storage server

    // sending out the request
    printf("CLEINT: sending operation %c to socket %d\n", operation, *ss_socket);
    if (write(*ss_socket, req_buffer, total_size) < 0) {
        perror("error sending operation to socket");
        return -1;
    }
    free(req_buffer);
    printf("CLEINT: finished sending operation %c to socket %d and wait for reply\n", operation, *ss_socket);

    // wait for the storage server response
    
    memset(client_resp_buffer, 0, BUFFER_SIZE);
    fprintf(stderr, "CLIENT: finished initialization with client_resp_buffer\n");

    int ss_response = recv(*ss_socket, client_resp_buffer, BUFFER_SIZE-1, 0);
    if (ss_response < 0) {
        perror("error receiving operation confirmation from storage server");
        return -1;
    }
    
    // read the data status from storage server
    uint64_t ss_resp = client_resp_buffer[0];
    free(client_resp_buffer);

    return ss_resp;
}

uint64_t send_chunk(int* ss_socket, const char* data_buffer, size_t chunk_size) {
    printf("CLIENT: sending chunk size %ld to socket %d \n", chunk_size, *ss_socket);
    unsigned int chunk_size_buffer = htonl(chunk_size);
    printf("CLIENT: sending chunk size buffer %u to socket %d \n", chunk_size_buffer, *ss_socket);
    // Send data size
    if (write(*ss_socket, &chunk_size_buffer, sizeof(chunk_size_buffer)) < 0) { 
        perror("error writing to socket");
        return -1;
    }
    // Send data 
    printf("CLIENT: sending chunk data buffer %u to socket %d \n", chunk_size_buffer, *ss_socket);
    if (write(*ss_socket, data_buffer, chunk_size) < 0) { 
        perror("error writing to socket");
        return -1;
    }
    printf("CLIENT: finish sending chunk data buffer %u to socket %d \n", chunk_size_buffer, *ss_socket);

    return chunk_size;
}

char* recv_chunk(int* ss_socket, size_t* chunk_size)
{   
    printf("CLIENT: ask at socket %d\n", *ss_socket);
    *chunk_size = 0;

    unsigned int length = 0;
    // Receive number of bytes

    int bytes_read = read(*ss_socket, &length, sizeof(length)); 
    if (bytes_read <= 0) {
        perror("Error in receiving message from server\n");
        return NULL;
    }
    length = ntohl(length);
    printf("CLIENT: expect chunk size %d\n", length);

    char *buffer = (char *)malloc(sizeof(char) * (length+1));
    if (!buffer) {
        perror("Error in allocating memory to receive message from server\n");
        return NULL;
    }

    char *pbuf = buffer;
    unsigned int buflen = length;
    while (buflen > 0) {
        bytes_read = read(*ss_socket, pbuf, buflen); // Receive bytes
        if (bytes_read <= 0) {
            perror("Error in receiving message from server\n");
            printf("CLIENT: Error in receiving message from server\n");
            free(buffer);
            return NULL;
        }
        pbuf += bytes_read;
        buflen -= bytes_read;
        printf("CLIENT: received %u bytes from server, now have %u bytes\n", bytes_read, length - buflen);
    }

    *chunk_size = length;
    return buffer;
}


// Called by syscalls

// replace pread in local filesystem
// pread(fi->fh, buf, size, offset)
int client_read(const char *path, int fd, char* buf, size_t count, off_t file_offset){
    assert(file_offset == 0);
    // pread() reads up to `count` bytes from file descriptor `fd` at `offset` 
    // offset (from the start of the file) into the buffer starting at
    // `buf`.  The file offset is not changed.

    printf("CLIENT: call client_read( %s, %d, %p, %zu, %ld )\n", path, fd, buf, count, file_offset);

    // Step 1: query local metadata
    size_t filesize = query_filesize(path);

    printf("CLIENT: stored file size for %s is %zu\n", path, filesize);

    if (filesize == (size_t)(-1)) {
        printf("target file is not found in bbfs");
        log_error("read file out found");
    }
    size_t target_chunk_size = get_chunk_size(filesize);

    printf("CLIENT: stored chunk size is %zu\n", target_chunk_size);

    // Step 2: send requests to storage servers
    int data_chunk_count = 0;
    int parity_chunk_count = 0;

    int failed_server = -1;

    for (int i=0; i<ss_num; i++) {
        printf("CLIENT: start %d send_recv_operation\n", i);
        uint64_t feedback = send_recv_operation(&ss_sockets[i], READ_OPERATION, target_chunk_size, path, file_offset);
        if (feedback == 1) {
            data_chunk_count += 1;
        } else if (feedback == 0) {
            parity_chunk_count += 1;
        } else {
            log_error("failed to connect to storage server");
            printf("CLIENT: FAILED to connect to %d\n", i);
            failed_server = i;
        }
        printf("CLIENT: done %d send_recv_operation\n", i);
    }

    char* tmp_file = (char *)malloc(sizeof(char) * target_chunk_size * (ss_num-1));
    char* tmp_file_offset = tmp_file;

    if(data_chunk_count + parity_chunk_count == ss_num || failed_server == ss_num - 1){
        // no storage server down, or parity server down
        // Step 3: recv blocks from storage servers
        printf("CLIENT: EVERY STORAGE SERVER IS ON\n");
        char * ss_buf;
        for (int i=0; i<ss_num-1; i++) {
            
            printf("CLIENT: ask for chunk from storage server %d\n", i);
            size_t chunk_size;
            ss_buf = recv_chunk(&ss_sockets[i], &chunk_size);

            printf("CLIENT: in total recevied %zu from storage server %d\n", chunk_size, i);

            if (i != ss_num - 2)
                assert(chunk_size == target_chunk_size);
            else
                assert(chunk_size == get_last_chunk_size(filesize));

            memcpy(tmp_file_offset, ss_buf, chunk_size);
            tmp_file_offset += chunk_size;
            printf("CLIENT: tmp_file location: %p\n", tmp_file);
            printf("CLIENT: tmp_file_offset location: %p\n", tmp_file_offset);

            printf("CLIENT: atttempt to free ss_buf\n");
            free(ss_buf);
            printf("CLIENT: can free ss_buf\n");
        }
    }
    else
    {
        printf("CLIENT: storage server %d is down\n", failed_server);
        // one storage server is down

        // read parity

        printf("CLIENT: ask for chunk from parity server\n");
        size_t chunk_size;
        char * recover_buf = recv_chunk(&ss_sockets[ss_num - 1], &chunk_size);
        printf("CLIENT: recevied %zu bytes from parity server\n", chunk_size);
        assert(chunk_size == target_chunk_size);

        // calc recovery size
        size_t skip_by;
        if(failed_server == ss_num - 2){
            skip_by = get_last_chunk_size(filesize);
        }else{
            skip_by = chunk_size;
        }

        char* recover_at;

        printf("CLIENT: asking chunks from storage server\n");
        for (int i=0; i<ss_num-1; i++) {
            if(failed_server == i){
                // skip first
                printf("CLIENT: will skip storage server %d because it's down\n", failed_server);
                recover_at = tmp_file;
                tmp_file += skip_by;
            }else{
                size_t chunk_size;
                char * ss_buf = recv_chunk(&ss_sockets[i], &chunk_size);
                printf("CLIENT: received %zu bytes from storage server %d\n", chunk_size, i);

                if(i != ss_num - 2)
                    assert(chunk_size == target_chunk_size);
                else
                    assert(chunk_size == get_last_chunk_size(filesize));

                memcpy(tmp_file, ss_buf, chunk_size);
                tmp_file += chunk_size;

                // recover
                for(int j=0; j<skip_by; j+=1){
                    recover_buf[j] = recover_buf[j] ^ recover_buf[j];
                }
            }
        }

        memcpy(recover_at, recover_buf, skip_by);
        free(recover_buf);
    }

    fprintf(stderr, "CLIENT: attempt to memcpy\n");
    fprintf(stderr, "CLIENT: count %zu\n", count);
    memcpy(buf, tmp_file, count);
    fprintf(stderr, "CLIENT: attempt to free tmp_file\n");
    free(tmp_file);

    fprintf(stderr, "CLIENT: client read done by %zu\n", count);

    // should return exactly the number of bytes requested except on EOF or error
    return count;
}

// replace pwrite in local filesystem
// call by bb_write
// write from buffer to file at fd at offset
int client_write(const char* filename, int fd, const char* buf, size_t size, off_t offset) {

    printf("CLIENT: call client_write( %s, %d, %p, %zu, %ld )\n", filename, fd, buf, size, offset);

    if(find_file_idx(filename) == (size_t)(-1)){
        add_file(filename, 0);
    }
    change_file_size(filename, query_filesize(filename) + size);

    // Step 1: initialise the pointers
    // divide the files chunks by `ss_num-1`
    const char *write_headers[ss_num-1];

    uint64_t chunk_size = get_chunk_size(size);
    printf("CLIENT: normal chunk size: %ld\n", chunk_size);

    for (int i=0; i<ss_num-1; i++) {
        write_headers[i] = buf + chunk_size * i;
    }

    // Step 2: XOR the parity chunk
    char* parity_chunk = (char*)malloc(sizeof(char) * chunk_size);
    memset(parity_chunk, 0, sizeof(char) * chunk_size);
    for (int i=0; i<chunk_size; i++) {
        for (int ss=0; ss<ss_num-1; ss++) {
            if(ss * chunk_size + i < size)
                parity_chunk[i] = parity_chunk[i] ^ write_headers[ss][i];
        }
    }

    // Step 3: notify the storage servers
    int data_chunk_count = 0;
    int parity_chunk_count = 0;
    for (int i=0; i<ss_num; i++) {
        printf("CLIENT: start %d send_recv_operation\n", i);
        uint64_t feedback = send_recv_operation(&ss_sockets[i], WRITE_OPERATION, chunk_size, filename, offset);
        if (feedback == 1) {
            data_chunk_count += 1;
        } else if (feedback == 0) {
            parity_chunk_count += 1;
        } else {
            perror("failed to connect to storage server");
            log_error("failed to connect to storage server");
            return -1;
        }

        printf("CLIENT: done %d send_recv_operation\n", i);
    }
    assert((data_chunk_count + parity_chunk_count) == ss_num);


    // Step 4: send out the chunks
    for (int i=0; i<ss_num-1; i++) {
        printf("CLIENT: start %d sending chunk \n", i);
        size_t send_size = chunk_size;
        if (i == ss_num - 2){
            send_size = get_last_chunk_size(size);
        }

        if (send_chunk(&ss_sockets[i], write_headers[i], send_size) != send_size) {
            printf("failed to send data chunk to storage server %d", i);
            return -1;
        }
    }
    if (send_chunk(&ss_sockets[ss_num-1], parity_chunk, chunk_size) != chunk_size) {
        printf("failed to send parity chunk to storage server %d", ss_num-1);
        return -1;
    }

    printf("CLIENT: done write\n");

    return size;
}