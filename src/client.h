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

int ss_num = 0;
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
        ss_num += 1;
    }

    // array of socket desciption
    return ss_sockets;
}

// Helper functions

// Send requests to storage servers and wait for the confirmed response
uint64_t send_recv_operation(int* ss_socket, char operation, uint64_t chunk_size, const char* filename) {

    
    size_t total_size = sizeof(char) + sizeof(size_t) + strlen(filename) + 1;

    // create buffer
    char* req_buffer = (char*)malloc(total_size);
    if (req_buffer == NULL) {
        perror("send read requests malloc failed");
        return -1;
    }

    // fill in buffer contents
    req_buffer[0] = operation;
    memcpy(req_buffer+sizeof(char), &chunk_size, sizeof(size_t));
    strcpy(req_buffer+sizeof(char)+sizeof(size_t), filename);

    // sending out the request
    if (write(*ss_socket, req_buffer, total_size) < 0) {
        perror("error sending operation to socket");
        return -1;
    }

    // wait for the storage server response
    char* resp_buffer = (char*)malloc(BUFFER_SIZE);

    int ss_response = recv(*ss_socket, resp_buffer, BUFFER_SIZE-1, 0);
    if (ss_response < 0) {
        perror("error receiving operation confirmation from storage server");
        return -1;
    }
    
    // read the data status from storage server
    uint64_t ss_resp = resp_buffer[0];
    free(req_buffer);
    free(resp_buffer);

    return ss_resp;
}

uint64_t send_chunk(int* ss_socket, const char* data_buffer, uint64_t chunk_size) {
    unsigned int chunk_size_buffer = htonl(chunk_size);
    // Send data size
    if (write(*ss_socket, &chunk_size_buffer, sizeof(chunk_size)) < 0) { 
        perror("error writing to socket");
        return -1;
    }
    // Send data 
    if (write(*ss_socket, data_buffer, chunk_size) < 0) { 
        perror("error writing to socket");
        return -1;
    }

    return 0;
}

char* recv_chunk(int* ss_socket, size_t* chunk_size)
{
    *chunk_size = 0;

    unsigned int length = 0;
    // Receive number of bytes
    int bytes_read = read(*ss_socket, &length, sizeof(length)); 
    if (bytes_read <= 0) {
        perror("Error in receiving message from server\n");
        return NULL;
    }
    length = ntohl(length);

    char *buffer = (char *)malloc(length+1);
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
            free(buffer);
            return NULL;
        }
        pbuf += bytes_read;
        buflen -= bytes_read;
    }

    *chunk_size = length;
    return buffer;
}


// Called by syscalls

// replace pread in local filesystem
// pread(fi->fh, buf, size, offset)
int client_read(const char *path, int fd, char* buf, size_t count, off_t offset){
    // pread() reads up to `count` bytes from file descriptor `fd` at `offset` 
    // offset (from the start of the file) into the buffer starting at
    // `buf`.  The file offset is not changed.


    // Step 1: query local metadata
    size_t target_chunk_size = query_file(path);
    if (target_chunk_size == (size_t)(-1)) {
        printf("target file is not found in bbfs");
        log_error("read file out found");
    }

    // Step 2: send requests to storage servers
    int data_chunk_count = 0;
    int parity_chunk_count = 0;

    for (int i=0; i<ss_num; i++) {
        uint64_t feedback = send_recv_operation(&ss_sockets[i], READ_OPERATION, target_chunk_size, path);
        if (feedback == 1) {
            data_chunk_count += 1;
        } else if (feedback == 0) {
            parity_chunk_count += 1;
        } else {
            perror("failed to connect to storage server");
            log_error("failed to connect to storage server");
            return -1;
        }
    }

    assert((data_chunk_count + parity_chunk_count) == ss_num);

    // TODO: read (n-1) servers (scenario: the down one is data storage/parity storage)


    // Step 3: recv blocks from storage servers
    for (int i=0; i<ss_num; i++) {
        size_t chunk_size;
        char * ss_buf = recv_chunk(&ss_sockets[i], &chunk_size);
        memcpy(buf, ss_buf, offset + chunk_size);
        offset += chunk_size;
    }

    // should return exactly the number of bytes requested except on EOF or error
    return 0;
}

// replace pwrite in local filesystem
// call by bb_write
int client_write(const char* filename, int fd, const char* buf, size_t size, off_t offset) {
    // divide the files chunks by `ss_num-1`

    // Step 1: initialise the pointers
    const char *write_headers[ss_num];
    uint64_t chunk_size = size / (ss_num-1);
    for (int i=0; i<ss_num; i++) {
        write_headers[i] = buf + chunk_size * i;
    }

    // Step 2: XOR the parity chunk
    char* parity_chunk = (char*)malloc(sizeof(char) * chunk_size);
    for (int i=0; i<chunk_size; i++) {
        for (int ss=0; ss<ss_num; ss++) {
            parity_chunk[i] = parity_chunk[i] ^ write_headers[ss][i];
        }
    }

    // Step 3: notify the storage servers
    int data_chunk_count = 0;
    int parity_chunk_count = 0;
    for (int i=0; i<ss_num; i++) {
        uint64_t feedback = send_recv_operation(&ss_sockets[i], WRITE_OPERATION, chunk_size, filename);
        if (feedback == 1) {
            data_chunk_count += 1;
        } else if (feedback == 0) {
            parity_chunk_count += 1;
        } else {
            perror("failed to connect to storage server");
            log_error("failed to connect to storage server");
            return -1;
        }
    }
    assert((data_chunk_count + parity_chunk_count) == ss_num);


    // Step 4: send out the chunks
    for (int i=0; i<ss_num; i++) {
        if (send_chunk(&ss_sockets[i], write_headers[i], chunk_size) != chunk_size) {
            printf("failed to send chunk to storage server");
            return -1;
        }
    }

    // Step 5: update local metadata
    if (add_file(filename, chunk_size) != 0) {
        printf("failed to modify metadata");
    }

    return 0;
}

// called when "touch tmp.txt"
int client_utime(const char *path, struct utimbuf *ubuf){
    memset(ubuf, 0, sizeof(struct utimbuf));
    return 0;
}