#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include "hash.h"
#include "server_log.h"

#define READ_OPERATION '1'
#define WRITE_OPERATION '2'

#define BUFFER_SIZE 1024
#define MAX_METADATA_SIZE 100
#define MAX_PATH_LEN 100

#define SERVER_PORT_BASE 5550

int mount_dir_len;
char* mount_dir;
struct Map* server_metadata;
size_t server_metadata_size;
int sockfd, newsockfd, port_num;
int n;
FILE* server_log;

struct Map
{
    char *meta_key;
    char *chunk_path;
    size_t chunk_size;
};

void error(const char *msg)
{
    perror(msg);
    exit(1);
}

int read_handler(char *filename, size_t chunk_size)
{
    server_log_msg(server_log, "Server %d: call read_handler\n", n);
    printf("Server %d: call read_handler\n", n);

    // Step 1: check metadata for file existance
    for (int i=0; i<server_metadata_size; i++) {
        if (strcmp(server_metadata[i].chunk_path, filename) == 0) {
            // file found, send back chunk
            if (chunk_size != server_metadata[i].chunk_size) {
                perror("error for chunk description");
                return -1;
            }
            
            // Step 2: read from local directory
            char* target_filepath = (char*)malloc(MAX_PATH_LEN);

            // memcpy(target_filepath, mount_dir, sizeof(mount_dir));
            // (target_filepath)[sizeof(mount_dir)] = '/';
            // memcpy(target_filepath + sizeof(mount_dir) + sizeof('/'), SHA256(filename), sizeof(SHA256(filename)));

            memcpy(target_filepath, mount_dir, mount_dir_len);
            (target_filepath)[mount_dir_len] = '/';
            
            char* sha_256 = SHA256(filename);
            int sha_len = strlen(sha_256);
            memcpy(target_filepath + mount_dir_len + 1, sha_256, sizeof(sha_len));

            // size_t target_filepath_size = sizeof(mount_dir) + sizeof('/') + sizeof(SHA256(filename));
            size_t target_filepath_size = sizeof(char) * (mount_dir_len + 1 + sha_len);

            (target_filepath)[target_filepath_size] = '\0';
            FILE *r_file = fopen(target_filepath, "rb");
            if (r_file == NULL) {
                perror("error opening reading file");
                free(target_filepath);
                return -2;
            }

            fseek(r_file, 0, SEEK_END);
            long rfile_size = ftell(r_file);
            fseek(r_file, 0, SEEK_SET);

            char* read_buffer = (char*)malloc(rfile_size + 1);
            if (read_buffer == NULL) {
                perror("error in memory allocation for read buffer");
                fclose(r_file);
                return -3;
            }
            
            size_t read_buffer_size = fread(read_buffer, sizeof(char), rfile_size, r_file);
            if (read_buffer_size < rfile_size) {
                perror("error in reading the whole file");
                fclose(r_file);
                free(read_buffer);
                free(target_filepath);
                return -4;
            }

            (read_buffer)[read_buffer_size] = '\0';


            // Step 3: send chunk size to bbfs
            unsigned int chunk_size_buffer = htonl(rfile_size);
            if (write(newsockfd, &chunk_size_buffer, sizeof(rfile_size)) < 0) {
                perror("error sending chunk size to bbfs");
                return -5;
            } 

            // Step 4: send chunk data to bbfs
            if (write(newsockfd, read_buffer, read_buffer_size) < 0) {
                perror("error sendning chunk data to bbfs");
                return -6;
            }

            // Step 5: cleanup
            fclose(r_file);
            free(read_buffer);
            free(target_filepath);
        }
    }
}

int write_handler(char *filename, size_t chunk_size)
{
    server_log_msg(server_log, "Server %d: call write_handler\n", n);
    printf("Server %d: call write_handler\n", n);

    // Step 1: check metadata for existing file checks
    server_log_msg(server_log, "Server %d: check metadata for write\n", n);
    printf("Server %d: check metadata for write\n", n);
    for (int i=0; i<server_metadata_size; i++) {
        if (strcmp(server_metadata[i].meta_key, filename) == 0) {
            perror("filename already exists");
            return -1; // error code: error already exists
        }
    }
    server_log_msg(server_log, "Server %d: saving file %s, with chunk size %ld\n", n, filename, chunk_size);
    printf("Server %d: saving file %s, with chunk size %ld\n", n, filename, chunk_size);

    // Step 2: recv chunks from bbfs
    unsigned int chunk_size_buffer;
    if (recv(newsockfd, &chunk_size_buffer, sizeof(chunk_size_buffer), 0) > 0) {
        unsigned int recv_chunk_size = ntohl(chunk_size_buffer);
        server_log_msg(server_log, "Server %d: recv chunk size %ld\n", n, recv_chunk_size);
        printf("Server %d: recv chunk size %u\n", n, recv_chunk_size);
        if (recv_chunk_size != (unsigned int) chunk_size) {
            perror("failed to receive chunk size from the bbfs");
            return -2;  // error code: error in receiving chunk size
        }
    }
    else {
        perror("failed to receive chunk size from the bbfs");
        return -2;  // error code: error in receiving chunk size
    }

    server_log_msg(server_log, "Server %d: receiving file %s data\n", n, filename);
    printf("Server %d: receiving file %s data\n", n, filename);

    char* data_buffer = (char*)malloc(chunk_size);
    if (recv(newsockfd, data_buffer, chunk_size, 0) < 0) {
        perror("failed to receive chunk content from the bbfs");
        return -3;  // error code: error in receiving chunk content
    }

    server_log_msg(server_log, "Server %d: finish receiving file %s data\n", n, filename);
    printf("Server %d: finish receiving file %s data\n", n, filename);

    // Step 3: update data and metadata
    char target_filepath[MAX_PATH_LEN];
    sprintf(target_filepath, "%s/%s", mount_dir, SHA256(filename));

    server_log_msg(server_log, "Server %d: preparing metadata and write to disk to local disk %s\n", n, target_filepath);
    printf("Server %d: preparing metadata and write to disk to local disk %s\n", n, target_filepath);

    FILE *w_file = fopen(target_filepath, "wb");
    if (w_file == NULL) {
        perror("error opening the target file");
        return -4; // error code: error opening the target file
    }

    size_t written_ret = fwrite(data_buffer, sizeof(char), chunk_size, w_file);
    if (written_ret < chunk_size) {
        perror("error writing to file");
        return -5; // error code: error writing to file
    }

    server_log_msg(server_log, "Server %d: finish metadata and write to disk\n", n);
    printf("Server %d: finish metadata and write to disk\n", n);

    server_metadata[server_metadata_size].meta_key = filename;
    server_metadata[server_metadata_size].chunk_size = chunk_size;
    server_metadata[server_metadata_size].chunk_path = SHA256(filename);
    server_metadata_size += 1;

    // Step 4: cleanup
    fclose(w_file);
    free(data_buffer);
    return 0;
}

int main(int argc, char *argv[])
{
    socklen_t clilen;
    char buffer[BUFFER_SIZE];
    struct sockaddr_in serv_addr, cli_addr;

    // ./server server_rank data_dir
    if (argc < 3)
    {
        fprintf(stderr, "ERROR, no rank or local directory provided\n");
        exit(1);
    }

    // init log
    n = atoi(argv[1]);
    assert(0 <= n && n <= 3);
    server_log = server_log_open(n);
    mount_dir_len = strlen(argv[2]);
    mount_dir = (char*)malloc(mount_dir_len);
    memcpy(mount_dir, argv[2], mount_dir_len);

    server_log_msg(server_log, "compiled\n");
    server_log_msg(server_log, "mount_dir is: %s\n", mount_dir);

    // Step 1: create a socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        error("ERROR opening socket");

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(SERVER_PORT_BASE + n);
    if(bind(sockfd, (struct sockaddr*) &server_addr, sizeof(server_addr)) < 0){
        server_log_msg(server_log, "bind error\n");
        abort();
    }
    server_log_msg(server_log, "Server %d: bind socket\n", n);
    printf("Server %d: bind socket\n", n);

    // start listening
    if(listen(sockfd, 1) < 0){
        server_log_msg(server_log, "listen error\n");
        abort();
    }
    server_log_msg(server_log, "Server %d: listening at %d\n", n, SERVER_PORT_BASE + n);
    printf("Server %d: listening at %d\n", n, SERVER_PORT_BASE + n);

    clilen = sizeof(cli_addr);
    
    newsockfd = accept(sockfd, (struct sockaddr*)&cli_addr, &clilen);
    if (newsockfd < 0)
        perror("error on accecpting the connection.");
    server_log_msg(server_log, "Server %d: accepted\n", n);

    // Step 2: initialize and maintain metadata
    // server_metadata = malloc(MAX_METADATA_SIZE * sizeof(*server_metadata));
    server_metadata = malloc(MAX_METADATA_SIZE * sizeof(struct Map));
    if (!server_metadata) {
        perror("failed to allocate memory for server metadata");
    }
    server_metadata_size = 0;

    // Step 3: wait for operation requests
    char *server_buffer = (char *)malloc(BUFFER_SIZE);
    while (1)
    {
        // printf("listening for requests");
        server_log_msg(server_log, "Listening for requests\n\n", n);
        int bbfs_request = recv(newsockfd, server_buffer, BUFFER_SIZE - 1, 0);
        // server_log_msg(server_log, "server_buffer is: %s\n", server_buffer);
        if (bbfs_request > 0)
        {
            server_log_msg(server_log, "handling a request from bbfs\n");
            size_t chunk_size;
            memcpy(&chunk_size, server_buffer + sizeof(char), sizeof(size_t));
            char *filename = server_buffer + sizeof(char) + sizeof(size_t);

            // TODO: send back
            server_log_msg(server_log, "before sending back a response to bbfs\n");
            char* resp_buffer = (char*)malloc(BUFFER_SIZE);
            if (n == 3) {
                resp_buffer[0] = 1;
            } else {
                resp_buffer[0] = 0;
            }
            resp_buffer[1] = '\0';
            size_t resp_size = 1;
            server_log_msg(server_log, "after sending back a response to bbfs\n");

            if (write(newsockfd, resp_buffer, resp_size) <0 ) {
                perror("error sending back operation response");
                return -1;
            }

            if (server_buffer[0] == '1')
            {
                // read operation
                server_log_msg(server_log, "Server %d: Recv READ operation\n", n);
                printf("Server %d: Recv READ operation\n", n);
                int ret = read_handler(filename, chunk_size);
                server_log_msg(server_log, "Server %d: error code: %d", ret);
                printf("Server %d: error code: %d", n, ret);
            }
            else if (server_buffer[0] == '2')
            {
                // write operation
                server_log_msg(server_log, "Server %d: Recv WRITE operation\n", n);
                printf("Server %d: Recv WRITE operation\n", n);
                int ret = write_handler(filename, chunk_size);
                server_log_msg(server_log, "Server %d: error code: %d", n, ret);
                printf("Server %d: error code: %d", n, ret);
            }
            else
            {
                server_log_msg(server_log, "ERROR, INVALID OPERATION\n", n);
            }
        }
    }

    // Step 3: cleanup
    close(newsockfd);
    close(sockfd);

    free(server_metadata);

    return 0;
}