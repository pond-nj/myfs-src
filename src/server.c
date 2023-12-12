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

char* mount_dir;
struct Map* server_metadata;
size_t server_metadata_size;
int sockfd, newsockfd, port_num;

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
            memcpy(target_filepath, mount_dir, sizeof(mount_dir));
            (target_filepath)[sizeof(mount_dir)] = '/';
            memcpy(target_filepath + sizeof(mount_dir) + sizeof('/'), SHA256(filename), sizeof(SHA256(filename)));
            size_t target_filepath_size = sizeof(mount_dir) + sizeof('/') + sizeof(SHA256(filename));
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
            if (write(sockfd, &chunk_size_buffer, sizeof(rfile_size)) < 0) {
                perror("error sending chunk size to bbfs");
                return -5;
            } 

            // Step 4: send chunk data to bbfs
            if (write(sockfd, read_buffer, read_buffer_size) < 0) {
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
    // Step 1: check metadata for existing file checks
    for (int i=0; i<server_metadata_size; i++) {
        if (strcmp(server_metadata[i].meta_key, filename) == 0) {
            perror("filename already exists");
            return -1; // error code: error already exists
        }
    }
    // Step 2: recv chunks from bbfs
    char* chunk_size_buffer = (char*)malloc(BUFFER_SIZE);
    if (recv(sockfd, chunk_size_buffer, sizeof(chunk_size), 0) > 0) {
        uint64_t recv_chunk_size = ntohl(*chunk_size_buffer);
        if (recv_chunk_size != chunk_size) {
            perror("failed to receive chunk size from the bbfs");
            return -2;  // error code: error in receiving chunk size
        }
    }
    else {
        perror("failed to receive chunk size from the bbfs");
        return -2;  // error code: error in receiving chunk size
    }
    
    free(chunk_size_buffer);

    char* data_buffer = (char*)malloc(chunk_size);
    if (recv(sockfd, data_buffer, chunk_size, 0) < 0) {
        perror("failed to receive chunk content from the bbfs");
        return -3;  // error code: error in receiving chunk content
    }

    // Step 3: update data and metadata
    char* target_filepath = (char*)malloc(MAX_PATH_LEN);
    memcpy(target_filepath, mount_dir, sizeof(mount_dir));
    (target_filepath)[sizeof(mount_dir)] = '/';
    memcpy(target_filepath + sizeof(mount_dir) + sizeof('/'), SHA256(filename), sizeof(SHA256(filename)));
    (target_filepath)[sizeof(mount_dir) + sizeof('/') + sizeof(SHA256(filename))] = '\0';

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

    server_metadata[server_metadata_size].meta_key = filename;
    server_metadata[server_metadata_size].chunk_size = chunk_size;
    server_metadata[server_metadata_size].chunk_path = SHA256(filename);
    server_metadata_size += 1;

    // Step 4: cleanup
    fclose(w_file);
    free(data_buffer);
    free(target_filepath);
    return 0;
}

int main(int argc, char *argv[])
{
    socklen_t clilen;
    char buffer[BUFFER_SIZE];
    struct sockaddr_in serv_addr, cli_addr;
    int n;

    // ./server server_rank data_dir
    if (argc < 3)
    {
        fprintf(stderr, "ERROR, no rank or local directory provided\n");
        exit(1);
    }

    // init log
    n = atoi(argv[1]);
    assert(0 <= n && n <= 3);
    FILE* server_log = server_log_open(n);
    mount_dir = (char*)malloc(strlen(argv[2]));
    memcpy(mount_dir, argv[2], strlen(argv[2]));
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
    printf("Server %d: bind socket\n", n);

    // start listening
    if(listen(sockfd, 1) < 0){
        server_log_msg(server_log, "listen error\n");
        abort();
    }
    printf("Server %d: listening at %d\n", n, SERVER_PORT_BASE + n);


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
        int bbfs_request = recv(sockfd, server_buffer, BUFFER_SIZE - 1, 0);
        // server_log_msg(server_log, "server_buffer is: %s\n", server_buffer);
        if (bbfs_request > 0)
        {
            size_t chunk_size;
            memcpy(&chunk_size, server_buffer + sizeof(char), sizeof(size_t));
            char *filename = server_buffer + sizeof(char) + sizeof(size_t);

            if (server_buffer[0] == '\1')
            {
                // read operation
                int ret = read_handler(filename, chunk_size);
            }
            else if (server_buffer[0] == '\2')
            {
                // write operation
                int ret = write_handler(filename, chunk_size);
            }
        }
    }

    // Step 3: cleanup
    close(newsockfd);
    close(sockfd);

    free(server_metadata);

    return 0;
}