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
#define MAX_METADATA_SIZE 1024 * 1024
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
    size_t offset;
};

void error(const char *msg)
{
    perror(msg);
    exit(1);
}


char* get_meta_key(const char* filename, size_t offset){
    // filename | _ | offset | \0
    // filename_offset?
    // char* key = (char*) malloc(sizeof(char) * (strlen(filename) + 1) + size());
    // sprintf(key, "%s_%ld", filename, offset);
    // return key;


    size_t length = snprintf(NULL, 0, "%s_%zu", filename, offset);
    char* key = (char*)malloc((length + 1) * sizeof(char));
    sprintf(key, "%s_%zu", filename, offset);
    return key;
}

int read_handler(char *filename, size_t chunk_size, size_t offset)
{
    server_log_msg(server_log, "Server %d: call read_handler\n", n);
    printf("Server %d: call read_handler\n", n);

    char * key = get_meta_key(filename, offset);

    // Step 1: check metadata for file existance
    for (int i=0; i<server_metadata_size; i++) {
        if (strcmp(server_metadata[i].meta_key, key) == 0) {
            // file found, send back chunk

            server_log_msg(server_log, "Server %d: file %s found\n", n, filename);
            printf("Server %d: file %s found\n", n, filename);

            if (chunk_size != server_metadata[i].chunk_size) {
                server_log_msg(server_log, "Server %d: ERROR, chunk size from server metadata and asked chunk size is different\n", n, filename);
                perror("error for chunk description");
                return -1;
            }
            
            // Step 2: read from local directory
            char target_filepath[MAX_PATH_LEN];
            sprintf(target_filepath, "%s/%s", mount_dir, SHA256(key));
            server_log_msg(server_log, "Server %d: will read from local file %s\n", n, target_filepath);
            printf("Server %d: will read from local file %s\n", n, target_filepath);

            FILE *r_file = fopen(target_filepath, "rb");
            if (r_file == NULL) {
                server_log_msg(server_log, "Server %d: cannot open local file %s\n", n, target_filepath);
                printf("Server %d: cannot open local file %s\n", n, target_filepath);
                perror("error opening reading file");
                return -2;
            }

            fseek(r_file, 0, SEEK_END);
            long rfile_size = ftell(r_file);
            fseek(r_file, 0, SEEK_SET);

            server_log_msg(server_log, "Server %d: expect local file size %ld\n", n, rfile_size);
            printf("Server %d: expect local file size %ld\n", n, rfile_size);

            char* read_buffer = (char*)malloc(rfile_size + 1);
            if (read_buffer == NULL) {
                perror("error in memory allocation for read buffer");
                fclose(r_file);
                return -3;
            }
            
            size_t read_buffer_size = fread(read_buffer, sizeof(char), rfile_size, r_file);
            server_log_msg(server_log, "Server %d: read in by size %zu\n", n, read_buffer_size);
            printf("Server %d: read in by size %zu\n", n, read_buffer_size);
            if (read_buffer_size < rfile_size) {
                server_log_msg(server_log, "Server %d: ERROR in reading the whole file\n", n);
                perror("error in reading the whole file");
                fclose(r_file);
                free(read_buffer);
                return -4;
            }

            (read_buffer)[read_buffer_size] = '\0';


            // Step 3: send chunk size to bbfs
            unsigned int chunk_size_buffer = htonl(rfile_size);
            if (write(newsockfd, &chunk_size_buffer, sizeof(chunk_size_buffer)) < 0) {
                server_log_msg(server_log, "Server %d: ERROR sending chunk size to bbfs\n", n);
                perror("error sending chunk size to bbfs");
                return -5;
            } 

            // Step 4: send chunk data to bbfs
            if (write(newsockfd, read_buffer, read_buffer_size) < 0) {
                server_log_msg(server_log, "Server %d: ERROR sending chunk data to bbfs\n", n);
                perror("error sendning chunk data to bbfs");
                return -6;
            }

            // Step 5: cleanup
            fclose(r_file);
            free(read_buffer);
            return 0;
        }
    }

    server_log_msg(server_log, "Server %d: file %s not found\n", n, filename);
    perror("File not found\n");
    return -7;
}

int write_handler(char *filename, size_t chunk_size, size_t offset)
{
    char* key = get_meta_key(filename, offset);

    server_log_msg(server_log, "\nServer %d: call write_handler(%s, %zu, %zu)\n", n, filename, chunk_size, offset);
    printf("\nServer %d: call write_handler(%s, %zu, %zu)\n", n, filename, chunk_size, offset);

    // Step 1: check metadata for existing file checks
    server_log_msg(server_log, "Server %d: check metadata for write\n", n);
    printf("Server %d: check metadata for write, server_metadata_size: %ld\n", n, server_metadata_size);

    // TODO: I think we allow over written (e.g. writting file with the same name), someone asked this on Piazza
    for (int i=0; i<server_metadata_size; i++) {
        // printf("Server %d: compare %s and %s\n", n, key, server_metadata[i].meta_key);
        if (strcmp(server_metadata[i].meta_key, key) == 0) {
            perror("filename already exists\n");
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
    sprintf(target_filepath, "%s/%s", mount_dir, SHA256(key));

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
    
    server_metadata[server_metadata_size].meta_key = key;
    server_metadata[server_metadata_size].chunk_size = chunk_size;
    server_metadata[server_metadata_size].chunk_path = SHA256(key);

    // Now str contains the integer as characters

    server_metadata[server_metadata_size].offset = offset;
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
            size_t chunk_size, offset;
            memcpy(&chunk_size, server_buffer + sizeof(char), sizeof(size_t));
            char *filename = server_buffer + sizeof(char) + sizeof(size_t);
            memcpy(&offset, server_buffer + sizeof(char) + sizeof(uint64_t) + sizeof(char) * (strlen(filename) + 1), sizeof(size_t));
            
            server_log_msg(server_log, "Server %d: get message from client %s %zu\n", n, filename, offset);


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
                int ret = read_handler(filename, chunk_size, offset);
                server_log_msg(server_log, "Server %d: error code: %d\n", n, ret);
                printf("Server %d: error code: %d\n", n, ret);
            }
            else if (server_buffer[0] == '2')
            {
                // write operation
                server_log_msg(server_log, "Server %d: Recv WRITE operation\n", n);
                printf("Server %d: Recv WRITE operation\n", n);
                int ret = write_handler(filename, chunk_size, offset);
                server_log_msg(server_log, "Server %d: error code: %d\n", n, ret);
                printf("Server %d: error code: %d\n", n, ret);
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