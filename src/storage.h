#pragma once

#include <sys/socket.h>
#include <netinet/in.h>
#include "server_log.h"
#include "params.h"
#include <assert.h>

typedef struct server{
    char * addr;
    int port;
} Server;

Server* get_all_storages_info(){
    Server* storages = (Server *) malloc(sizeof(Server) * 4);
    storages[0].addr = "10.0.54.4";
    storages[0].port = 5550;

    storages[1].addr = "10.0.54.4";
    storages[1].port = 5551;

    storages[2].addr = "10.0.54.4";
    storages[2].port = 5552;

    storages[3].addr = "10.0.54.4";
    storages[3].port = 5553;

    return storages;
}

// init connection with the client(master) on server side
int storage_connection_init(int n){
    assert(0 <= n && n <= 4);

    printf("%d: start storage\n", n);
    Server* storages = get_all_storages_info();
    Server this_storage = storages[n];

    // init log
    FILE* log = server_log_open(n);


    // reserve socket
    int sd = socket(AF_INET, SOCK_STREAM, 0);
    if( sd == -1 ){
        server_log_msg(log, "socket create error\n");
        abort();
    }

    // bind to a port
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(this_storage.port);
    if(bind(sd, (struct sockaddr*) &server_addr, sizeof(server_addr)) < 0){
        server_log_msg(log, "bind error\n");
        abort();
    }

    printf("%d: bind socket\n", n);

    // start listening
    if(listen(sd, 1) < 0){
        server_log_msg(log, "listen error\n");
        abort();
    }

    printf("%d: listening at %d\n", n, this_storage.port);

    struct sockaddr_in client_addr;
    int addr_len = sizeof(client_addr);
    int client_sd;
    if ((client_sd = accept(sd, (struct sockaddr*) &client_addr, &addr_len)) < 0) {
        server_log_msg(log, "accept error\n");
        abort();
    }

    printf("%d: accept client at socket des %d\n", n, client_sd);

    return client_sd;
}

// read data from file and send back to client (indicates by socket descriptor sd)
void storage_read(int sd, char* filename){
    // TODO(Pond)
}

// receive data from client (indicates by socket descriptor sd) and write back to filename
void storage_write(int sd, char* filename){
    // TODO(Pond)
}

int start_storage(int n, char * path_to_data_dir){

    assert(0 <= n && n <= 4);
    storage_connection_init(n);
    // check path_to_data_dir exist

    // check for incoming request from client
    while(1){
        // check for package from client

        // if package indicates write request, call server_write()

        // if package indicates read request, call server_read()
    }
}

