#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <netinet/in.h>
#include <resolv.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <iostream>
#include <vector>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#define SIZE 1000


using namespace google::protobuf::io;

using namespace std;



int main(int argv, char** argc){

        int host_port= 9016;
        char host_name[]="172.20.3.153";

        struct sockaddr_in my_addr;

        char buffer[1024];
        char server_message[1024]={0};
        int bytecount;
        int buffer_len=0;

        int hsock;
        int * p_int;
        int err;

        hsock = socket(AF_INET, SOCK_STREAM, 0);
        if(hsock == -1){
                printf("Error initializing socket %d\n",errno);
                goto FINISH;
        }

        p_int = (int*)malloc(sizeof(int));
        *p_int = 1;

        if( (setsockopt(hsock, SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int)) == -1 )||
                (setsockopt(hsock, SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int)) == -1 ) ){
                printf("Error setting options %d\n",errno);
                free(p_int);
                goto FINISH;
        }
        free(p_int);

        my_addr.sin_family = AF_INET ;
        my_addr.sin_port = htons(host_port);

        memset(&(my_addr.sin_zero), 0, 8);
        my_addr.sin_addr.s_addr = inet_addr(host_name);
        if( connect( hsock, (struct sockaddr*)&my_addr, sizeof(my_addr)) == -1 ){
                if((err = errno) != EINPROGRESS){
                        fprintf(stderr, "Error connecting socket %d\n", errno);
                        goto FINISH;
                }
        }
        
        while( (bytecount = recv(hsock , server_message , SIZE, 0)) > 0 )
        {
            if ((bytecount = send(*csock, server_message, SIZE, 0)) == -1) {
                cerr << "Error sending data " << endl;
            }
        }

FINISH:
        close(hsock);
}
