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
#include <pthread.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>


#define SIZE 1024

pthread_mutex_t mutexBuffer;

using namespace google::protobuf::io;

using namespace std;

volatile sig_atomic_t flag = 0;
char message[SIZE];

void* sendMessage(void*);
void* receiveMessage(void*);

int main(int argv, char** argc){

    int host_port= 9015;
    char host_name[]="172.20.3.153";

    struct sockaddr_in my_addr;

    char buffer[1024];
    char server_message[1024]={0};
    int bytecount;
    int buffer_len=0;

    pthread_mutex_init(&mutexBuffer, NULL);

    int hsock;
    int * p_int;
    int err;

    pthread_t send ;
    pthread_t receive;
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

    pthread_t recv_msg;
    if(pthread_create(&recv_msg, NULL, receiveMessage, (void*)&hsock) != 0){
        cout << "Error creating thread" << endl;
        return EXIT_FAILURE;
    }
    pthread_t send_msg;
    if(pthread_create(&send_msg, NULL,  sendMessage, (void*)&hsock) != 0){
        cout << "Error creating thread" << endl;
        return EXIT_FAILURE;
    }

    while (1){
        if(flag){
            printf("\nWorker Disconnected\n");
            return 0;
        }
    }
    FINISH:
    close(hsock);
    return EXIT_SUCCESS;


}

void* sendMessage(void* lp){
    int *csock = (int*)lp;
    int bytecount=0;

    cout << "\n\n" << endl;
    while(true) {
        if(strlen(message) != '\0') {
            pthread_mutex_lock(&mutexBuffer);

            if ((bytecount = send(*csock, message, SIZE, 0)) == -1) {
                cerr << "Error sending data " << endl;
            }
            memset(message, 0, sizeof(message));
            pthread_mutex_unlock(&mutexBuffer);
        }
    }

}

void* receiveMessage(void* lp){
    int *csock = (int*)lp;
    int bytecount=0;
    char buffer[4];

    while(true){
        if(strlen(message) == '\0') {
            pthread_mutex_lock(&mutexBuffer);

            if ((bytecount = recv(*csock, message, SIZE, 0)) == -1) {
                cerr << "Error receiving data " << endl;
            }
            else if(bytecount==0 || strcmp(message, "exit") == 0){
                flag =1;
            }
            else{
                cout << message << endl;
            }
            pthread_mutex_unlock(&mutexBuffer);

        }

    }
}
