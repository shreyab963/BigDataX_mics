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
#include <time.h>
#include <semaphore.h>

sem_t semEmpty;
sem_t semFull;

pthread_mutex_t mutexBuffer;

#define SIZE 1024
#define NUM_WORKERS 4

#define MAX_NUM_ARGUMENTS 10
#define WHITESPACE " \t\n" //command line tokenization
#define MAX_MESSAGE_SIZE 250

char message[SIZE];

using namespace std;
using namespace google::protobuf::io;

void* SocketHandler(void*);
void* sendMessage(void*);
void* receiveMessage(void*);

int main(int argv, char** argc){

        int host_port= 9016;

        struct sockaddr_in my_addr;
        vector<string> ip_addr;

        int hsock;
        int * p_int ;
        int err;

        socklen_t addr_size = 0;
        int* csock;
        sockaddr_in sadr;


        pthread_t thread_id=0;

        pthread_t send =0;
        pthread_t receive=0;
        //pthread_mutex_init(&mutexBuffer, NULL);

        hsock = socket(AF_INET, SOCK_STREAM, 0);
        if(hsock == -1){
                printf("Error initializing socket %d\n", errno);
                goto FINISH;
        }

        p_int = (int*)malloc(sizeof(int));
        *p_int = 1;

        if( (setsockopt(hsock, SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int)) == -1 )||
                (setsockopt(hsock, SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int)) == -1 ) ){
                printf("Error setting options %d\n", errno);
                free(p_int);
                goto FINISH;
        }
        free(p_int);

        my_addr.sin_family = AF_INET ;
        my_addr.sin_port = htons(host_port);

        memset(&(my_addr.sin_zero), 0, 8);
        my_addr.sin_addr.s_addr = INADDR_ANY ;

        if( bind( hsock, (sockaddr*)&my_addr, sizeof(my_addr)) == -1 ){
                fprintf(stderr,"Error binding to socket %d\n",errno);
                goto FINISH;
        }
        if(listen( hsock, 10) == -1 ){
                fprintf(stderr, "Error listening %d\n",errno);
                goto FINISH;
        }


        addr_size = sizeof(sockaddr_in);
        cout<<"hii" <<endl;

        while(true){
                printf("waiting for a connection\n");
                csock = (int*)malloc(sizeof(int));
                if((*csock = accept( hsock, (sockaddr*)&sadr, &addr_size))!= -1){
                        printf("---------------------\nReceived connection from %s\n",inet_ntoa(sadr.sin_addr));
                        ip_addr.push_back(inet_ntoa(sadr.sin_addr));

                       // pthread_create(&send,0,&sendMessage, (void*)csock );
                       // pthread_create(&receive,0,&receiveMessage, (void*)csock );
                        pthread_create(&thread_id,0,&SocketHandler, (void*)csock );
                        pthread_detach(thread_id);
                       // pthread_detach(send);
                     //  pthread_detach(receive);
                }
                else{
                        fprintf(stderr, "Error accepting %d\n", errno);
                }
        }

FINISH:
    pthread_mutex_destroy(&mutexBuffer);

;
}

void* SocketHandler(void* lp){
    int *csock = (int*)lp;

        pthread_t send;
        pthread_t receive;
        pthread_mutex_init(&mutexBuffer, NULL);


        pthread_create(&send,0,&sendMessage, (void*)csock );

        pthread_create(&receive,0,&receiveMessage, (void*)csock );

        pthread_join(send, NULL);
        pthread_join(receive, NULL);

        pthread_mutex_destroy(&mutexBuffer);
        free(csock);
    return 0;
}

void* sendMessage(void* lp){
    int *csock = (int*)lp;
    int bytecount=0;

    cout << "\n\n"<<endl;
    cout << "Send instructions to client" << endl;


    while(true) {
        if(strlen(message) == '\0') {
            pthread_mutex_lock(&mutexBuffer);
            cin.getline(message, SIZE);

            if ((bytecount = send(*csock, message, SIZE, 0)) == -1) {
                cerr << "Error sending data " << endl;
            }
            pthread_mutex_unlock(&mutexBuffer);
        }

    }

}
void* receiveMessage(void* lp){
    char client_message[SIZE] = {0};
    int *csock = (int*)lp;
    int bytecount=0;
    char buffer[4];

    while(true){
        if(strlen(message) != '\0') {
            pthread_mutex_lock(&mutexBuffer);

            if ((bytecount = recv(*csock, client_message, SIZE, 0)) == -1) {
                cerr << "Error receiving data " << endl;
            }
            cout << "it's the client message: " << client_message << endl;
            memset(message, 0, sizeof(message));
            pthread_mutex_unlock(&mutexBuffer);
        }
    }
}
