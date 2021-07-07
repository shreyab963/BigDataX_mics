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
#include <signal.h>
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

#define MAX_WORKERS 4

char message[SIZE];

using namespace std;
using namespace google::protobuf::io;

static atomic<int> num_workers{0};
volatile sig_atomic_t flag = 0;
static int uid = 10;

void* SocketHandler(void*);
void* sendMessage(void*);
void* receiveMessage(void*);
void catch_ctrl_c_and_exit(int);

void str_overwrite_stdout() {
    printf("%s", "> ");
    fflush(stdout);
}


/* Worker structure */
typedef struct{
    struct sockaddr_in address;
    int sockfd;
    int uid;
    char name[32];
} worker_t;

worker_t *workers[MAX_WORKERS];

pthread_mutex_t workers_mutex = PTHREAD_MUTEX_INITIALIZER;

void str_trim_lf (char* arr, int length) {
    int i;
    for (i = 0; i < length; i++) { // trim \n
        if (arr[i] == '\n') {
            arr[i] = '\0';
            break;
        }
    }
}

/* Add workers to queue */
void queue_add(worker_t *cl){
    pthread_mutex_lock(&workers_mutex);

    for(int i=0; i < MAX_WORKERS; ++i){
        if(!workers[i]){
            workers[i] = cl;
            break;
        }
    }

    pthread_mutex_unlock(&workers_mutex);
}

/* Remove workers to queue */
void queue_remove(int uid){
    pthread_mutex_lock(&workers_mutex);

    for(int i=0; i < MAX_WORKERS; ++i){
        if(workers[i]){
            if(workers[i]->uid == uid){
                workers[i] = NULL;
                break;
            }
        }
    }

    pthread_mutex_unlock(&workers_mutex);
}


int main(int argv, char** argc){

    int host_port= 9015;
    struct sockaddr_in my_addr;

    int hsock;
    int * p_int ;
    int err;

    socklen_t addr_size = 0;
    int* csock;
    sockaddr_in sadr;

    pthread_t thread_id=0;

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
    while(true){
        printf("waiting for a connection\n");
        csock = (int*)malloc(sizeof(int));
        if((*csock = accept( hsock, (sockaddr*)&sadr, &addr_size))!= -1){
            printf("---------------------\nReceived connection from %s\n",inet_ntoa(sadr.sin_addr));
            if((num_workers + 1) == MAX_WORKERS){
                cout << "Connection from" << inet_ntoa(sadr.sin_addr) << "declined: Max workers exceeded" << endl;
                close(*csock);
                continue;
            }
            /* Worker settings */
            worker_t *worker = (worker_t *)malloc(sizeof(worker_t));
            worker->address = sadr;
            worker->sockfd = *csock;
            worker->uid = uid++;

            /* Add worker to the queue and fork thread */
            queue_add(worker);
            pthread_create(&thread_id,0,&SocketHandler, (void*)worker );

            /* Reduce CPU usage */
            sleep(1);
        }
        else{
            fprintf(stderr, "Error accepting %d\n", errno);
        }
    }
    FINISH:
    ;
}

void* SocketHandler(void* lp){
    char worker_name[50];
    int leave_flag = 0;
    int bytecount=0;
    char worker_message[SIZE] = {0};

    num_workers++;
    worker_t *worker = (worker_t *)lp;
    pthread_t send;
    pthread_t receive;

        pthread_create(&send,0, &sendMessage, (void*)worker);
        pthread_create(&receive,0, &receiveMessage, (void*)worker);

    while (1){
        if(flag){
            printf("\nBye\n");
            break;
        }
    }

        pthread_join(send, NULL);
        pthread_join(receive, NULL);

    close(worker->sockfd);
    queue_remove(worker->uid);
    free(worker);
    num_workers--;
    pthread_detach(pthread_self());

    return NULL;
}

/* Send message to all workers */
void* receiveMessage(void* lp){
    char worker_message[SIZE] = {0};
    int bytecount=0;
    worker_t *worker = (worker_t *)lp;
    char buffer[4];

    while(true){
        if(strlen(message) != '\0') {
            pthread_mutex_lock(&mutexBuffer);
            if ((bytecount = recv(worker->sockfd, worker_message, SIZE, 0)) == -1) {
                cerr << "Error receiving data " << endl;

            }
            else if(bytecount== 0){
                cout << worker->name << "disconnected" << endl;
            }
            else{
                cout << worker_message << endl;
            }
            memset(message, 0, sizeof(message));
            pthread_mutex_unlock(&mutexBuffer);
        }
    }
}

void* sendMessage(void* lp){
    //str_overwrite_stdout();
    while(true) {
        if (strlen(message) == '\0') {
            pthread_mutex_lock(&workers_mutex);
            cin.getline(message, SIZE);
            for (int i = 0; i < MAX_WORKERS; ++i) {
                if (workers[i]) {
                    if (send(workers[i]->sockfd, message, strlen(message), 0) < 0) {
                        cout << "Error sending message" << endl;
                        break;
                    }
                }
            }
            if(strcmp(message, "exit")==0) {
                cout << "end"<<endl;
                return 0;
            }
            pthread_mutex_unlock(&workers_mutex);
        }
    }
}

void catch_ctrl_c_and_exit(int sig) {
    flag = 1;
}
