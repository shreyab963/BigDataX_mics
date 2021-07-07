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
#include "message.pb.h"
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

google::protobuf::uint32 readHdr(char *buf)
{
    google::protobuf::uint32 size;
    google::protobuf::io::ArrayInputStream ais(buf,4);
    CodedInputStream coded_input(&ais);
    coded_input.ReadVarint32(&size);
    return size;
}

void readBody(void* lp,google::protobuf::uint32 dataSize)
{
    WorkersLog workerlog;
    worker_t *worker = (worker_t *)lp;

    int bytecount;

    //--Worker data[size of the data + header]
    char buffer [dataSize+4];

    //Read the entire buffer including the hdr
    if((bytecount = recv(worker->sockfd, (void *)buffer, 4+dataSize, MSG_WAITALL))== -1){
        fprintf(stderr, "Error receiving data %d\n", errno);
    }

    //Assign ArrayInputStream with enough memory
    google::protobuf::io::ArrayInputStream ais(buffer,dataSize+4);
    CodedInputStream coded_input(&ais);

    //Read an unsigned integer with Varint encoding, truncating to 32 bits.
    coded_input.ReadVarint32(&dataSize);

    // prevent the CodedInputStream from reading beyond that length.Limits are used when parsing length-delimited
    //embedded messages
    google::protobuf::io::CodedInputStream::Limit msgLimit = coded_input.PushLimit(dataSize);

    //De-Serialize
    workerlog.ParseFromCodedStream(&coded_input);

    //undo the limit from line 125
    coded_input.PopLimit(msgLimit);

    //Print the message
    cout<<"Client message "<< workerlog.DebugString();
    cout <<"\n\n"<<endl;

    //printInfo(workerlog);

}


/* Send message to all workers */
void* receiveMessage(void* lp){
    char worker_message[SIZE] = {0};
    int bytecount=0;
    worker_t *worker = (worker_t *)lp;
    char buffer[4];

    while(true){
        if(strlen(message) != '\0') {
            /*pthread_mutex_lock(&mutexBuffer);
            char *token[5]; //parse message
            int   token_count = 0;

            // Pointer to point to the token parsed by strsep
            char *arg_ptr;
            char *working_str  = strdup(message);

            // move the working_str pointer to keep track of its original value so we can deallocate
            // the correct amount at the end
            char *working_root = working_str;

            // Tokenize the server_message with whitespace used as the delimiter
            while ( ( (arg_ptr = strsep(&working_str, WHITESPACE ) ) != NULL) &&
                    (token_count<MAX_NUM_ARGUMENTS))
            {
                token[token_count] = strndup( arg_ptr, MAX_MESSAGE_SIZE );
                if( strlen( token[token_count] ) == 0 )
                {
                    token[token_count] = NULL;
                }
                token_count++;
            }
            cout << message << endl;
            cout << token[0] <<endl;
            cout << token[1] <<endl;*/
            cout << message << endl;
            if(strcmp(message,"shutdown")==0)
            {
                if((bytecount = recv(worker->sockfd, worker_message, SIZE, 0)) == -1) {
                    cerr << "Error receiving data "  << endl;
                }
                cout << worker_message << endl;
                cout << "Shutting down leader ..." << endl;
                exit(0);
            }
           /* else if ((strcmp(token[0], "make")==0 && strcmp(token[1], "index")==0) ||
                     ((strcmp(token[0],"add")==0) && (token[1]!=NULL) && (token[2] !=NULL))||
                     ((strcmp(token[0],"query")==0) && (token[1]!=NULL))||
                     ((strcmp(token[0],"remove")==0) && (token[1]!=NULL) && (token[2] !=NULL)))
            {
                /*while (1) {
                    //Peek into the socket and get the packet size
                    if((bytecount = recv(worker->sockfd,buffer,4, MSG_PEEK))== -1){
                        fprintf(stderr, "Error receiving data %d\n", errno);
                    }else if (bytecount == 0){
                        cout << worker->name << "disconnected" << endl;
                        break;
                    }
                    //readBody((void*)worker,readHdr(buffer));
                    break;
                }
            }*/
            else
            {
                cout << "Invalid Command" <<endl;
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
            pthread_mutex_unlock(&workers_mutex);
        }
    }
}


void catch_ctrl_c_and_exit(int sig) {
    flag = 1;
}
