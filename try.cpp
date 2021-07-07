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
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "message.pb.h"
#include <iostream>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <deque>
#include <thread>
#include <atomic>
#include <mutex>
#include <tuple>
#include <vector>


#include "ouroboros.hpp"

#define SIZE 1024
#define PORT 9015


#define NUM_INDEX_THREADS 4
#define BLOCK_SIZE 262144


#define MAX_NUM_ARGUMENTS 10
#define WHITESPACE " \t\n"
#define MAX_MESSAGE_SIZE 250
#define CAPACITY 1000 // Size of the Hash Table

#define NAME_LENGTH 256

#define QUEUE_SIZE_RATIO 4
#define DELIMITERS " \t\n/_.,;:"

char* pkt;
int siz;


pthread_mutex_t mutexBuffer;

using namespace google::protobuf::io;
using namespace std;

volatile sig_atomic_t flag = 0;
char message[SIZE];
char name[NAME_LENGTH];

using namespace ouroboros;

struct FilesQueue{
    deque<char*> queue;
    mutex mtx;
};

typedef struct HTItems HTItems;
struct HTItems {
    char* fileName;
    char* filePath;
};


typedef struct LinkedList LinkedList;
struct LinkedList {
    HTItems* item;
    LinkedList* next;
};


typedef struct HashTable HashTable;
struct HashTable {
    HTItems** items;
    LinkedList** overflow_buckets;
    int size;
    int count;
};

unsigned long hashFunction(char* );
void printHT(HashTable*);
static LinkedList* allocate_list ();
static LinkedList* linkedlist_insert(LinkedList*, HTItems*);
static HTItems* linkedlist_removeFile(LinkedList*);
static void free_linkedlist(LinkedList* );
static LinkedList** create_overflow_buckets(HashTable* );
HTItems* create_item(char*, char*);
HashTable* create_table(int);
void free_item(HTItems*);
void free_table(HashTable*);
void handle_collision(HashTable*, unsigned long , HTItems*);
char* search(HashTable* , char* );
void printSearch(Worker*, HashTable* , char* );
void printHT(HashTable*);
int index(Worker*);
int addFile(Worker*, HashTable*, char*, char*);
int removeFile(Worker*, HashTable*, char*, char*);
void* sendMessage(void*);
void* receiveMessage(void*);
void initialize(int,int, MemoryComponentManager*, FilesQueue*,
                vector<StdDirectTFIDFIndex*>*, vector<StdAppendFileIndex*>*,
                vector<thread>&, vector<thread>&);
void indexFile(char* , FilesQueue*);
void workRead(MemoryComponentManager*, FilesQueue*, StdAppendFileIndex*&,
              unsigned int, int);
void workIndex(MemoryComponentManager* , StdDirectTFIDFIndex*& ,unsigned int ,int );
void searchTerm(Worker*,char* , int, vector<StdDirectTFIDFIndex*>*, vector<StdAppendFileIndex*>* );
void shutdown(int, MemoryComponentManager* , FilesQueue* ,
              vector<StdAppendFileIndex*>* ,
              vector<StdDirectTFIDFIndex*>* ,
              vector<thread>& ,
              vector<thread>&  );
void workInit(MemoryComponentManager*, unsigned int, int, int);


int main(int argv, char** argc){

    WorkersLog workers_log;
    HashTable* ht = create_table(CAPACITY);
    Worker worker;

    char host_name[]="172.20.3.153";
    int size;
    struct sockaddr_in my_addr;

    char buffer[1024];
    char server_message[1024]={0};
    int bytecount;
    int buffer_len=0;
    int node_name;

    int hsock;
    int * p_int;
    int err;

    pthread_mutex_init(&mutexBuffer, NULL);

    int hsock;
    int * p_int;
    int err;

    // create each queue in a specific NUMA node and add the queues to the manager
    MemoryComponentManager* manager;
    manager = new MemoryComponentManager();
    FilesQueue* files_queue;

    vector<StdAppendFileIndex*>* fileIndexes;
    vector<StdDirectTFIDFIndex*>* termsIndexes;

    //allocating memory for files_queue
    files_queue = new FilesQueue{};

    //allocating memory for findex that stores files
    fileIndexes = new vector<StdAppendFileIndex*>();

    //allocating memory for index that stores terms
    termsIndexes = new vector<StdDirectTFIDFIndex*>();

    // list of threads that will read the contents of the input files
    vector<thread> threads_read;

    // list of threads that will index the contents of the input files
    vector<thread> threads_index;

    initialize(NUM_INDEX_THREADS, BLOCK_SIZE, manager, files_queue, termsIndexes,
               fileIndexes, threads_read, threads_index);

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
    my_addr.sin_port = htons(PORT);

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
            else if(bytecount==0 || strcmp(message, "shutdown") == 0){
                flag =1;
            }
            else{
                char *token[5]; //parse server_message
                int   token_count = 0;

                // Pointer to point to the token parsed by strsep
                char *arg_ptr;
                char *working_str  = strdup(server_message);

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
                char client_message[SIZE] = "NULL";


            }
            pthread_mutex_unlock(&mutexBuffer);

        }

    }
}


