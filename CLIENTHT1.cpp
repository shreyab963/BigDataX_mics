
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

#define SIZE 1000
#define PORT 9017

#define MAX_NUM_ARGUMENTS 10
#define WHITESPACE " \t\n" 
#define MAX_MESSAGE_SIZE 250 
#define CAPACITY 1000 // Size of the Hash Table

char* pkt;
int siz;

using namespace google::protobuf::io;
using namespace std;
 

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
int printSearch(Worker*, HashTable* , char* );
void printHT(HashTable*);
 
int index(Worker*);
int addFile(Worker*, HashTable*, char*, char*);
int removeFile(Worker*, HashTable*, char*, char*);

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
        my_addr.sin_port = htons(PORT);

        


        memset(&(my_addr.sin_zero), 0, 8);
        my_addr.sin_addr.s_addr = inet_addr(host_name);
        if( connect( hsock, (struct sockaddr*)&my_addr, sizeof(my_addr)) == -1 ){
                if((err = errno) != EINPROGRESS){
                        fprintf(stderr, "Error connecting socket %d\n", errno);
                        goto FINISH;
                }
        }
     
        recv(hsock, server_message,1000,0);
        cout << server_message <<endl;

        while( (bytecount = recv(hsock , server_message , SIZE, 0)) > 0 )
        {

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

            if(strcmp(token[0], "shutdown")==0)
            {
                strcpy(client_message, "Shutting down client\n");
                if( (bytecount=send(hsock, client_message ,SIZE,0))== -1 ) 
                {
                        cerr << "Error sending shutdown message\n "  << endl;
                        goto FINISH;
                }
                cout << client_message << endl;
                return 0;
            }

            else if(strcmp(token[0], "make")==0 && strcmp(token[1], "index")==0)
            {
                siz= index(workers_log.add_workers());             
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
                printHT(ht);
            }

            else if((strcmp(token[0],"add")==0) && (token[1]!=NULL) && (token[2] !=NULL))
            {
               siz= addFile(workers_log.add_workers(),ht,token[1],token[2]);             
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
                printHT(ht);
            }

            else if((strcmp(token[0],"query")==0) && (token[1]!=NULL))
            {
                siz= printSearch(workers_log.add_workers(),ht,token[1]); 
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
            }

            else if((strcmp(token[0],"remove")==0) && (token[1]!=NULL) && (token[2] !=NULL))
            {
               siz= removeFile(workers_log.add_workers(),ht,token[1],token[2]);             
                if( (bytecount=send(hsock,(void*)pkt,siz,0))== -1 ) 
                {
                        cerr << "Error sending data\n " << endl;   
                }
                printHT(ht);
            }
            else
            {
                strcpy(client_message, "Invalid\n");
                if( (bytecount=send(hsock, client_message ,SIZE,0))== -1 ) 
                {
                        cerr << "Error sending shutdown message\n "  << endl;
                        goto FINISH;
                }
                cout << client_message << endl;
            }
        }  
        delete pkt;
FINISH:
        close(hsock);
}

int index(Worker* worker)
{
    Worker::Index* index = worker->add_indexes();
    index->set_indexed("Indexing sucessful");
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);
return siz;
}

unsigned long hashFunction(char* str) {
    unsigned long i = 0;
    for (int j=0; str[j]; j++)
        i += str[j];
    return i % CAPACITY;
} 
 
static LinkedList* allocate_list () {
    LinkedList* list = (LinkedList*) malloc (sizeof(LinkedList));
    return list;
}
 
static LinkedList* linkedlist_insert(LinkedList* list, HTItems* item) {
    if (!list) {
        LinkedList* head = allocate_list();
        head->item = item;
        head->next = NULL;
        list = head;
        return list;
    } 
     
    else if (list->next == NULL) {
        LinkedList* node = allocate_list();
        node->item = item;
        node->next = NULL;
        list->next = node;
        return list;
    }
 
    LinkedList* temp = list;
    while (temp->next->next) {
        temp = temp->next;
    }
    LinkedList* node = allocate_list();
    node->item = item;
    node->next = NULL;
    temp->next = node; 
    return list;
}
 
 //removeFile head and return item of removeFiled elemet
static HTItems* linkedlist_removeFile(LinkedList* list) {
    if (!list)
        return NULL;
    if (!list->next)
        return NULL;
    LinkedList* node = list->next;
    LinkedList* temp = list;
    temp->next = NULL;
    list = node;
    HTItems* it = NULL;
    memcpy(temp->item, it, sizeof(HTItems));
    free(temp->item->fileName);
    free(temp->item->filePath);
    free(temp->item);
    free(temp);
    return it;
}
 
static void free_linkedlist(LinkedList* list) {
    LinkedList* temp = list;
    while (list) {
        temp = list;
        list = list->next;
        free(temp->item->fileName);
        free(temp->item->filePath);
        free(temp->item);
        free(temp);
    }
}
 
static LinkedList** create_overflow_buckets(HashTable* table) {
    // Create the overflow buckets; an array of linkedlists
    LinkedList** buckets = (LinkedList**) calloc (table->size, sizeof(LinkedList*));
    for (int i=0; i<table->size; i++)
        buckets[i] = NULL;
    return buckets;
}
 
static void free_overflow_buckets(HashTable* table) {
    // Free all the overflow bucket lists
    LinkedList** buckets = table->overflow_buckets;
    for (int i=0; i<table->size; i++)
        free_linkedlist(buckets[i]);
    free(buckets);
}
 
 
HTItems* create_item(char* fileName, char* filePath) {
    HTItems* item = (HTItems*) malloc (sizeof(HTItems));
    item->fileName = (char*) malloc (strlen(fileName) + 1);
    item->filePath = (char*) malloc (strlen(filePath) + 1);
     
    strcpy(item->fileName, fileName);
    strcpy(item->filePath, filePath);
 
    return item;
}
 
HashTable* create_table(int size) {
    HashTable* table = (HashTable*) malloc (sizeof(HashTable));
    table->size = size;
    table->count = 0;
    table->items = (HTItems**) calloc (table->size, sizeof(HTItems*));
    for (int i=0; i<table->size; i++)
        table->items[i] = NULL;
    table->overflow_buckets = create_overflow_buckets(table);
 
    return table;
}
 
void free_item(HTItems* item) {
    free(item->fileName);
    free(item->filePath);
    free(item);
}
 
void free_table(HashTable* table) {
    for (int i=0; i<table->size; i++) {
        HTItems* item = table->items[i];
        if (item != NULL)
            free_item(item);
    }
 
    free_overflow_buckets(table);
    free(table->items);
    free(table);
}
 
void handle_collision(HashTable* table, unsigned long index, HTItems* item) {
    LinkedList* head = table->overflow_buckets[index]; 
    if (head == NULL) {
        head = allocate_list();
        head->item = item;
        table->overflow_buckets[index] = head;
        return;
    }
    else {
        table->overflow_buckets[index] = linkedlist_insert(head, item);
        return;
    }
 }
 
int addFile(Worker* worker, HashTable* table, char* fileName, char* filePath) {
    HTItems* item = create_item(fileName, filePath);
   
    unsigned long indexHT = hashFunction(fileName);
    Worker::Index* index = worker->add_indexes();
    index->set_file_name(fileName);
    index->set_file_path(filePath);
 
    HTItems* current_item = table->items[indexHT];
     
    if (current_item == NULL) {
        if (table->count == table->size) {
            printf("Insert Error: Hash Table is full\n");
            index->set_file_added("No storage left in the disk");
            free_item(item);
            goto FINISH;
        } 
        // Insert directly
        table->items[indexHT] = item; 
        table->count++;
        index->set_file_added("File Successfully Added");
        cout << fileName << " successfully added\n" <<endl;
    }
    else {
            // Scenario 1: We only need to update filePath
            if (strcmp(current_item->fileName, fileName) == 0) {
                strcpy(table->items[indexHT]->filePath, filePath);
                index->set_file_added("File Successfully Added");
                goto FINISH;
            }
        else {
            // Scenario 2: Collision
            handle_collision(table, indexHT, item);
            goto FINISH;
        }
    }

FINISH:
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output); 
    return siz;
}
 
char* search(HashTable* table, char* fileName) {
    int index = hashFunction(fileName);
    HTItems* item = table->items[index];
    LinkedList* head = table->overflow_buckets[index];
 
    // Ensure that we move to items which are not NULL
    while (item != NULL) {
        if (strcmp(item->fileName, fileName) == 0)
            return item->filePath;
        if (head == NULL)
            return NULL;
        item = head->item;
        head = head->next;
    }
    return NULL;
}

int removeFile(Worker* worker,HashTable* table, char* fileName, char* filePath) {
    // Deletes an item from the table
    int indexHT = hashFunction(fileName);
    HTItems* item = table->items[indexHT];
    LinkedList* head = table->overflow_buckets[indexHT];
    char* val = search(table, fileName);
    Worker::Index* index = worker->add_indexes();
    index->set_file_name(fileName);
    index->set_file_path(val);
 
    if (item == NULL) {//doesn't exist
        index->set_file_removed("File doesn't exist");
        goto FINISH;
    }
    else {
        if (head == NULL && strcmp(item->fileName, fileName) == 0) {//no collsion
            table->items[indexHT] = NULL;
            free_item(item);
            table->count--;
            cout << fileName << " removed"<<endl;
            index->set_file_removed("File Successfully removed");
            goto FINISH;
        }
        else if (head != NULL) { //collision, set head as new file
            if (strcmp(item->fileName, fileName) == 0) {
                free_item(item);
                LinkedList* node = head;
                head = head->next;
                node->next = NULL;
                table->items[indexHT] = create_item(node->item->fileName, node->item->filePath);
                free_linkedlist(node);
                table->overflow_buckets[indexHT] = head;
                cout << fileName << " removed"<<endl;
                index->set_file_removed("File Successfully removed");
                goto FINISH;
            }
            LinkedList* curr = head;
            LinkedList* prev = NULL; 
            while (curr) {
                if (strcmp(curr->item->fileName, fileName) == 0) {
                    if (prev == NULL) {
                        // First element of the chain. removeFile the chain
                        free_linkedlist(head);
                        table->overflow_buckets[indexHT] = NULL;
                        cout << fileName << " removed"<<endl;
                        index->set_file_removed("File Successfully removed");
                        goto FINISH;
                    }
                    else {
                        // This is somewhere in the chain
                        prev->next = curr->next;
                        curr->next = NULL;
                        free_linkedlist(curr);
                        table->overflow_buckets[indexHT] = head;
                        cout << fileName << " removed"<<endl;
                        index->set_file_removed("File Successfully removed");
                        goto FINISH;
                    }
                }
                curr = curr->next;
                prev = curr;
            }
        }
    }
FINISH:    
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);

    return siz;
}
 
int printSearch(Worker* worker,HashTable* table, char* fileName) {
    char* val;
    char temp[11];
    pid_t pid = getpid();
    sprintf(temp, "%d", pid);

    char pID[20] = "Query ID: ";
    strcat(pID, temp);
    
    Worker::Query* query1 = worker->add_queries();
    if ((val = search(table, fileName)) == NULL) {
        printf("%s does not exist\n", fileName);
        query1->set_result_found("Match for the query not found");
        goto FINISH;
    }
    else {
        printf("fileName:%s, filePath:%s\n", fileName, val);
        query1->set_result_found("Match for the query found");
        query1->set_path(val);
    }

FINISH:   
    query1->set_query(fileName);
    query1->set_query_id(pID);
    siz = worker->ByteSizeLong()+4;
    pkt = new char [siz];
    google::protobuf::io::ArrayOutputStream aos(pkt,siz);
    CodedOutputStream *coded_output = new CodedOutputStream(&aos);
    coded_output->WriteVarint32(worker->ByteSizeLong());
    worker->SerializeToCodedStream(coded_output);

    return siz;
}
 
void printHT(HashTable* table) {
    printf("\n---------------------------------\n");
    for (int i=0; i<table->size; i++) {
        if (table->items[i]) {
            printf("Index:%d, fileName:%s, filePath:%s", i, table->items[i]->fileName, table->items[i]->filePath);
            if (table->overflow_buckets[i]) {
                printf(" => Overflow Bucket => ");
                LinkedList* head = table->overflow_buckets[i];
                while (head) {
                    printf("fileName:%s, filePath:%s ", head->item->fileName, head->item->filePath);
                    head = head->next;
                }
            }
            printf("\n");
        }
    }
    printf("----------------------------------\n");
}
