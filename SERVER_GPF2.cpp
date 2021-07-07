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
#include "message.pb.h"
#include <iostream>
#include <vector>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#define SIZE 1000

using namespace std;
using namespace google::protobuf::io;



void* SocketHandler(void*);

int main(int argv, char** argc){

        int host_port= 9015;

        struct sockaddr_in my_addr;
        vector<string> ip_addr;

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
                        ip_addr.push_back(inet_ntoa(sadr.sin_addr));
                        pthread_create(&thread_id,0,&SocketHandler, (void*)csock );
                        pthread_detach(thread_id);
                }
                else{
                        fprintf(stderr, "Error accepting %d\n", errno);
                }
        }

FINISH:
;
}

google::protobuf::uint32 readHdr(char *buf)
{
  google::protobuf::uint32 size;
  google::protobuf::io::ArrayInputStream ais(buf,4);
  CodedInputStream coded_input(&ais);
  coded_input.ReadVarint32(&size);//Decode the HDR and get the size
  //cout<<"size of data is "<<size<<endl;
  return size;
}

void readBody(int csock,google::protobuf::uint32 siz)
{
  int bytecount;
  info data;
  char buffer [siz+4];//size of the data and hdr

  //Read the entire buffer including the hdr
  if((bytecount = recv(csock, (void *)buffer, 4+siz, MSG_WAITALL))== -1){
                fprintf(stderr, "Error receiving data %d\n", errno);
        }
        

  //Assign ArrayInputStream with enough memory 
  google::protobuf::io::ArrayInputStream ais(buffer,siz+4);
  CodedInputStream coded_input(&ais);

  //Read an unsigned integer with Varint encoding, truncating to 32 bits.
  coded_input.ReadVarint32(&siz);

  //After the message's length is read, PushLimit() is used to prevent the CodedInputStream 
  //from reading beyond that length.Limits are used when parsing length-delimited 
  //embedded messages
  google::protobuf::io::CodedInputStream::Limit msgLimit = coded_input.PushLimit(siz);

  //De-Serialize
  data.ParseFromCodedStream(&coded_input);

  //Once the embedded message has been parsed, PopLimit() is called to undo the limit
  coded_input.PopLimit(msgLimit);

  //Print the message
  cout<<"Message is "<<data.DebugString();

}

void* SocketHandler(void* lp){
    int *csock = (int*)lp;

        char buffer[4];
        int bytecount=0;
        info logp;
        char message[1024] ="This is the query\n";

        memset(buffer, '\0', 4);

        while(1)
        {
            char client_message[SIZE] = "NULL";
            cin.getline(message,1000);

            if( (bytecount=send(*csock,message,1000,0))== -1 ) {
                    cerr << "Error sending data "  << endl;
            }

            if(strcmp(message,"query")==0)
            {
              while (1) {

              //Peek into the socket and get the packet size
              if((bytecount = recv(*csock,buffer,4, MSG_PEEK))== -1){
              fprintf(stderr, "Error receiving data %d\n", errno);
              }else if (bytecount == 0)
                break;
              readBody(*csock,readHdr(buffer));
              continue;
        
              }
            }
            else if(strcmp(message,"shutdown")==0)
            {
              if((bytecount=recv(*csock, client_message,SIZE,0 ))==-1){
                   cerr << "Error receiving data "  << endl;
              }
              cout << client_message << endl;
              cout << "Shutting down leader ..." << endl;
              exit(0);
            }
            else {
              if((bytecount=recv(*csock, client_message,SIZE,0 ))==-1){
                   cerr << "Error receiving data "  << endl;
              }
              cout << client_message << endl;
            }
        } 
        

FINISH:
        free(csock);
    return 0;
}
