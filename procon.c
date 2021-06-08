#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <semaphore.h>

#define THREAD_NUM 8

sem_t semEmpty;
sem_t semFull;

pthread_mutex_t mutexBuffer;

int buffer[10];
int count = 0;

void* producer(void* args)
{ 
  while(1)
  {
	//produce
	int x = rand() % 100;
	
	
	//Add to buffer
	sem_wait(&semEmpty); //if semEmpty is 0, wait
	pthread_mutex_lock(&mutexBuffer);
	buffer[count] =x;
	count ++;
	
	pthread_mutex_unlock(&mutexBuffer);
	sem_post(&semFull);//increment sem_full
  }
}

void* consumer(void* args)
{
  while(1)
  {
  	int y;
  	
	//Remove from the buffer
	sem_wait(&semFull); //if semFull is 0, wait
	
	pthread_mutex_lock(&mutexBuffer);

	y = buffer[count -1];
	count --;
	
	pthread_mutex_unlock(&mutexBuffer);
	sem_post(&semEmpty); //there's an empty slot now that we have taken something from it
	
	
	//consume
	printf("Got %d\n\n", y);
	sleep(1);
  }

}

int main(int argc, char* argv[])
{
	pthread_t thread[THREAD_NUM];
	pthread_mutex_init(&mutexBuffer, NULL);
	sem_init(&semEmpty,0,10);
	sem_init(&semFull,0,0);
	
	//,only 1 process, no of slots
	int i;
	for(int i=0; i< THREAD_NUM; i++)
	{
		if(i>0 ==0) //7 elements per second, 7 producers
		{
			if(pthread_create(&thread[i], NULL, &producer, NULL) != 0)
			{
				perror("Failed to create thread");
			}
		}
		else
		{
			if(pthread_create(&thread[i], NULL, &consumer, NULL) != 0)
			{
				perror("Failed to create thread");
			}
		}
	}
	for(int i=0; i< THREAD_NUM; i++)
	{
		if(pthread_join(thread[i], NULL) != 0)
		{
			perror("Failed to join thread");
		}
		
	}
	sem_destroy(&semEmpty);
	sem_destroy(&semFull);
	
	pthread_mutex_destroy(&mutexBuffer);
	return 0;

}
