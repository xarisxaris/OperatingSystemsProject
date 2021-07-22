/* server.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/
//Dimitrios Samourelis 2811 
//Xarisis Gekas 2668
#include <signal.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"
#include <sys/time.h>
#include<stdio.h>

//******************
long secs=0, usecs=0;
long total_service_time = 0.0;
long total_waiting_time = 0.0;
int tail=0;
int head=0;
int completed_requests = 0;
int reader_count = 0,writer_count=0;

pthread_mutex_t mutex_queue= PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_for_time = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_put_get = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t cond_non_full_queue = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_non_empty_queue = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_enter  = PTHREAD_COND_INITIALIZER;


//***********************
#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10
#define threads_num         10
#define queue_size 30
#define sec_to_usec 1000000

// Definition of the operation type.
typedef enum operation 
{
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request 
{
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;


struct description{

	int connection_fd;
	struct timeval starting_time;
};

typedef struct description description_t;

struct timeval before_execute,after_execute;


description_t queue[queue_size];

// Definition of the database.
KISSDB *db = NULL;


/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.


 */

Request *parse_request(char *buffer) 
{
	char *token = NULL;
 	Request *req = NULL;
  
  // Check arguments.
 	 if (!buffer)
    	return NULL;
  
  // Prepare the request.
 	 req = (Request *) malloc(sizeof(Request));
   memset(req->key, 0, KEY_SIZE);
   memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
 	 token = strtok(buffer, ":");    
 	 if (!strcmp(token, "PUT")) 
 	 {
   	 	req->operation = PUT;
   } 
  	else if (!strcmp(token, "GET")) 
 	 {
    	req->operation = GET;
 	 } 
	 else 
   {
      free(req);
    	return NULL;
   }
  
  // Extract the key.
 	 token = strtok(NULL, ":");
 	 if (token) 
 	 {
    	strncpy(req->key, token, KEY_SIZE);
   } 
   else
   {
   		free(req);
    	return NULL;
   }
  
  // Extract the value.
  	token = strtok(NULL, ":");
  	if (token) 
 	  {
    	strncpy(req->value, token, VALUE_SIZE);
  	} 
  	else if (req->operation == PUT) 
  	{
   		free(req);
    	return NULL;
  	}
 	return req;
}





/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
void process_request(const int socket_fd)
{
  	char response_str[BUF_SIZE], request_str[BUF_SIZE];
    int numbytes = 0;
    Request *request = NULL;

    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);
    
    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);
    
    // parse the request.
    if (numbytes) 
    {
      request = parse_request(request_str);
      if (request) 
      {
        switch (request->operation) 
        {
          case GET:
			pthread_mutex_lock(&mutex_put_get);
            // Read the given key from the database.
			
			while(writer_count!=0){
				pthread_cond_wait(&cond_enter,&mutex_put_get);
			}
			reader_count++;

			pthread_mutex_unlock(&mutex_put_get);

            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);
            break;
			
			pthread_mutex_lock(&mutex_put_get);

			reader_count--;
	
			pthread_cond_signal(&cond_enter);

			pthread_mutex_unlock(&mutex_put_get);
          case PUT:
			pthread_mutex_lock(&mutex_put_get);

			while(reader_count || writer_count ){
				pthread_cond_wait(&cond_enter,&mutex_put_get);
			}
			writer_count++;
			pthread_mutex_unlock(&mutex_put_get);
            // Write the given key/value pair to the database.
            if (KISSDB_put(db, request->key, request->value)) 
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");
			pthread_mutex_lock(&mutex_put_get);

			writer_count--;
			pthread_cond_signal(&cond_enter);
            pthread_mutex_unlock(&mutex_put_get);
            break;
          default:
            // Unsupported operation.
            sprintf(response_str, "UNKOWN OPERATION\n");
        }
        // Reply to the client.
        write_str_to_socket(socket_fd, response_str, strlen(response_str));
        if (request)
          free(request);
        request = NULL;
        return;
      }
    }
    // Send an Error reply to the client.
    sprintf(response_str, "FORMAT ERROR\n");
    write_str_to_socket(socket_fd, response_str, strlen(response_str));
}


void *consumer(void *arg){

	int new_connection_fd=0;

    while(1)
    {
        pthread_mutex_lock(&mutex_queue);

        while(tail - head == 0)
        {
            pthread_cond_wait(&cond_non_empty_queue,&mutex_queue);
        }
		
		head=(head+1)%queue_size;

        new_connection_fd=queue[tail-head].connection_fd;

		pthread_cond_signal(&cond_non_full_queue);

        pthread_mutex_unlock(&mutex_queue);


        gettimeofday(&before_execute,NULL);

		process_request(new_connection_fd);
		
		gettimeofday(&after_execute,NULL);

        pthread_mutex_lock(&mutex_for_time);

		
		secs = queue[tail-head].starting_time.tv_sec;
        usecs = queue[tail-head].starting_time.tv_usec;

        total_waiting_time += (before_execute.tv_sec*sec_to_usec + before_execute.tv_usec) -( secs*sec_to_usec - usecs);

		total_service_time += (after_execute.tv_sec - before_execute.tv_sec)*sec_to_usec + (after_execute.tv_usec - before_execute.tv_usec);

		completed_requests++;
		
        pthread_mutex_unlock(&mutex_for_time);

        close(new_connection_fd);
    }

}

long compute_average_total_waiting_time(long total_waiting_time,int completed_requests){
	
	double average_total_waiting_time=0.0;

	average_total_waiting_time = (total_waiting_time / (double)completed_requests);
	
	return average_total_waiting_time;
}

long compute_average_total_service_time(long total_service_time, int completed_requests){
	
	double average_total_service_time=0.0;

	average_total_service_time = (total_service_time /(double)completed_requests);
	
	return average_total_service_time;
}

void my_handler(int sig)
{
	double avg = compute_average_total_waiting_time(total_waiting_time,completed_requests);

	double avg2 = compute_average_total_service_time(total_service_time,completed_requests);

	printf("Completed requests: %d\n",completed_requests);
	printf("Average total waiting time in usecs: %.2f\n",avg);
	printf("Average total service time in usecs: %.2f\n",avg2);

}

void add_to_queue(int new_fd , struct timeval starting_time){
		
	queue[tail-head].connection_fd = new_fd;

    queue[tail-head].starting_time.tv_sec = starting_time.tv_sec;

    queue[tail-head].starting_time.tv_usec = starting_time.tv_usec;

    tail=(tail+1)%queue_size;
}

int main() 
{

  struct timeval starting_time;
  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information
  pthread_t consumers[threads_num];
  int i=0;

  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);
  signal(SIGTSTP,my_handler);
 
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);

  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) 
  {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) 
  {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }

  for(i=0; i<threads_num; i++)
  {
      pthread_create(&consumers[i],NULL,consumer,NULL);

  }

  // main loop: wait for new connection/requests
  while (1) 
  { 
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) 
    {
      ERROR("accept()");
    }
    
    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));
    pthread_mutex_lock(&mutex_queue);

    while(tail -head == queue_size)
    {
          pthread_cond_wait(&cond_non_full_queue,&mutex_queue);
    }

    gettimeofday(&starting_time,NULL);
	
	add_to_queue(new_fd , starting_time);
	
	pthread_cond_signal(&cond_non_empty_queue);

    pthread_mutex_unlock(&mutex_queue);

  } 
  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}



/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */

