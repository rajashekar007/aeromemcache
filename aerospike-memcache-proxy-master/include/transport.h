/*
 * transport.h
 *
 *  Created on: Mar 17, 2012
 *      Author: carolyn
 */

#ifndef TRANSPORT_H_
#define TRANSPORT_H_

typedef struct {
	char *hostname;
	int  port;
	int  sockfd;
}cl_TCP_transport;

typedef struct {
 	char *name;
 	int  sockfd;
}cl_unix_socket_transport;

typedef struct {
 	const char *name; // name will have prepended ".toServer" and ".toClient"
 	int fdRead;
 	int fdWrite;
}cl_pipe_transport;

typedef enum{
	TRANSPORT_INVALID = 0,
	TRANSPORT_TCP_SOCKET,
	TRANSPORT_UDP_SOCKET,
	TRANSPORT_UNIX_SOCKET,
	TRANSPORT_PIPE
}cl_transport_type;



#endif /* TRANSPORT_H_ */
