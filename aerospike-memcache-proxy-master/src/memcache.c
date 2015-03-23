/*
 * memcache.c
 *
 *  Created on: Mar 17, 2012
 *  Memcache connector for Citrusleaf database.
 *  Copyright 2012, Citrusleaf Inc
 *
 *  - CSW
 */
#include <sys/socket.h>
#include <netdb.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <libxml2/libxml/xmlexports.h>
#include <libxml2/libxml/xmlstring.h>
#include <libxml2/libxml/tree.h>

#include "citrusleaf/cf_shash.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/citrusleaf.h"
#include "proxy.h"
#include "memcache.h"
#include "memcache_protocol.h"
#include "reactor.h"
#include "xmlUtils.h"
#include "log.h"



static void memcache_init();
static void memcache_shutdown();
static void *memcache_worker(void *arg);


// the memcache_msg structure is
// used for holding message coming from the
// the memcache client, plus some necessary
// metadata.
typedef struct{
	int32_t  fd;        // fd on which this message was sent
	uint32_t fd_uid;    // unique id for the fd, to prevent us from ever sending on the wrong socket
	uint32_t idx;       // used only if this is a queued request - idx of request.
	uint32_t batch_idx; // used only if this is a batch idx      - idx of batch operation.
	int32_t bytes_read; // total humber of bytes read - header + body
	memcache_connector *connector;
	memcache_header header;
	uint8_t *body;
}memcache_msg;

// each batch request on a given fd has a structure representing the state
// - this state contains the total number of requests (so far) in the batch,
// the number of requests that the server has not yet responded to, whether
// or not we've received all the requests, and a linked list of the responses.
// Once we've received all of the requests and are no longer waiting for
// responses, we can send the full response back to the application.
typedef struct{
	uint32_t n_requests;
	uint32_t n_outstanding;
	uint32_t requests_complete; // are we finished adding to this batch job?
	cf_ll *responses;
}batch_state;

typedef struct{
	cf_ll_element *next;
	cf_ll_element *prev;
	memcache_header *response; // may be NULL, if we don't know the response yet.
}batch_response_elem;

// Each open connection to the application is associated with some
// additional connection state information. This information includes
// whether or not we're currently in batch mode, the index of the current
// (or next, if we're not in batch mode) batch operation, and possibly
// a partially built message from the application
typedef struct{
	uint16_t      batch_idx;        // id of the batch currently in-process on this fd
	uint16_t      in_batch;         // whether there's a batch currently in process on this id
	uint32_t      fd_uid;           // unique id of this fd, to help if fd gets reused while messages are in-flight
	uint32_t      shutdown;         // if true, in the process of shutting down
	cf_queue     *fd_msg_queue;     // per-fd message queue, iff we're doing strict pipelining.
	pthread_t     fd_worker_thread; // per-fd worker thread, iff we're doing strict pipelining
	memcache_msg *cur_msg;
}fd_state;

// This structure is used as a key for the hash of batch states
typedef struct{
	int fd;
	uint32_t batch_idx;
}fd_and_idx;


static cf_queue *g_free_msg_queue;   // free queue for memcache headers and metadata
static cf_queue *g_requests_queue;   // queue of incoming messages, waiting to be taken by worker threads!
static shash    *g_fd_state_hash;    // hash (by fd), containing information about that connection - the current batch idx, the partially completed message, etc
static shash    *g_batch_state_hash; // hash (by fd + batch_idx) containing the list of queued responses for batch type commands
static cf_ll    *g_workers_list;     // linked list of worker threads
static int       g_running            = false;  // flag used to stop threads on shutdown
static int       g_memcache_init_done = false;  // only initialize underlying memcache singletons once
static uint32_t  g_fd_uid             = 0; // unique id for each open connection - prevents erroneous sending if we reuse fds.
static int       g_num_connectors     = 0;
static int       g_strict_pipelining  = true;


static cf_ll_element *
cf_ll_get_at(cf_ll *list, unsigned int idx)
{
	cf_ll_element *elem = cf_ll_get_head(list);
	unsigned int cur_idx = 0;
	while(elem && cur_idx < idx){
		elem = elem->next;
		cur_idx++;
	}

	return elem;
}

static memcache_msg *
get_mc_msg()
{
	memcache_msg *msg;
	// pop message off the queue
	if (0 != cf_queue_pop(g_free_msg_queue, &msg, 0)) {
		msg = (memcache_msg *)cf_malloc(sizeof(memcache_msg));
	}else{
	}
	memset(msg, 0, sizeof(memcache_msg));
	return msg;
}

static void
put_mc_msg(memcache_msg *msg)
{
	if (msg->body) {
		cf_free(msg->body);
	}
	cf_queue_push(g_free_msg_queue, &msg);
}


static void
batchResponseElemDtor(cf_ll_element *e)
{
	batch_response_elem *batch_elem  = (batch_response_elem *)e;
	if (batch_elem ->response){
		cf_free(batch_elem->response);
	}

	cf_free(e);
}

// note that quitq is not considered a
// batch operation, although it is quiet.
// It does not get queued.
static bool
is_batch_op(int op){
	switch (op){
	case MC_op_getQ:
	case MC_op_setQ:
	case MC_op_incrementQ:
	case MC_op_decrementQ:
	case MC_op_appendQ:
	case MC_op_prependQ:
	case MC_op_addQ:
	case MC_op_replaceQ:
	case MC_op_deleteQ:
	case MC_op_flushQ:
	case MC_op_GATQ:
	case MC_op_GATKQ:
	case MC_op_getKQ:
		return true;
	}

	return false;
}
// Have a full memcache message. Set state on the
// fd, and send it to one of the worker threads.
static void
memcache_set_batch(memcache_msg *msg, fd_state *info)
{
	batch_state *b_state;
	pthread_mutex_t *vlock = NULL;

	if (info->shutdown)
		return;

	// give batch id to the message so it can be tracked
	msg->batch_idx     = info->batch_idx;

	switch (msg->header.opcode) {
	case MC_op_getQ:
	case MC_op_setQ:
	case MC_op_incrementQ:
	case MC_op_decrementQ:
	case MC_op_appendQ:
	case MC_op_prependQ:
	case MC_op_addQ:
	case MC_op_replaceQ:
	case MC_op_deleteQ:
	case MC_op_flushQ:
	case MC_op_GATQ:
	case MC_op_GATKQ:
	case MC_op_getKQ:{

		// get ready to find batch in the batch hash
		fd_and_idx batch_id;
		batch_id.fd        = msg->fd;
		batch_id.batch_idx = msg->batch_idx;

		// set up batch mode if not set up already
		if (!info->in_batch) {
			batch_state new_batch_state;
			memset(&new_batch_state, 0, sizeof(batch_state));
			new_batch_state.responses = (cf_ll *)cf_malloc(sizeof(cf_ll));
		 	cf_ll_init(new_batch_state.responses, batchResponseElemDtor, false);
		 	cl_debug("Putting batch id %d in batch state hash", batch_id.batch_idx);
		 	shash_put(g_batch_state_hash, &batch_id, &new_batch_state);
		 	info->in_batch = 1;
		}
		// Get the batch information for this fd and batch idx
		cl_debug("Batch command - Getting batch info for batch id %d", batch_id.batch_idx);
		if (0 != shash_get_vlock(g_batch_state_hash, &batch_id, (void **)&b_state, &vlock)) {
			cl_warning("Internal error - no existing batch structure for request");
			if (vlock) {
				cl_debug("unlocking batch hash on error");
				pthread_mutex_unlock(vlock);
			}
			return;
		}

		// Add a placeholder for the response
		batch_response_elem *rsp_elem = (batch_response_elem *)cf_malloc(sizeof(batch_response_elem));
		memset(rsp_elem, 0, sizeof(batch_response_elem));
		cf_ll_append(b_state->responses, (cf_ll_element *)rsp_elem);

		// set idx and counters for batch
		msg->idx = b_state->n_requests;
		b_state->n_outstanding += 1;
		b_state->n_requests    += 1;

		cl_debug("unlocking batch info after batched command");
		pthread_mutex_unlock(vlock);
		break;
	}
	case MC_op_get:
	case MC_op_set:
	case MC_op_increment:
	case MC_op_decrement:
	case MC_op_append:
	case MC_op_prepend:
	case MC_op_add:
	case MC_op_replace:
	case MC_op_delete:
	case MC_op_flush:
	case MC_op_getK:
	case MC_op_GAT:
	case MC_op_GATK:
	case MC_op_nop:
		// If we're in batch mode, stop
		if (info->in_batch) {
			fd_and_idx batch_id;
			batch_id.fd        = msg->fd;
			batch_id.batch_idx = info->batch_idx;

			info->batch_idx += 1;
			info->in_batch = false;

			cl_debug("Getting batch info to terminate batch job %d", batch_id.batch_idx);
			if (0 == shash_get_vlock(g_batch_state_hash, &batch_id, (void **)&b_state, &vlock)) {
				b_state->requests_complete = 1;
			}else{
				cl_warning("could not find batch id %d in batch hash", batch_id.batch_idx);
			}

			if (vlock) {
				cl_debug("Unlocking batch mutex after terminating batch job");
				pthread_mutex_unlock(vlock);
			}
		}else{
			cl_debug("Not in batch, not going to set batch idx");

		}
		break;
	case MC_op_version:
	case MC_op_stat:
		break; // no affect on batch mode - returns immediately anyway. (?that true?)
	default:
		break;
	}
}

static void
memcache_queue_msg(memcache_msg *msg, fd_state *state)
{
	if (state->shutdown){
		cf_free(msg);
		return;
	}

	if (g_strict_pipelining) {
		cf_queue_push(state->fd_msg_queue, &msg);
	}else{
		cf_queue_push(g_requests_queue, &msg);
	}
}

static int
reduce_delete_batch(void *key, void *data, void *udata)
{
	int fd = *(int *)udata;

	fd_and_idx *f_n_idx = (fd_and_idx *)key;
	if (f_n_idx->fd == fd) {
		// this batch state belongs to this fd. Kill kill kill.
		batch_state *b_state = (batch_state *)data;
		// there is some chance that the list has already been deleted. Check.
		if (b_state->responses) {
			util_deleteList(b_state->responses);
		}
		return SHASH_REDUCE_DELETE;
	}

	return 0;
}

static void
close_socket(int fd, fd_state *state, bool join)
{
	if (0 != reactor_deregister(get_global_reactor(), fd))
		return;  // already closed...

	pthread_mutex_t *lock = NULL;
	bool do_exit = false;

	if (state == NULL) {
		if (SHASH_OK != shash_get_vlock(g_fd_state_hash, &fd, (void **)&state, &lock)) {
			return;
		}
	}

	// release the current, in progress message
	if (state->cur_msg){
		if (state->cur_msg->body){
			cf_free(state->cur_msg->body);
		}
		cf_free(state->cur_msg);
	}

	if (g_strict_pipelining) {
		// release messages in the queue
		if (state->fd_msg_queue) {
			memcache_msg *msg;
			int rv = cf_queue_pop(state->fd_msg_queue, &msg, 0);
			while (0 == rv) {
				cf_free(msg);
				rv = cf_queue_pop(state->fd_msg_queue, &msg, 0);
			}
		}

		// This code is called both from within the associated thread
		// (internally) and from another thread (externally).
		// Internally, it's called when we get an error on the socket,
		// or when we receive a 'quit' message. In these cases we want
		// to detach and exit the thread. Externally, it's called during
		// application shutdown, in which case we want to send a shutdown
		// message to the thread, and wait for the external thread to
		// join.
		// Ah, hell. There are three threads I need to placate, not two.
		// The
		if (state->fd_worker_thread != pthread_self()){
			// send shutdown message
			memcache_msg *msg = (memcache_msg *)cf_malloc(sizeof(memcache_msg));
			memset(msg, 0, sizeof(memcache_msg));
			msg->header.magic = 0x80;
			msg->header.opcode = CL_op_thr_shutdown;
			msg->fd = fd;
			msg->fd_uid = join; // it's a convenient place to put this...
			cf_queue_push(state->fd_msg_queue, &msg);
//			cf_queue_destroy(state->fd_msg_queue); //  do not destroy! shutdown will destroy!
		}else{
			cf_queue_destroy(state->fd_msg_queue);
			pthread_detach(pthread_self());
			do_exit = true;
		}
	}

	// delete state - NB - 'state' is invalid from now on
	shash_delete_lockfree(g_fd_state_hash, &fd);
	if (lock)
		pthread_mutex_unlock(lock);

	shash_reduce_delete(g_batch_state_hash, reduce_delete_batch, (void *)&fd);

	close(fd);

	cl_debug("finish close on fd %d", fd);

	if (do_exit){
		pthread_exit(0);
	}
}

// data from the memcache client!
static void
memcache_client_cb(int fd, uint32_t epoll_events, void *udata)
{
	cl_debug("Client callback called!!");
	if (epoll_events & EPOLLIN) {
		memcache_msg *msg;
		fd_state *state;
		pthread_mutex_t *vlock;

		// first, check to see if there's a partially built message in the
		// hash.
		if (SHASH_OK != shash_get_vlock(g_fd_state_hash, &fd, (void **)&state, &vlock)) {
			cl_warning("Error getting vlock on fd - internal inconsistency!");
			return;
		}

		msg = state->cur_msg;

		while (true){
			if (!msg) {
				msg = get_mc_msg();
				msg->fd = fd;
				msg->fd_uid = state->fd_uid;
				msg->connector = (memcache_connector *)udata;
				state->cur_msg = msg;
			}else{
				cl_assert(msg->connector == (memcache_connector *)udata, "Error - incoming connector does not match partial message!");
			}

			// check - have I already read the header? If not, read it.
			cl_debug("No of bytes already read %d ",msg->bytes_read);
			if (msg->bytes_read < sizeof(memcache_header)){
				cl_debug("Attempting to read header...");
				int n_header_bytes_to_read = sizeof(memcache_header) - msg->bytes_read;
				cl_debug("Trying to read %d of header",n_header_bytes_to_read);
				int n_bytes_read = read(fd, ((uint8_t *)(&msg->header)) + msg->bytes_read, n_header_bytes_to_read);	
				if (n_bytes_read > 0) {
					msg->bytes_read += n_bytes_read;
					if (n_bytes_read < n_header_bytes_to_read) {
						// Throw it back. Didn't read enough bytes.
						cl_debug("Read %d bytes",n_bytes_read);
						cl_debug("Did not read enough bytes for header, awaiting next packet");
						break;
					}
				}else if (n_bytes_read == 0) {
					cl_debug("no bytes read - closing socket");
					close_socket(fd, state, false);
					break;
				}else {// Handle error...
					if (errno != EAGAIN && errno != EWOULDBLOCK){
						if (errno != ECONNRESET)
							cl_info("error on read, %s", strerror(errno));
						close_socket(fd, state, false);
					}else{
						cl_debug("Awaiting more data, msg %"PRIx64"", msg);
					}
					break;
				}
				// If we're here, we've just read the full header. De-network it.
				cl_debug("Operation requested %d",msg->header.opcode);
				memcache_header_from_network(&msg->header);
			}else{
				cl_debug("Number of bytes read is %d", msg->bytes_read);
				
			}

			// If I get here, I should have the full header
			if (msg->header.body_len == 0) {
				memcache_set_batch(msg, state);
				memcache_queue_msg(msg, state);
				state->cur_msg = NULL;
				msg = NULL;
				continue;
			}else{
				cl_debug("Attempting to read body, want %d bytes", msg->header.body_len);
				if (!msg->body) {
					msg->body = (uint8_t *)cf_malloc(msg->header.body_len);
				}
				int n_bytes_to_read = sizeof(memcache_header) + msg->header.body_len - msg->bytes_read;
				int n_bytes_read = read(fd, msg->body, n_bytes_to_read);
				if (n_bytes_read > 0) {
					msg->bytes_read += n_bytes_read;
					if (n_bytes_read < n_bytes_to_read) {
						// throw it back. Didn't read enough bytes
						cl_debug("Did not read full message body (%d v %d), awaiting next packet", n_bytes_read, n_bytes_to_read);
						break;
					}else{
						cl_debug("read body bytes %d", n_bytes_read);
						// de-network the body
						memcache_body_from_network(&msg->header, msg->body);
						memcache_set_batch(msg,state);
						cl_debug("message and body put on queue, %"PRIx64"", msg);
						memcache_queue_msg(msg, state);
						state->cur_msg = NULL;
						msg = NULL;
						continue;
					}
				}else if (n_bytes_read == 0) {
					cl_debug("no bytes to read - socket closed");
					close_socket(fd, state, false);
					break;
				}else{ // error...
					if (errno != EAGAIN && errno != EWOULDBLOCK){
						cl_info("error on read, %s", strerror(errno));
						close_socket(fd, state, false);
					}else{
						cl_debug("Awaiting more data, msg %"PRIx64"", msg);
					}
					break;
				}
			}
		}// end loop reading and processing data
		if (vlock) {
			pthread_mutex_unlock(vlock);
		}
	}

	if (epoll_events & EPOLLHUP) {
		close_socket(fd, NULL, false);
		cl_debug("received HUP from epoll, closing socket %d", fd);
	} else if (epoll_events & EPOLLERR) {
		close_socket(fd, NULL, false);
		cl_info("received error on socket, closing");
	}
}


static void
memcache_listener_cb(int fd, uint32_t epoll_events, void *udata)
{
	// the options we have here are really IN and ERR. I don't think HUP is possible...
	if (epoll_events & EPOLLIN) {
		// accept the connection.
		memcache_connector *connector = (memcache_connector *)udata;
        int new_fd;
        struct sockaddr_un un_addr;
        struct sockaddr_in in_addr;
        struct sockaddr *addr = ((connector->transportType == TRANSPORT_TCP_SOCKET) ? (struct sockaddr *)&in_addr : (struct sockaddr *)&un_addr);
        socklen_t clen = sizeof(*addr);

        if (-1 == (new_fd = accept(fd, addr, &clen))){
        	cl_warning("Error on memcache listener accept, ignoring");
        }
        cl_info("Accepted new connection on memcache listener");

        // create new fd info struct and put it in the hash
        fd_state state;
        memset(&state, 0, sizeof(state));
        state.fd_uid    = g_fd_uid++;
        if (g_strict_pipelining) {
        	state.fd_msg_queue = cf_queue_create(sizeof(memcache_msg *), true);
        	pthread_create(&state.fd_worker_thread, 0, memcache_worker, state.fd_msg_queue);

        }
        if (0 != shash_put(g_fd_state_hash, &new_fd, &state)){
        	cl_warning("Could not put new fd into hash table, aborting connection");
        	close(fd);
        }else{
        	// and now register with the reactor.
        	reactor_register(get_global_reactor(), new_fd, EPOLLIN, memcache_client_cb, udata);
        }
	}

	if (epoll_events & EPOLLERR) {
		// NB - this should never happen, and I really don't know what to do if
		// it does. Note and move on
		cl_warning("Error on memcache listener socket, ignoring");
	}
}

void
memcacheConnectorDestroy(struct cl_proxy_connector_s *c)
{
	memcache_connector *connector = (memcache_connector *)c;
	switch (connector->transportType) {
	case TRANSPORT_TCP_SOCKET:
		reactor_deregister(get_global_reactor(), connector->t.TCP_transport.sockfd);
		cf_free(connector->t.TCP_transport.hostname);
		cl_debug("closing tcp socket listener, %d", connector->t.TCP_transport.sockfd);
		close(connector->t.TCP_transport.sockfd);
		break;
	case TRANSPORT_UNIX_SOCKET:
		reactor_deregister(get_global_reactor(), connector->t.unix_socket_transport.sockfd);
		cf_free(connector->t.unix_socket_transport.name);
		close(connector->t.unix_socket_transport.sockfd);
		break;
	case TRANSPORT_UDP_SOCKET:
	case TRANSPORT_PIPE:
	default:
		// not supported yet...
		break;
	}

	cf_free(connector->namespace);
	if (connector->set)
		cf_free(connector->set);

	g_num_connectors--;
	if (g_num_connectors <= 0) {
		memcache_shutdown();
	}
}

static void *
couchbase_admin_thread(void *arg);

memcache_connector *
memcacheConnectorCreate(xmlNode *node, cl_cluster *cluster, int read_timeout, int write_timeout)
{
	memcache_init();
	g_num_connectors++;

	// set up connector, inherit timeouts and low level citrusleaf cluster
	memcache_connector *newConnector = (memcache_connector *)cf_malloc(sizeof(memcache_connector));
	memset(newConnector, 0, sizeof(memcache_connector));
	newConnector->base.connectorSize = sizeof(memcache_connector);
	newConnector->base.dtor = memcacheConnectorDestroy;
	newConnector->read_timeout  = read_timeout;
	newConnector->write_timeout = write_timeout;
	newConnector->cluster       = cluster;

	// pull out namespace and set associated with this connector
	xmlNode *namespaceNode = xmlNodeGetFirstChild(node, (xmlChar *)"namespace");
	if (!namespaceNode || !namespaceNode->children || !namespaceNode->children->type == XML_TEXT_NODE) {
		cl_warning("No namespace specified in memcache connector!");
		cf_free(newConnector);
		return NULL;
	}
	newConnector->namespace = cf_strdup((char *)namespaceNode->children->content);

	xmlNode *setNode = xmlNodeGetFirstChild(node, (xmlChar *)"set");
	if (setNode && setNode->children && setNode->children->type == XML_TEXT_NODE) {
		newConnector->set = cf_strdup((char *)setNode->children->content);
	}else{
		newConnector->set = NULL;
	}

	// And the version. If the version isn't set, we'll prepend to be a standard memcache version
	xmlNode *versionNode = xmlNodeGetFirstChild(node, (xmlChar *)"version");
	char *version;
	if (versionNode && versionNode->children && versionNode->children->type == XML_TEXT_NODE) {
		version = (char *)versionNode->children->content;
	} else {
		version = "1.4.13";
	}
	newConnector->version = cf_strdup(version);

	// Are we doing strict pipelining of not?
	xmlNode *asyncRequestsNode = xmlNodeGetFirstChild(node, (xmlChar *)"asynchronous_requests");
	if (asyncRequestsNode) {
		g_strict_pipelining = false;

		// get the number of worker threads, create them.  This parameter is optional
		xmlNode *workersNode = xmlNodeGetFirstChild(asyncRequestsNode, (xmlChar *)"num_workers");
		int num_workers = 4; // default number of workers per connector
		if (workersNode && workersNode->children && workersNode->children->type == XML_TEXT_NODE) {
			num_workers = atoi((char *)workersNode->children->content);
			if (num_workers < 0){
				num_workers = 4;
			}
		}
		memcache_add_workers(num_workers);
	}
	char *strict = (char *)xmlGetProp(node, (xmlChar *)"strict_pipelining");
	if (strict && (!strcasecmp(strict, "false"))) {
		g_strict_pipelining = false;
	}

	// Get transport type; set up transport
	xmlNode *transport = xmlNodeGetFirstChild(node, (xmlChar *)"transport");
	xmlNode *type = xmlNodeGetFirstChild(transport, (xmlChar *)"type");
	xmlChar *typeStr = type->children->content;
	if (!xmlStrcmp(typeStr, (xmlChar *)"TCP_socket")) { // TCP socket
		xmlNode *hostNode = xmlNodeGetFirstChild(transport, (xmlChar *)"host");
		// at this point, we can no longer count on DTD validation....
		xmlChar *hostAddr;
		if (!hostNode || !hostNode->children || hostNode->children->type != XML_TEXT_NODE) {
			hostAddr = (xmlChar *)"127.0.0.1";
		}else{
			hostAddr = hostNode->children->content;
		}
		xmlNode *portNode = xmlNodeGetFirstChild(transport, (xmlChar *)"port");
		if (!portNode || !portNode->children || portNode->children->type != XML_TEXT_NODE) {
			cl_warning("Connector specifies type TCP, but does not give port");
			goto ErrExit;
		}
		int port = atoi((char *)(portNode->children->content));
		if (port <= 0) {
			cl_warning("Connector has invalid port %s", portNode->children->content);
			goto ErrExit;
		}
		newConnector->transportType = TRANSPORT_TCP_SOCKET;
		cl_TCP_transport *transport = &newConnector->t.TCP_transport;
		transport->hostname = cf_strdup((char *)hostAddr);
		transport->port     = port;
		transport->sockfd   = socket(AF_INET, SOCK_STREAM, 0);

		int dummy = 1;
		setsockopt(transport->sockfd, SOL_SOCKET, SO_REUSEADDR, &dummy, sizeof(dummy));

		// bind the socket
		struct sockaddr_in serveraddr;
//		struct hostent *host_ent = gethostbyname((char *)hostAddr);

		memset(&serveraddr, 0, sizeof(serveraddr));
		serveraddr.sin_family = AF_INET;
		serveraddr.sin_port   = htons(port);
		serveraddr.sin_addr.s_addr = htonl(INADDR_ANY); //*(struct in_addr *)host_ent->h_addr_list[0];

		int rv = bind(transport->sockfd, (struct sockaddr *)&serveraddr, sizeof(struct sockaddr_in));
		if (rv < 0){
			cl_warning("Bind on tcp socket fails, error %s", strerror(errno));
			goto ErrExit;
		}

		// now register with the reactor...
		reactor_register(get_global_reactor(), transport->sockfd, EPOLLIN, memcache_listener_cb, newConnector);
		cl_debug("registered listener with reactor");

		rv = listen(transport->sockfd, 10);
		if (rv < 0){
			cl_warning("Listen on tcp socket fails, error %s", strerror(errno));
			goto ErrExit;
		}

	}else if (!xmlStrcmp(typeStr, (xmlChar *)"UNIX_socket")) {  // Unix socket
		xmlNode *nameNode = xmlNodeGetFirstChild(transport, (xmlChar *)"name");
		if (!nameNode || !nameNode->children || nameNode->children->type != XML_TEXT_NODE) {
			cl_warning("Connector specifies unix type socket, but does not give a name!");
			goto ErrExit;
		}
		newConnector->transportType = TRANSPORT_UNIX_SOCKET;
		cl_unix_socket_transport *transport = &newConnector->t.unix_socket_transport;
		transport->name = cf_strdup((char*) nameNode->children->content);
		transport->sockfd = socket(AF_UNIX, SOCK_STREAM, 0);

		struct sockaddr_un serveraddr;
		memset(&serveraddr, 0, sizeof(serveraddr));
		serveraddr.sun_family = AF_UNIX;
		unlink(transport->name);
		strcpy(serveraddr.sun_path, transport->name);

		int rv = bind(transport->sockfd, (struct sockaddr *)&serveraddr, sizeof(struct sockaddr_un));
		if (rv < 0){
			cl_warning("Bind on unix socket fails, %s", strerror(errno));
			goto ErrExit;
		}else{
			cl_info("Bind on unix socket succeeds, sockfd is %d", transport->sockfd);
		}
		rv = listen(transport->sockfd, 10);
		if (rv < 0) {
			cl_warning("Listen on unix socket fails, %s", strerror(errno));
			goto ErrExit;
		}

		// now register with the reactor...
		reactor_register(get_global_reactor(), transport->sockfd, EPOLLIN, memcache_listener_cb, newConnector);
	}

	// Check to see whether we're requesting couchbase smart client compatibility. If we are,
	// the couchbase_admin_port will be set.
	xmlNode *adminPortNode = xmlNodeGetFirstChild(node, (xmlChar*)"couchbaseAdminPort");
	if( adminPortNode ){
		int port = atoi((char *)(adminPortNode->children->content));
		if (port <= 0) {
			cl_warning("Connector has invalid admin port %s", adminPortNode->children->content);
			goto ErrExit;
		}
		newConnector->couchbase_admin_port = port;

		// set up the couchbase info listener
		pthread_t pthread_couchbase_admin;
		pthread_create(&pthread_couchbase_admin, 0, couchbase_admin_thread, newConnector);
	}

	// and I'm not going to bother with the named pipes right now.

	return newConnector;

ErrExit:
	if (newConnector) {
		if (newConnector->transportType == TRANSPORT_UNIX_SOCKET) {
			cl_info("close listener");
			close(newConnector->t.unix_socket_transport.sockfd);
			unlink(newConnector->t.unix_socket_transport.name);
		}else if (newConnector->transportType == TRANSPORT_TCP_SOCKET) {
			cl_warning("Error on connector creation - closing socket");
			close(newConnector->t.TCP_transport.sockfd);
		}
		cf_free(newConnector);
	}
	return NULL;
}


static int
is_valid_msg(memcache_header *msg)
{
	switch (msg->opcode) {
	case MC_op_get:
	case MC_op_getQ:
	case MC_op_getK:
	case MC_op_getKQ:
	case MC_op_delete:
	case MC_op_deleteQ:
		// check - no extras, no value, must have key
		if (msg->extra_len > 0 || msg->key_len == 0 || msg->body_len != msg->key_len) {
			return MC_INVALID_PARAM;
		}
		break;
	case MC_op_set:
	case MC_op_setQ:
	case MC_op_replace:
	case MC_op_replaceQ:
	case MC_op_add:
	case MC_op_addQ:
		// check - must have 8 bytes of extras, key, value
		if (msg->extra_len != 8 || msg->key_len == 0 || msg->body_len <= msg->key_len + msg->extra_len) {
			return MC_INVALID_PARAM;
		}
		break;
	case MC_op_increment:
	case MC_op_incrementQ:
	case MC_op_decrement:
	case MC_op_decrementQ:
		// check - must have 20 bytes of extras, key, no value
		if (msg->extra_len != 20 || msg->key_len == 0 || msg->body_len != msg->extra_len + msg->key_len) {
			return MC_INVALID_PARAM;
		}
		break;
	case MC_op_quit:
	case MC_op_quitQ:
	case MC_op_nop:
	case MC_op_version:
	case MC_op_stat:
	case CL_op_thr_shutdown:
		// check - no key, no value, no extras
		if (msg->extra_len != 0 || msg->key_len != 0 || msg->body_len != 0) {
			return MC_INVALID_PARAM;
		}
		break;
	case MC_op_flush:
	case MC_op_flushQ:
		// check - no key, no value
		if (msg->key_len != 0 || msg->body_len != msg->extra_len) {
			return MC_INVALID_PARAM;
		}
		break;
	case MC_op_append:
	case MC_op_appendQ:
	case MC_op_prepend:
	case MC_op_prependQ:
		// check - key, value, no extras
		if (msg->key_len == 0 || msg->extra_len != 0 || msg->body_len <= msg->key_len) {
			return MC_INVALID_PARAM;
		}
		break;
	case MC_op_verbosity:
		// check - no key, no value, 4 bytes of extras
		if (msg->key_len != 0 || msg->extra_len != 4 || msg->body_len != msg->key_len) {
			return MC_INVALID_PARAM;
		}
		break;
	case MC_op_touch:
	case MC_op_GAT:
	case MC_op_GATQ:
	case MC_op_GATK:
	case MC_op_GATKQ:
		// check - key, no value, 4 bytes of extras
		if (msg->key_len == 0 || msg->extra_len != 4 || msg->body_len != msg->key_len + msg->extra_len) {
			return MC_INVALID_PARAM;
		}
		break;

	default:
		return MC_UNKNOWN_COMMAND;
		break;
	}
	return MC_OK;
}

static const char *
mc_getErrStr(int mc_rv)
{
	const char *errStr = "";
	switch (mc_rv){
	case MC_KEY_NOT_FOUND:
		errStr = "Not found";
		break;
	case MC_KEY_EXISTS:
		errStr = "Data exists for key";
		break;
	case MC_VALUE_TOO_LARGE:
		errStr = "Too large";
		break;
	case MC_INVALID_PARAM:
		errStr = "Invalid arguments";
		break;
	case MC_NOT_STORED:
		errStr = "Not stored";
		break;
	case MC_NOT_NUMERIC:
		errStr = "Non-numeric server-side value for incr or decr";
		break;
	case MC_WRONG_SERVER:
		errStr = "Wrong server";
		break;
	case MC_AUTH_ERROR:
		errStr = "Auth failure";
		break;
	case MC_AUTH_CONTINUE:
		errStr = "Auth Continue";
		break;
	case MC_UNKNOWN_COMMAND:
		errStr = "Unknown command";
		break;
	case MC_OO_MEMORY:
		errStr = "Out of memory";
		break;
	case MC_UNSUPPORTED:
		errStr = "Unsupported";
		break;
	case MC_INTERNAL_ERROR:
		errStr = "Internal Error";
		break;
	case MC_BUSY:
		errStr = "Busy";
		break;
	case MC_TEMPORARY_FAIL:
		errStr = "Temporary Failure";
		break;
	case MC_UNKNOWN:
		errStr = "Unknown Error";
		break;
	case MC_TIMEOUT:
		errStr = "Timeout";
		break;
	}

	return errStr;
}

static memcache_header *
make_mc_return_msg(int msg_size, uint32_t opcode, uint64_t generation, uint32_t opaque)
{
	memcache_header *rv_mc_msg = (memcache_header *)cf_malloc(msg_size);
	memset(rv_mc_msg, 0, msg_size);
	rv_mc_msg->magic  = 0x81;
	rv_mc_msg->status = MC_OK;
	rv_mc_msg->CAS    = generation;
	rv_mc_msg->opaque = opaque;
	rv_mc_msg->opcode = opcode;

	return rv_mc_msg;
}

static memcache_header *
make_mc_err_msg(int mc_rv)
{
	int rv_msg_size = sizeof(memcache_header);
	memcache_header *rv_mc_msg;
	const char *msg_str = mc_getErrStr(mc_rv);
	rv_msg_size += strlen(msg_str);
	rv_mc_msg = (memcache_header *)cf_malloc(rv_msg_size);
	memset(rv_mc_msg, 0, rv_msg_size);
	rv_mc_msg->magic = 0x81;
	rv_mc_msg->status = mc_rv;
	rv_mc_msg->body_len = strlen(msg_str);
	strncpy((char *)rv_mc_msg + sizeof(memcache_header), msg_str, strlen(msg_str));

	return rv_mc_msg;
}

// Convert from a citrusleaf return value to a
// memcache return value.
static uint16_t
make_mc_rv(cl_rv rv)
{
	uint16_t mc_rv = MC_UNKNOWN;

	// check return code...
	switch (rv) {
	case CITRUSLEAF_OK:
		mc_rv = MC_OK;
		break;
	case CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT:
	case CITRUSLEAF_FAIL_TIMEOUT:
		mc_rv = MC_TIMEOUT;
		break;
	case CITRUSLEAF_FAIL_NOTFOUND:
		mc_rv = MC_KEY_NOT_FOUND;
		break;
	case CITRUSLEAF_FAIL_PARAMETER:
		mc_rv = MC_INVALID_PARAM;
		break;
	case CITRUSLEAF_FAIL_GENERATION:
	case CITRUSLEAF_FAIL_KEY_EXISTS:
	case CITRUSLEAF_FAIL_BIN_EXISTS:
		mc_rv = MC_KEY_EXISTS;
		break;
	case CITRUSLEAF_FAIL_CLIENT:
	case CITRUSLEAF_FAIL_UNKNOWN:
	case CITRUSLEAF_FAIL_ASYNCQ_FULL:
	case CITRUSLEAF_FAIL_CLUSTER_KEY_MISMATCH:
	case CITRUSLEAF_FAIL_PARTITION_OUT_OF_SPACE:
		mc_rv = MC_UNKNOWN;
		break;
	case CITRUSLEAF_FAIL_INCOMPATIBLE_TYPE:
		mc_rv = MC_NOT_NUMERIC;
		break;
	default:
		mc_rv = MC_UNKNOWN;
		break;
	}

	return mc_rv;
}

static int
send_response(int fd, uint32_t fd_uid, memcache_header *rv, bool take_fd_lock)
{
	int bytes_to_send = sizeof(memcache_header) + rv->body_len;
	int bytes_sent = 0;
	pthread_mutex_t *vlock = NULL;
	fd_state *state;
	int retVal = 0;

	// get lock on fd state
	cl_debug("Sending response - cmd is %d, opaque is %"PRIx32", status is %d", rv->opcode, rv->opaque, rv->status);
	if (take_fd_lock){
		if (SHASH_OK != shash_get_vlock(g_fd_state_hash, &fd, (void **)&state, &vlock)) {
			cl_info("Can't get lock on fd when sending - has fd been closed?");
			return -1;
		}

		// check this is really the fd that we think it is...
		if (state->fd_uid != fd_uid) {
			cl_info("fd uid does not match - socket probably closed and re-opened. Will not send data");
			if (vlock)
				pthread_mutex_unlock(vlock);
			return -1;
		}
	}

	// change to network byte order
	memcache_to_network(rv, ((uint8_t *)rv) + sizeof(memcache_header));

	while (bytes_sent < bytes_to_send){
		uint8_t *send_buf = (uint8_t *)rv;
		int bytes_sent_cur;
		bytes_sent_cur= send(fd, send_buf, bytes_to_send-bytes_sent, 0);
		if (bytes_sent_cur > 0){
			bytes_sent += bytes_sent_cur;
			send_buf   += bytes_sent_cur;
		}else if (bytes_sent_cur < 0){
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				usleep(1000);
				continue;
			}else {
				cl_warning("Failure sending message to app!");
				retVal = -1;
				break;
			}
		}else{
			cl_warning("socket confusion - no more bytes to send?");
			break;
		}
	}

	if (vlock)
		pthread_mutex_unlock(vlock);
	return retVal;
}

// call with lock held on b_state!!
static int
send_batch(int fd, uint32_t fd_uid, batch_state *b_state, bool take_fd_lock)
{
	int nSent = 0;
	if (b_state->responses) {
		batch_response_elem *batch_elem = (batch_response_elem *)cf_ll_get_head(b_state->responses);
		while (batch_elem){
			memcache_header *rv = batch_elem->response;
			if (rv != NULL){
				cl_debug("attempting to send batch response, opaque is %d", rv->opaque);
				if (0 != send_response(fd, fd_uid, rv, take_fd_lock)){
					cl_warning("error sending batch response, aborting");
					break;
				}
				nSent += 1;
			}else{
				cl_debug("batch elem NULL, skipping");
			}
			batch_elem = (batch_response_elem *)batch_elem->next;
		}
	}
	util_deleteList(b_state->responses);
	b_state->responses = NULL;
	return nSent;
}

static void
maybe_send_batch_response(memcache_msg *msg)
{
	fd_and_idx fd_idx;
	fd_idx.fd        = msg->fd;
	fd_idx.batch_idx = msg->batch_idx;
	batch_state *b_state;
	pthread_mutex_t *vlock = NULL;
	bool batch_sent = false;

	cl_debug("Getting batch info to decide whether to send batch responses for batch idx %d", fd_idx.batch_idx);
	if (0 == shash_get_vlock(g_batch_state_hash, &fd_idx, (void **)&b_state, &vlock)) {

		if (b_state->n_outstanding <= 0 && b_state->requests_complete) {
			// send them back. Send them all back.
			send_batch(msg->fd, msg->fd_uid, b_state, true);
			batch_sent = true;
		}else{
			cl_debug("Decided not to send batch responses - number outstanding is %d, complete is %s", b_state->n_outstanding, b_state->requests_complete?"true":"false");
		}
	}

	if (vlock) {
		cl_debug("Releasing batch hash after deciding whether to send responses");
		pthread_mutex_unlock(vlock);
	}

	if (batch_sent) {
		shash_delete(g_batch_state_hash, &fd_idx);
		cl_debug("deleted batch idx %d", msg->batch_idx);
	}
}


static void
store_batch_response(memcache_msg *msg, memcache_header *rv_mc_msg)
{
	fd_and_idx fd_idx;
	fd_idx.fd        = msg->fd;
	fd_idx.batch_idx = msg->batch_idx;
	batch_state *b_state;
	pthread_mutex_t *vlock = NULL;

	cl_debug("*** Attempting to store batch response for request id %"PRIx32", batch id %d", msg->header.opaque, msg->batch_idx);
	if (0 == shash_get_vlock(g_batch_state_hash, &fd_idx, (void **)&b_state, &vlock)) {
		batch_response_elem *batch_elem = (batch_response_elem *)cf_ll_get_at(b_state->responses, msg->idx);
		if (batch_elem) {
			batch_elem->response = rv_mc_msg;
			b_state->n_outstanding -= 1;
			cl_debug("new number of outstanding requests is %d", b_state->n_outstanding);
		}else{
			cl_warning("Could not find element id %d in batch list", msg->idx);
		}
		cl_debug("unlocking batch hash after storing response");
		pthread_mutex_unlock(vlock);
		maybe_send_batch_response(msg);
	}else{
		cl_warning("No matching batch response, will not process batch job");
		if (vlock) {
			cl_debug("unlocking batch mutex after storing response");
		}
	}
}

void
make_key_blob(cl_object *key, memcache_msg *msg)
{
	citrusleaf_object_init_blob(key, msg->body + msg->header.extra_len, msg->header.key_len);
}

static void
memcache_handle_get(memcache_msg *msg)
{
	cl_object key;
	uint32_t generation;
	int n_bins;
	cl_bin *bins = NULL;
	cl_rv rv;
	uint16_t mc_rv;
	memcache_connector *connector = msg->connector;
	int b_setKey = 0;
	if (msg->header.opcode == MC_op_getK || msg->header.opcode == MC_op_getKQ)
		b_setKey = 1;

	cl_debug("Handling memcache get");
	make_key_blob(&key, msg);
	rv = citrusleaf_get_all(connector->cluster, connector->namespace, connector->set, &key, &bins, &n_bins,
			connector->read_timeout, &generation);
	mc_rv = make_mc_rv(rv);
	

	if (n_bins == 0) {
		mc_rv = MC_KEY_NOT_FOUND;
	}

	int rv_msg_size = sizeof(memcache_header);
	memcache_header *rv_mc_msg = NULL;
	if (mc_rv == MC_OK) {
		char strbuf[64]; // way big enough
		uint8_t extra_size = 4; // 4 bytes of extras. Set according to the data type
		uint32_t value_size = bins[0].object.sz;
		uint16_t key_size = b_setKey ? msg->header.key_len : 0;

		uint8_t *value = (uint8_t *)bins[0].object.u.blob;
		// special case!! if the value is an int, we have to translate to ascii. Deep sigh.
		if( bins[0].object.type == CL_INT ){
			sprintf(strbuf, "%"PRId64, bins[0].object.u.i64);
			value = (uint8_t *)strbuf;
			value_size = strlen(strbuf);
		}
		rv_msg_size += extra_size;
		rv_msg_size += value_size;
		rv_msg_size += key_size;

		rv_mc_msg = (memcache_header *)cf_malloc(rv_msg_size);
		memset(rv_mc_msg, 0, rv_msg_size);
		rv_mc_msg->magic = 0x81;
		rv_mc_msg->status = mc_rv;
		rv_mc_msg->extra_len = extra_size;
		rv_mc_msg->key_len = b_setKey ? msg->header.key_len : 0;
		rv_mc_msg->data_type = 0;
		rv_mc_msg->opaque = msg->header.opaque;
		rv_mc_msg->opcode = msg->header.opcode;
		rv_mc_msg->CAS = generation;
		rv_mc_msg->body_len = rv_msg_size - sizeof(memcache_header);
		uint32_t  *rv_msg_extras_flags = (uint32_t *)((uint8_t *)rv_mc_msg + sizeof(memcache_header));
		uint8_t *rv_msg_key  = (uint8_t *)rv_msg_extras_flags + extra_size;
		uint8_t *rv_msg_data = rv_msg_key + key_size;
		*rv_msg_extras_flags = 0; //Initialization

		//set the flag byte in memcache header, which signifies the datatype of the value.
		if(n_bins > 0) {
			if(bins[0].object.type == CL_INT) {
				*rv_msg_extras_flags |= MC_EXFLAGS_LONG;
			}
			else if(bins[0].object.type == CL_STR) {
					// No flags for plain strings
					*rv_msg_extras_flags = 0x0;
			}
			else if(bins[0].object.type == CL_BLOB) {
				*rv_msg_extras_flags |= MC_EXFLAGS_BLOB;
			}
		}

		if (b_setKey) memcpy(rv_msg_key, msg->body + msg->header.extra_len, key_size);
		memcpy(rv_msg_data, value, value_size);
		citrusleaf_bins_free(bins, n_bins);
	}else{
		rv_mc_msg = make_mc_err_msg(mc_rv);
		rv_mc_msg->opaque = msg->header.opaque;
		rv_mc_msg->opcode = msg->header.opcode;
	}

	// now, depending on whether this is a get or a getQ, we send the thing back.
	if (msg->header.opcode == MC_op_get || msg->header.opcode == MC_op_getK){
		send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
		cf_free(rv_mc_msg);
	}else{ // op_getQ or op getKQ
		if (mc_rv == MC_OK) {
			store_batch_response(msg, rv_mc_msg);
		} else {
			store_batch_response(msg, NULL);  // tests claim error on GETQ doesn't return anything. So.
			cf_free(rv_mc_msg);
		}
	}

	if (bins) {
		cf_free(bins);
	}
}

static void
set_write_params(memcache_msg *msg, uint32_t ttl, cl_write_parameters *cl_w_p)
{
	cl_write_parameters_set_default(cl_w_p);
	cl_w_p->w_pol = CL_WRITE_ONESHOT;
	if (msg->header.CAS != 0){
		cl_w_p->generation     = msg->header.CAS;
		cl_w_p->use_generation = true;
	}
	cl_w_p->timeout_ms = msg->connector->write_timeout;
	cl_w_p->record_ttl = ttl;
	switch (msg->header.opcode) {
	case MC_op_add:
	case MC_op_addQ:
		cl_w_p->unique = true;
		break;
	default:
		break;
	}
}

// Sets, adds, and their friends all come in here

static void
memcache_handle_set(memcache_msg *msg)
{
	cl_object key;
	cl_operation op;
//	cl_bin bin;
	cl_rv rv;
	uint16_t mc_rv;
	uint32_t generation;
	memcache_connector *connector = msg->connector;


	// get 'extras' - in this case, just the expiration
	uint32_t expiration = *(uint32_t *)(msg->body + 4);

	// initialize key blob
	make_key_blob(&key, msg);

	// set up operator
	op.bin.bin_name[0] = 0;
	op.op = CL_OP_WRITE;

	// check - is this an integer? If so, it gets the type integer. Otherwise, it's a blob
	uint8_t *data_ptr = msg->body + msg->header.extra_len + msg->header.key_len;
	char *number_end_ptr;
	int data_len = msg->header.body_len - (msg->header.extra_len + msg->header.key_len);
	uint32_t extras_flags;
	int64_t int_value;
	// store the flag byte sent by memcache client which indicates the type of the data
	extras_flags = (*(uint32_t*)msg->body);

	// If no flag is set, it is plain string data
	if(extras_flags == 0x0)
	{
		citrusleaf_object_init_str2(&op.bin.object, data_ptr, data_len);
	}
	else if (extras_flags & MC_EXFLAGS_COMPRESSED) {
		// compressed is not a native type in citrusleaf.
		// We cannot remember & reconstruct this flag when we get back data
		// TODO: One idea is to store the flags as a value in a special bin
		// Note that both compressed & string/int/long can be set
		cl_warning("Cannot handle compressed data");
		rv = CITRUSLEAF_FAIL_PARAMETER;
		goto skip_operate;
	}
	else if((extras_flags & MC_EXFLAGS_INT) || (extras_flags & MC_EXFLAGS_LONG))
	{
		char str_value[22]; //Max long int can fit in 22 chars
		memcpy(str_value, data_ptr, data_len);
		str_value[data_len] = '\0';
		int_value=strtoll(str_value, &number_end_ptr, 10);
		citrusleaf_object_init_int(&op.bin.object, int_value);
	}
	else if (extras_flags & MC_EXFLAGS_BLOB) {
		citrusleaf_object_init_blob(&op.bin.object, data_ptr, data_len);
	} else {
		cl_warning("Unknown datatype");
		rv = CITRUSLEAF_FAIL_PARAMETER;
		goto skip_operate;
	}

	// set up write parameters
	cl_write_parameters cl_w_p;
	set_write_params(msg, expiration, &cl_w_p);


	// Do the put
	rv = citrusleaf_operate(connector->cluster, connector->namespace, connector->set, &key, &op, 1, &cl_w_p,
			msg->header.opcode == MC_op_replace || msg->header.opcode == MC_op_replaceQ,
			&generation);

/*
	// Do the put - XXX no generation returned.
	if (msg->header.opcode == MC_op_replace || msg->header.opcode == MC_op_replaceQ) {
		rv = citrusleaf_put_replace(connector->cluster, connector->namespace, connector->set, &key, &bin, 1, &cl_w_p);
	} else {
		rv = citrusleaf_put(connector->cluster, connector->namespace, connector->set, &key, &bin, 1, &cl_w_p);
	}
*/

skip_operate:
	// translate the return code
	mc_rv = make_mc_rv(rv);

	// create return message
	memcache_header *rv_mc_msg = NULL;
	if (mc_rv == MC_OK) {
		rv_mc_msg = (memcache_header *)cf_malloc(sizeof(memcache_header));
		memset(rv_mc_msg, 0, sizeof(memcache_header));
		rv_mc_msg->magic     = 0x81;
		rv_mc_msg->status    = mc_rv;
		rv_mc_msg->extra_len = 0;
		rv_mc_msg->key_len   = 0;
		rv_mc_msg->data_type = 0;
		rv_mc_msg->opaque    = msg->header.opaque;
		rv_mc_msg->opcode    = msg->header.opcode;
		rv_mc_msg->CAS       = generation;
		rv_mc_msg->body_len  = 0;
		// to queue or not to queue...
		if (msg->header.opcode == MC_op_set || msg->header.opcode == MC_op_add || msg->header.opcode == MC_op_replace) {
			send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
			cf_free(rv_mc_msg);
		}else{
			store_batch_response(msg, NULL); // no response on good...
			cf_free(rv_mc_msg);
		}
	}else{
		rv_mc_msg = make_mc_err_msg(mc_rv);
		rv_mc_msg->opaque = msg->header.opaque;
		rv_mc_msg->opcode = msg->header.opcode;
		if (msg->header.opcode == MC_op_setQ /*|| msg->header.opcode == MC_op_addQ || msg->header.opcode == MC_op_replaceQ*/) {
			store_batch_response(msg, rv_mc_msg);
		}else{
			send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
			cf_free(rv_mc_msg);
		}
	}
}


// appends, prepends, and their friends all come in here
static void
memcache_handle_append_prepend(memcache_msg *msg)
{
	cl_object key;
	cl_rv rv;
	uint16_t mc_rv;
	uint32_t generation = 1;

	memcache_connector *connector = msg->connector;

	cl_debug("Handling memcache append/prepend, msg opcode is %d", msg->header.opcode);

	// initialize key blob
	make_key_blob(&key, msg);

	// set up operator to append/prepend
	cl_operation op;
	op.bin.bin_name[0] = 0;
	op.op = (msg->header.opcode == MC_op_prepend || msg->header.opaque == MC_op_prependQ) ? CL_OP_MC_PREPEND : CL_OP_MC_APPEND;

	uint8_t *data_ptr = msg->body + msg->header.extra_len + msg->header.key_len;
	int data_len = msg->header.body_len - (msg->header.extra_len + msg->header.key_len);
	citrusleaf_object_init_str2(&op.bin.object, data_ptr, data_len);

	// set up write parameters
	cl_write_parameters cl_w_p;
	set_write_params(msg, 0, &cl_w_p);

	// Do the put
	rv = citrusleaf_operate(connector->cluster, connector->namespace, connector->set, &key, &op, 1, &cl_w_p, true, &generation);

	// translate the return code
	mc_rv = make_mc_rv(rv);

	// create return message
	memcache_header *rv_mc_msg = NULL;
	if (mc_rv == MC_OK) {
		rv_mc_msg = make_mc_return_msg(sizeof(memcache_header),
				msg->header.opcode, generation, msg->header.opaque);

		// to queue or not to queue...
		if (msg->header.opcode == MC_op_append || msg->header.opcode == MC_op_prepend) {
			send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
			cf_free(rv_mc_msg);
		}else{
			cl_debug("store batch response good on append/prepend, id %d", msg->header.opaque);
			store_batch_response(msg, NULL); // no response on good...
			cf_free(rv_mc_msg);
		}
	}else{
		// some stupidity here. The memcache server returns 'not stored' for this command
		// when it can't find the key, rather than 'key doesn't exist'. Sigh.
		if (mc_rv == MC_KEY_NOT_FOUND) {
			mc_rv = MC_NOT_STORED;
		}
		rv_mc_msg = make_mc_err_msg(mc_rv);
		rv_mc_msg->opaque = msg->header.opaque;
		rv_mc_msg->opcode = msg->header.opcode;
		if (msg->header.opcode == MC_op_appendQ || msg->header.opcode == MC_op_prependQ) {
			store_batch_response(msg, NULL);
		}
		//else{
			send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
			cf_free(rv_mc_msg);
	//	}
	}
}


static void
memcache_handle_delete(memcache_msg *msg)
{
	cl_object key;
	cl_rv rv;
	uint16_t mc_rv;
	memcache_header *rv_mc_msg;
	memcache_connector *connector = msg->connector;

	cl_debug("memcache handle delete");

	// initialize key blob
	make_key_blob(&key, msg);

	cl_write_parameters cl_w_p;
	set_write_params(msg, 0, &cl_w_p);

	// Do the delete
	rv = citrusleaf_delete(connector->cluster, connector->namespace, connector->set, &key, &cl_w_p);

	// translate the return code
	mc_rv = make_mc_rv(rv);

	if (mc_rv == MC_OK) {
		cl_debug("Successful delete");
		rv_mc_msg = (memcache_header *)cf_malloc(sizeof(memcache_header));
		memset(rv_mc_msg, 0, sizeof(memcache_header));
		rv_mc_msg->magic     = 0x81;
		rv_mc_msg->status    = mc_rv;
		rv_mc_msg->opaque    = msg->header.opaque;
		rv_mc_msg->opcode    = msg->header.opcode;
		if (msg->header.opcode == MC_op_deleteQ) {
			store_batch_response(msg, NULL); // no response on success
		}else{
			send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
		}
		cf_free(rv_mc_msg);
	}else{
		cl_debug("Unsuccessful delete, error %d", mc_rv);
		rv_mc_msg = make_mc_err_msg(mc_rv);
		rv_mc_msg->opcode = msg->header.opcode;
		rv_mc_msg->opaque = msg->header.opaque;
		if (msg->header.opcode == MC_op_deleteQ) {
			store_batch_response(msg, NULL); // according to the memcache test, we're expected to return an error immediately on delete. This seems wrong, but ...
		}
		send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
		cf_free(rv_mc_msg);
	}
}

// NB -
// The memcache protocol uses uint64's for its integers; Citrusleaf
// uses int64's. Citrusleaf also allows decrement to decrement below
// 0, whereas memcache explicitly does not.
// Citrusleaf also does not bother to return a friendly error code
// if the key is non-numeric - I think it just fails silently (sigh.)
// An Citrusleaf always creates the bin if it does not exist. (also sigh.)
// C'est la vie.
static void
memcache_handle_incr(memcache_msg *msg)
{
	cl_object key;
	cl_rv rv;
	uint16_t mc_rv;
	uint64_t ret_val = 0;

	memcache_connector *connector = msg->connector;

	cl_debug("Handling memcache increment message");

	// initialize key blob
	make_key_blob(&key, msg);

	// pull parameters out of 'extras'
	uint64_t delta       = *(uint64_t *)(msg->body);
	uint64_t initial_val = *(uint64_t *)(msg->body + 8);
	uint32_t expiration  = *(uint32_t *)(msg->body + 16);
	uint32_t generation  = 1;

	cl_debug("Increment - delta is %"PRIx64", initial value is %"PRIx64", expiration is %"PRIx32"", delta, initial_val, expiration);

	// set up write parameters
	cl_write_parameters cl_w_p;
	set_write_params(msg, expiration, &cl_w_p); // XXX is expiration -1 considered a no-op or not?? check this

	// Does this have an initial set, or is it an increment/decrement of
	// an existing value?
	bool b_replace = false;
	if (expiration == 0xffffffff){
		b_replace  = true;
		expiration = 0; // use default
	}

	cl_operation ops[2];
	ops[0].bin.bin_name[0] = 0;
	ops[0].op = CL_OP_MC_INCR;

	ops[1].bin.bin_name[0] = 0;
	ops[1].op = CL_OP_READ;

	if (msg->header.opcode == MC_op_decrement || msg->header.opcode == MC_op_decrementQ){
		delta = -delta;
	}

	uint64_t packed_values[2];
	packed_values[0] = delta;
	packed_values[1] = initial_val;

	citrusleaf_object_init_blob(&ops[0].bin.object, packed_values, sizeof(packed_values));
	citrusleaf_object_init_int(&ops[1].bin.object, delta);
	rv = citrusleaf_operate(connector->cluster, connector->namespace, connector->set, &key, ops, 2, &cl_w_p, b_replace, &generation);

	mc_rv = make_mc_rv(rv);

	ret_val = (uint64_t)ops[0].bin.object.u.i64; // NB - another difference. i64 v u64.
	citrusleaf_bins_free(&ops[0].bin, 1);
	citrusleaf_bins_free(&ops[1].bin, 1);

	if (mc_rv != MC_OK)
		goto SendResult;


SendResult: ;
	// and now set up the final result
	memcache_header *rv_mc_msg = NULL;
	if (mc_rv == MC_OK) {
		rv_mc_msg = (memcache_header *)cf_malloc(sizeof(memcache_header) + 8);
		rv_mc_msg->magic     = 0x81;
		rv_mc_msg->status    = mc_rv;
		rv_mc_msg->extra_len = 0;
		rv_mc_msg->key_len   = 0;
		rv_mc_msg->data_type = 0;
		rv_mc_msg->opaque    = msg->header.opaque;
		rv_mc_msg->opcode    = msg->header.opcode;
		rv_mc_msg->CAS       = generation;
		rv_mc_msg->body_len  = 8;
		*(uint64_t *)((uint8_t *)rv_mc_msg + sizeof(memcache_header)) = ret_val;

		// to queue or not to queue...
		if (msg->header.opcode == MC_op_increment || msg->header.opcode == MC_op_decrement) {
			send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
			cf_free(rv_mc_msg);
		}else{
			store_batch_response(msg, NULL);
			cf_free(rv_mc_msg);
		}
	}else{
		rv_mc_msg = make_mc_err_msg(mc_rv);
		rv_mc_msg->opaque = msg->header.opaque;
		rv_mc_msg->opcode = msg->header.opcode;
		if (msg->header.opcode == MC_op_incrementQ || msg->header.opcode == MC_op_decrementQ) {
			store_batch_response(msg, rv_mc_msg);
		}else{
			send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
			cf_free(rv_mc_msg);
		}
	}
}

static void
memcache_handle_thr_shutdown(memcache_msg *msg, cf_queue *queue)
{
	// this is only applicable in strict pipelining mode.
	// Delete the queue and shut down the worker.
	cl_assert(g_strict_pipelining, "Thread shutdown called but not strict pipelining mode!");

	cl_debug("Thread shutdown called");

	bool join = msg->fd_uid;

	cf_free(msg);

	cf_queue_destroy(queue);
	if (!join)
		pthread_detach(pthread_self());

	pthread_exit(NULL);
}

static void
memcache_handle_quit(memcache_msg *msg)
{
	// lock down fd ...
	fd_state *state;
	pthread_mutex_t *vlock;
	if (0 != shash_get_vlock(g_fd_state_hash, &msg->fd, (void **)&state, &vlock)) {
		cl_info("Internal error - no existing batch structure for request");
		return;
	}

	if (state->fd_uid != msg->fd_uid) {
		cl_info("Handle quit: fd appears to have already been closed");
		if (vlock)
			pthread_mutex_unlock(vlock);
		if (g_strict_pipelining) {
			cl_warning("Internal error - wrong fd receives a message!\n");
			pthread_exit(0);
		}
		return;
	}

	// Copy off what we need, and invalidate the state so no one else can use this fd.
	fd_state local_state;
	memcpy(&local_state, state, sizeof(fd_state));
	state->shutdown = true;
	pthread_mutex_unlock(vlock);

	// Now send the response
	if (msg->header.opcode == MC_op_quit) { // non-quiet version

		// flush out any waiting batch operations
		if (local_state.in_batch) {
			fd_and_idx batch_id;
			batch_id.batch_idx = local_state.batch_idx;
			batch_id.fd        = msg->fd;

			batch_state *b_state;
			if (0 == shash_get_vlock(g_batch_state_hash, &batch_id, (void **)&b_state, &vlock))
				send_batch(msg->fd, msg->fd_uid, b_state, false);
			if (vlock)
				pthread_mutex_lock(vlock);
		}

		// send response to the 'quit' command
		memcache_header hdr;
		memset(&hdr, 0, sizeof(hdr));
		hdr.opcode = msg->header.opcode;
		hdr.opaque = msg->header.opaque;
		hdr.magic  = 0x81;
		hdr.status = MC_OK;

		send_response(msg->fd, msg->fd_uid, &hdr, false);
	}

	// free 'quit' message
	int fd = msg->fd;
	cf_free(msg);

	// close fd
	close_socket(fd, NULL, false);

}

static void
memcache_handle_version(memcache_msg *msg)
{
	// Get version...
	int version_len = strlen(msg->connector->version) + 1;

	// and now set up the final result
	memcache_header *rv_mc_msg = make_mc_return_msg(sizeof(memcache_header) + version_len,
			                          msg->header.opcode, msg->header.CAS, msg->header.opaque);
	rv_mc_msg->body_len = version_len;
	rv_mc_msg->status   = MC_OK;
	strcpy((char *)rv_mc_msg + sizeof(memcache_header), msg->connector->version);

	send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
	cf_free(rv_mc_msg);
}


/*
static void
memcache_handle_flush(memcache_msg *msg)
{

}
*/


static void
memcache_handle_nop(memcache_msg *msg)
{
	cl_debug("handling memcache no-op");
	memcache_header *rv_mc_msg = (memcache_header *)cf_malloc(sizeof(memcache_header));
	memset(rv_mc_msg, 0, sizeof(memcache_header));
	rv_mc_msg->magic     = 0x81;
	rv_mc_msg->status    = MC_OK;
	rv_mc_msg->opcode    = msg->header.opcode;
	rv_mc_msg->opaque    = msg->header.opaque;
	send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
	cf_free(rv_mc_msg);
}

/*
static void
memcache_handle_version(memcache_msg *msg)
{

}

static void
memcache_handle_stat(memcache_msg *msg)
{

}
*/

static void
memcache_handle_touch(memcache_msg *msg)
{
	cl_object key;
	cl_rv rv;
	cl_operation ops[2];
	uint16_t mc_rv;
	int nOps;
	memcache_connector *connector = msg->connector;
	uint32_t generation;

	cl_debug("** Handling touch command, op is %d\n", msg->header.opcode);

	// get 'extras' - in this case, just the expiration
	uint32_t expiration = *(uint32_t *)(msg->body);

	// initialize key blob
	make_key_blob(&key, msg);

	// set up write parameters
	cl_write_parameters cl_w_p;
	set_write_params(msg, expiration, &cl_w_p);

	// set up the operation. Just a 'touch' is a write
	// with no bins. A 'get and touch' is a write with no bins,
	// followed by a read.
	citrusleaf_object_init_null(&ops[0].bin.object);
	citrusleaf_object_init_null(&ops[1].bin.object);
	ops[0].op = CL_OP_READ;
	ops[0].bin.bin_name[0] = '\0';
	ops[1].op = CL_OP_MC_TOUCH;
	ops[1].bin.bin_name[0]= '\0';

	MC_op mc_op = msg->header.opcode;
	if (mc_op == MC_op_GAT || mc_op == MC_op_GATQ || mc_op == MC_op_GATK || mc_op == MC_op_GATKQ){
		nOps = 2;
	}else{
		nOps = 1;
	}

	// do the operation
	rv = citrusleaf_operate(connector->cluster, connector->namespace, connector->set, &key, nOps==0?NULL:ops, nOps, &cl_w_p, false, &generation);

	// translate the return code
	mc_rv = make_mc_rv(rv);

	// create return message
	memcache_header *rv_mc_msg = NULL;
	if (mc_rv == MC_OK) {
		uint8_t  extra_size = 4; // 4 bytes of extras, not used "0xdeadbeef');
		uint32_t key_size   = ( (mc_op == MC_op_GATK || mc_op == MC_op_GATKQ)? msg->header.key_len : 0);
		uint32_t value_size = ops[0].bin.object.sz; // XXX - won't work with getall
		rv_mc_msg = (memcache_header *)cf_malloc(sizeof(memcache_header)+ extra_size + key_size + value_size);
		rv_mc_msg->magic     = 0x81;
		rv_mc_msg->status    = mc_rv;
		rv_mc_msg->extra_len = extra_size;
		rv_mc_msg->key_len   = key_size;
		rv_mc_msg->data_type = 0;
		rv_mc_msg->opaque    = msg->header.opaque;
		rv_mc_msg->opcode    = msg->header.opcode;
		rv_mc_msg->CAS       = generation;
		rv_mc_msg->body_len  = extra_size + key_size + value_size;
		*(uint32_t*)((uint8_t *)rv_mc_msg + sizeof(memcache_header)) = 0xdeadbeef;
		if (key_size > 0){
			uint8_t *keyPtr = ((uint8_t *)&msg->header) + sizeof(memcache_header) + msg->header.extra_len;
			memcpy((uint8_t *)rv_mc_msg + sizeof(memcache_header) + extra_size, keyPtr, key_size);
		}
		memcpy((uint8_t *)rv_mc_msg + sizeof(memcache_header) + extra_size + key_size, ops[0].bin.object.u.blob, value_size);
		// to queue or not to queue...
		if (msg->header.opcode == MC_op_touch || msg->header.opcode == MC_op_GAT || msg->header.opcode == MC_op_GATK) {
			send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
			cf_free(rv_mc_msg);
		}else{ // GATQ
			store_batch_response(msg, rv_mc_msg);
		}
	}else{
		rv_mc_msg = make_mc_err_msg(mc_rv);
		rv_mc_msg->opaque = msg->header.opaque;
		rv_mc_msg->opcode = msg->header.opcode;
		if (mc_op == MC_op_GATQ || mc_op == MC_op_GATKQ) {
			store_batch_response(msg, rv_mc_msg);
		}else{
			send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
			cf_free(rv_mc_msg);
		}
	}
	citrusleaf_bins_free(&ops[0].bin, 1);
	citrusleaf_bins_free(&ops[1].bin, 1);
}

static char* 
memcache_msg_toString(memcache_msg *msg) {
	static char buff[256];
	char *s;

	buff[0] = '\0';
	if (msg) {
		snprintf(buff, 255, "fd=%d, fd_uid=%d, idx=%d, batch_idx, bytes_read=%d, opcode=%d opaque=0x%x",
			 msg->fd, msg->fd_uid, msg->batch_idx, msg->bytes_read, msg->header.opcode, msg->header.opaque);
		s = buff;
	} else {
		s = "[empty]";
	}
	return s;
}

static void
memcache_handle_err(memcache_msg *msg, int errCode)
{
	cl_info("Error on message %"PRIx32" (%s), error code %d", msg->header.opaque, memcache_msg_toString(msg), errCode);
	// cons up error message, set status code, do
	// the batch dance...
	memcache_header *rv_mc_msg = make_mc_err_msg(errCode);

	bool batch = false;
	if (errCode != MC_UNKNOWN) {
		batch = is_batch_op(msg->header.opcode);
	}

	rv_mc_msg->opcode = msg->header.opcode;
	rv_mc_msg->opaque = msg->header.opaque;
	if (batch) {
		store_batch_response(msg, rv_mc_msg);
	}else{
		send_response(msg->fd, msg->fd_uid, rv_mc_msg, true);
		cf_free(rv_mc_msg);
	}
}

static bool
terminates_batch(int opcode)
{
	switch (opcode){
	case MC_op_get:
	case MC_op_set:
	case MC_op_increment:
	case MC_op_decrement:
	case MC_op_append:
	case MC_op_prepend:
	case MC_op_add:
	case MC_op_replace:
	case MC_op_delete:
	case MC_op_flush:
	case MC_op_getK:
//	case MC_op_quit:  // well, yes, but it does it in its own sweet way
	case MC_op_nop:
		return true;
		break;
	default:
		return false;
	}
}

static
void *memcache_worker(void *arg)
{
	cf_queue *requests_queue = g_strict_pipelining ? (cf_queue *)arg : g_requests_queue;
	while (g_running){
		memcache_msg *msg;

		if (0 != cf_queue_pop(requests_queue, &msg, 1000))
			continue;

		cl_debug("worker thread: popped msg %"PRIx64" off of the requests queue!", msg);

		int ret;
		if (MC_OK != (ret = is_valid_msg(&msg->header))) {
			cl_info("Received invalid message!");
			memcache_handle_err(msg, ret);
			put_mc_msg(msg);
			continue;
		}

		// spit out the batch responses, if we're ready
		if (terminates_batch(msg->header.opcode)) {
			maybe_send_batch_response(msg);
		}

		bool msg_freed=false;
		// what we do here depends on the request we've been given
		switch (msg->header.opcode) {
		case MC_op_get:
		case MC_op_getQ:
		case MC_op_getK:
		case MC_op_getKQ:
			memcache_handle_get(msg);
			break;
		case MC_op_set:
		case MC_op_setQ:
		case MC_op_add:
		case MC_op_addQ:
		case MC_op_replace:
		case MC_op_replaceQ:
			memcache_handle_set(msg);
			break;
		case MC_op_increment:
		case MC_op_decrement:
		case MC_op_incrementQ:
		case MC_op_decrementQ:
			memcache_handle_incr(msg);
			break;
		case MC_op_append:
		case MC_op_appendQ:
		case MC_op_prepend:
		case MC_op_prependQ:
			memcache_handle_append_prepend(msg);
			break;
		case MC_op_delete:
		case MC_op_deleteQ:
			memcache_handle_delete(msg);
			break;
		case MC_op_nop:
			memcache_handle_nop(msg);
			break;
		case MC_op_flush:
		case MC_op_flushQ:   // NB so sad!! no flush!!
			memcache_handle_err(msg, MC_UNSUPPORTED);
			break;
		case MC_op_quit:
		case MC_op_quitQ:
			memcache_handle_quit(msg);
			msg_freed=true;
			break;
		case CL_op_thr_shutdown:
			memcache_handle_thr_shutdown(msg, requests_queue);
			msg_freed=true;
			break;
		case MC_op_stat:
			memcache_handle_err(msg, MC_UNSUPPORTED);
			break;
		case MC_op_version:
			memcache_handle_version(msg);
			break;
		case MC_op_touch:
		case MC_op_GAT:
		case MC_op_GATQ:
		case MC_op_GATK:
		case MC_op_GATKQ:
			memcache_handle_touch(msg);
			break;
		default:
			memcache_handle_err(msg, MC_UNKNOWN_COMMAND);
			break;
		}

		if(!msg_freed)put_mc_msg(msg);
	}

	return NULL;
}

static uint32_t
fd_hash_fn(void *fd)
{
	return cf_hash_fnv(fd, sizeof(int));
}

static uint32_t
fd_plus_idx_hash_fn(void *fd_plus)
{
	return cf_hash_fnv(fd_plus, sizeof(fd_and_idx));
}

typedef struct{
	cf_ll_element *next;
	cf_ll_element *prev;
	pthread_t worker;
}worker_elem;

// Set up a few global structures that everyone loves...
void
memcache_init()
{
	// This only happens once...
	if (g_memcache_init_done)
		return;

	// Create the free queue for incoming messages - avoid malloc'ing message headers and metadata
	g_free_msg_queue = cf_queue_create(sizeof(memcache_msg *), true);

	// Create a queue of requests that will be parcelled out to the worker threads.
	g_requests_queue    = cf_queue_create(sizeof(memcache_msg *), true);

	// Create a hash (per fd) of fd state, which includes whether we're in batch mode,
	// the current batch index, and the current (partially finished) incoming message buffer
	shash_create(&g_fd_state_hash, fd_hash_fn, sizeof(int), sizeof(fd_state), 100, SHASH_CR_MT_MANYLOCK);

	// Create a hash (per fd+batch idx) of the batch request state for a particular
	// batch operation.
	shash_create(&g_batch_state_hash, fd_plus_idx_hash_fn, sizeof(fd_and_idx), sizeof(batch_state), 100, SHASH_CR_MT_MANYLOCK);

	// create linked list for workers. (actual workers get added later)
	g_workers_list = (cf_ll *)cf_malloc(sizeof(cf_ll));
	cf_ll_init(g_workers_list, util_genericLlElementDtor, false);

	g_running = true;
	g_memcache_init_done = true;
}

typedef struct {
	cf_ll_element *next;
	cf_ll_element *prev;
	pthread_t      thread;
}pthread_ll_elem;

static int
reduce_stop_input(void *key, void *data, void *udata)
{
	fd_and_idx *fd_n_idx = (fd_and_idx *)key;

	reactor_deregister(get_global_reactor(), fd_n_idx->fd);

	return 0;
}

static int
reduce_delete_fd_state(void *key, void *data, void *udata)
{
	return SHASH_REDUCE_DELETE;
}

static int
reduce_close_connections(void *key, void *data, void *udata)
{
	int fd = *(int *)key;

	cl_info("closing connection %d", fd);

	// and we also want to free any blocks hanging around...
	fd_state *state = (fd_state *)data;

	// If strict pipelining, get a list of those threads so we can join them
	if (g_strict_pipelining && udata) {
		pthread_ll_elem *pthread_elem = (pthread_ll_elem *)cf_malloc(sizeof(pthread_ll_elem));
		memset(pthread_elem, 0, sizeof(pthread_ll_elem));
		pthread_elem->thread = state->fd_worker_thread;
		cf_ll_append((cf_ll *)udata, (cf_ll_element *)pthread_elem);
	}

	// close the socket and clean up associated state.
	// This will also send a shutdown message to the associated thread in
	// strict pipelining mode
	state->shutdown = true;
	close_socket(fd, state, true);

	return 0;
}

static void
memcache_shutdown()
{
	g_running = false;

	// stop input - remove fd's from reactor.  Listeners should already be shut down.
	shash_reduce(g_fd_state_hash, reduce_stop_input, NULL);

	// Allow global workers to gracefully exit
	worker_elem *worker = (worker_elem *)cf_ll_get_head(g_workers_list);
	while (worker) {
		cl_debug("killing worker 0x%"PRIx64"", worker->worker);
		pthread_join(worker->worker, NULL);
		pthread_detach(worker->worker);
		worker = (worker_elem *)worker->next;
	}
	util_deleteList(g_workers_list);

	// close down open connections, and pthread_join any workers associated with
	// particular fds. (Closing the connection also cleans up any state
	// associated with the open fd)
	cf_ll *pthread_list = (cf_ll *)cf_malloc(sizeof(cf_ll));
	cf_ll_init(pthread_list, util_genericLlElementDtor, false);
	shash_reduce(g_fd_state_hash, reduce_close_connections, pthread_list);
	pthread_ll_elem *pthread_elem = (pthread_ll_elem *)cf_ll_get_head(pthread_list);
	while (pthread_elem) {
		if (pthread_elem->thread) {
			pthread_join(pthread_elem->thread, NULL);
			pthread_detach(pthread_elem->thread);
		}
		pthread_elem = (pthread_ll_elem *)pthread_elem->next;
	}
	util_deleteList(pthread_list);

	// delete anything left in the state hash.
	shash_reduce_delete(g_fd_state_hash, reduce_delete_fd_state, NULL);

	// destroy the hashes
	shash_destroy(g_fd_state_hash);
	shash_destroy(g_batch_state_hash);

	// and delete the queues
	memcache_msg *msg;
	int rv = cf_queue_pop(g_free_msg_queue, &msg, 0);
	while (0 == rv) {
		cf_free(msg);
		rv = cf_queue_pop(g_free_msg_queue, &msg, 0);
	}
	cf_queue_destroy(g_free_msg_queue);

	if (!g_strict_pipelining) {
		rv = cf_queue_pop(g_requests_queue, &msg, 0);
		while (0 == rv) {
			cf_free(msg);
			rv = cf_queue_pop(g_requests_queue, &msg, 0);
		}
		cf_queue_destroy(g_requests_queue);
	}
}

// Add some number of worker threads
void
memcache_add_workers(int n_workers)
{
	for (int i=0; i<n_workers; i++){
		worker_elem *worker = (worker_elem *)cf_malloc(sizeof(worker_elem));
		memset(worker, 0, sizeof(worker_elem));
		pthread_create(&worker->worker, 0, memcache_worker, NULL);
		cl_debug("adding worker %"PRIx64" to list", worker->worker);
		cf_ll_append(g_workers_list, (cf_ll_element *)worker);
	}
}

static char validServerInfoRequest1[] = "GET /pools/default/buckets/";
static char validServerInfoRequest2[] = "GET /pools/default/bucketsStreaming/";


static char invalidServerInfoResponse[] = "\r\n\r\nHTTP/1.1 401 Bad Request\r\n\r\n";

// in place substitution of substring. We assume the allocated buffer is big enough...
static void
subStrReplace(char *haystack, char *needle, char *newNeedle)
{
	char *p_needle;
	size_t needle_len = strlen(needle);
	size_t new_needle_len = strlen(newNeedle);
	char *p_end = haystack + strlen(haystack);
	char *p_cur = haystack;
	while (0 != (p_needle = strstr(p_cur, needle)) ){
		// move remaining data to make room for new needle
		char *p_needle_end = p_needle + needle_len;
		char *p_new_needle_end = p_needle + new_needle_len;
		memmove(p_new_needle_end, p_needle_end, p_end - p_needle_end + 1);  // keep the null termination
		// copy in new needle
		memcpy(p_needle, newNeedle, new_needle_len);
		// move along...
		p_cur =  p_needle + new_needle_len;
	}
}

#define SERVER_INFO_RESPONSE_BUF_SIZE 16*1024
static char serverInfoResponse[SERVER_INFO_RESPONSE_BUF_SIZE];
static char validServerInfoResponseHeader[] = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n";
static char validServerInfoResponseBody[] =	"{\"name\":\"default\",\"bucketType\":\"membase\",\"authType\":\"sasl\",\"saslPassword\":\"\",\
\"proxyPort\":0,\"uri\":\"/pools/default/buckets/default\",\
\"streamingUri\":\"/pools/default/bucketsStreaming/default\",\
\"flushCacheUri\":\"/pools/default/buckets/default/controller/doFlush\",\
\"nodes\":[{\"systemStats\":{\"cpu_utilization_rate\":7.0,\
\"swap_total\":4194295808.0,\"swap_used\":118784},\
\"interestingStats\":{\"curr_items\":96877,\"curr_items_tot\":96877,\
\"vb_replica_curr_items\":0},\"uptime\":\"51\",\
\"memoryTotal\":2108219392,\"memoryFree\":110518272,\
\"mcdMemoryReserved\":1608,\"mcdMemoryAllocated\":1608,\
\"replication\":0.0,\"clusterMembership\":\"active\",\"status\":\"healthy\",\
\"hostname\":\"127.0.0.1:XXX_ADMIN_PORT\",\"clusterCompatibility\":1,\
\"version\":\"1.8.0r-55-g80f24f2-enterprise\",\"os\":\"x86_64-unknown-linux-gnu\",\
\"ports\":{\"proxy\":11211,\"direct\":XXX_DATA_PORT}}],\
\"stats\":{\"uri\":\"/pools/default/buckets/default/stats\",\
\"directoryURI\":\"/pools/default/buckets/default/statsDirectory\",\
\"nodeStatsListURI\":\"/pools/default/buckets/default/nodes\"},\
\"nodeLocator\":\"vbucket\",\
\"vBucketServerMap\":{\"hashAlgorithm\":\"CRC\",\"numReplicas\":1,\
\"serverList\":[\"127.0.0.1:XXX_DATA_PORT\"],\
\"vBucketMap\":[[0,-1],[0,-1],[0,-1],[0,-1]]},\
\"replicaNumber\":1,\"quota\":{\"ram\":1686110208,\"rawRAM\":1686110208},\
\"basicStats\":{\"quotaPercentUsed\":8.33289439405375,\"opsPerSec\":0,\
\"diskFetches\":0,\"itemCount\":96877,\"diskUsed\":116420928,\"memUsed\":140501783}}\n\n\n\n";

static char validServerInfoResponseFooter[] = "\r\n";

static int
parse_vbucket_request(const char *data,  int data_len)
{
	// The world's cheapest HTTP parser. Just check the first part of the URL
	int req1Len = strlen(validServerInfoRequest1);
	int req2Len = strlen(validServerInfoRequest2);
	int cmp = 1; // 0 - match; -1, no match; 1 - indeterminant; need more data

	if (data_len >= req1Len) {
		cmp = memcmp(validServerInfoRequest1, data, req1Len);
		if (cmp != 0) {
			cmp = -1; // no match
		}
	}

	// If we don't have a positive match, try the next possibility
	if ((cmp != 0) && (data_len >= req2Len)) {
		cmp = memcmp(validServerInfoRequest2, data, req2Len);
		if (cmp != 0) {
			cmp = -1;
		}
	}

	return cmp;
}

static int
send_standard_vbucket_header(int connfd)
{
	return send(connfd, validServerInfoResponseHeader, strlen(validServerInfoResponseHeader), 0);
}
static int
send_standard_vbucket_body(int connfd, memcache_connector *connector)
{
	char *cur_ptr = serverInfoResponse;
	char *body_start;
	memset(serverInfoResponse, 0, SERVER_INFO_RESPONSE_BUF_SIZE);
	sprintf(cur_ptr, "XXX_CHUNK_LEN\r\n");
	cur_ptr = serverInfoResponse + strlen(serverInfoResponse);
	body_start = cur_ptr;
	strcpy(cur_ptr, validServerInfoResponseBody);

	// do the substitutions before we put on the final footer
	char dataPortStr[16];
	char adminPortStr[16];
	char chunkLenStr[16];
	sprintf(dataPortStr,  "%d", connector->t.TCP_transport.port);
	sprintf(adminPortStr, "%d", connector->couchbase_admin_port);
	subStrReplace(serverInfoResponse, "XXX_DATA_PORT", dataPortStr);
	subStrReplace(serverInfoResponse, "XXX_ADMIN_PORT",  adminPortStr);
	int bodyLen = serverInfoResponse + strlen(serverInfoResponse) - body_start;
	sprintf(chunkLenStr, "%x", (unsigned int)bodyLen);
	subStrReplace(serverInfoResponse, "XXX_CHUNK_LEN", chunkLenStr);

	// and now the footer
	cur_ptr = serverInfoResponse + strlen(serverInfoResponse);
	strcpy(cur_ptr, validServerInfoResponseFooter);

	// send the response
	return send(connfd, serverInfoResponse, strlen(serverInfoResponse), 0);
}

static int
send_error_vbucket_response(int connfd)
{
	return send(connfd, invalidServerInfoResponse, strlen(invalidServerInfoResponse), 0);
}

// Listener - used to tell client about status of vbuckets
// (as far as the client needs to know, all the vbuckets are on
// the proxy)
// This is all written blocking, as I don't expect the client to
// ask very often, or to care if the request takes a little time
// to return.
static void*
couchbase_admin_thread(void *arg)
{
	int rv;

	memcache_connector *connector = (memcache_connector *)arg;
	if (!connector) {
		return NULL;
	}
	if (connector->transportType != TRANSPORT_TCP_SOCKET) {
		cl_warning("Couchbase smart client compatibility requested, but not using TCP sockets for data. Will not be able to handle smart client");
		return NULL;
	}

	// set up socket to listen for admin requests
	int listener = socket(AF_INET, SOCK_STREAM, 0);
	int on = 1;
	setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
	struct sockaddr_in addr;

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	addr.sin_port = htons(connector->couchbase_admin_port);//8099/*8091*/); // NB - standard membase admin port. // XXX make into config var?
	rv = bind(listener, (const struct sockaddr *)&addr, sizeof(addr));
	if (rv != 0) {
		cl_warning("Failed to start membase info listener (%s) - may not be able to handle libcouchbase clients", strerror(errno));
		return NULL;
	}

	while (true) {  /// XXX not shutting down this thread...
		rv = listen(listener, 10);
		if (rv == 0) {
			// got a connection!
			struct sockaddr_in connAddr;
	        socklen_t clen = sizeof(connAddr);
			int connfd = accept(listener, (struct sockaddr *)&connAddr, &clen);
			if (connfd > 0){
				// XXX - ideally, this should happen on its own thread.
				time_t timeout = time(NULL) + 2;  // 2 second timeout. more than enough.
				while (time(NULL) < timeout) {
					// Read data. We're looking for an HTTP GET request with the URL
					// /pools/default/buckets/<bucketname> or
					// /pools/default/bucketsStreaming/<bucketname>
					int buf_len = 1024;
					char buf[buf_len];
					char *p_data = buf;
					int read_len;
					read_len = read(connfd, p_data, buf_len - (p_data-buf));
					if (read_len < 0 && errno != EAGAIN) {
						break;
					} else if (read_len == 0) { // remote close
						break;
					} else {
						p_data += read_len;
						int parse_status = parse_vbucket_request(buf, p_data-buf);  // parse returns 0 (complete) -1 (error) and 1 (waiting for data)
						if (parse_status == 0) { // successful complete
							cl_info("Received a request for vbuckets! Sending standard response.");
							send_standard_vbucket_header(connfd);
							do {
								int nsent = send_standard_vbucket_body(connfd, connector);
								if (nsent <= 0) {
									goto AdminSocketShutdown;  // would just break, but inside a double while. ack.
								}
								sleep(1);
							}while (true);
							break;
						} else if (parse_status < 1) { // error
							cl_info("Received a bad request for vbuckets. Sending error response.");
							send_error_vbucket_response(connfd);
							break;
						}
					}
					usleep(1000);
				}
AdminSocketShutdown:
				shutdown(connfd, SHUT_RDWR);
				close(connfd);
			}
		}
	}
	return NULL;
}

// Current issues:

// - DTD
// - batch. Still a chance that it will linger...
// - ttl semantics are different
// - Change the retry policy to retry once in all examples and this code (default setter sets retry!)
// - authentication?
// - libevent v epoll?

// Puntable?
// - Cluster management. This isn't hooked up - what can I do here?
// - Do I need to support the ascii protocol as well?
// - Be able to configure string or blob as the default type

