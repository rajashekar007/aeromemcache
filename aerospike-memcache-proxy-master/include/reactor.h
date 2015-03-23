/*
 * reactor.h
 *
 *  Created on: Mar 18, 2012
 *      Author: carolyn
 */

#ifndef REACTOR_H_
#define REACTOR_H_

#include <citrusleaf/cf_shash.h>

typedef struct {
	shash *g_reactor_callbacks;  // hash, by fd, of cb info
	int g_epoll_fd;
	int g_epoll_interval;
	int g_running;
	pthread_t g_reactor_thr;
}reactor;

typedef void (*reactor_cb_fn)(int fd, uint32_t, void *udata);

int reactor_register(reactor *r, int fd, int epoll_events, reactor_cb_fn callback, void *udata);
int reactor_deregister(reactor *r, int fd);
int reactor_modify(reactor *r, int fd, int epoll_events, reactor_cb_fn callback, void *udata);
reactor *reactor_create();
void reactor_destroy(reactor *r);


#endif /* REACTOR_H_ */
