/*
 * reactor.c
 *
 *  Created on: Mar 18, 2012
 *  Copyright 2012, Citrusleaf
 *
 *  Little reactor object that allows the caller
 *  to register an fd, a list of events to poll,
 *  and a callback function for those events.
 *
 *  The callback happens on the same thread as
 *  the epoll loop, so the callback is strongly
 *  encouraged to get out of dodge as soon as
 *  possible.
 */

#include <sys/epoll.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "citrusleaf/cf_shash.h"
#include "citrusleaf/cf_alloc.h"

#include "reactor.h"
#include "log.h"

typedef struct {
	reactor_cb_fn cb_fn;
	void *udata;
}reactor_cb_info;


#define EPOLL_SZ 1024

#define cf_malloc malloc
#define cf_free   free
#define cf_calloc calloc
#define cf_strdup strdup

int
reactor_register(reactor *r, int fd, int epoll_events, reactor_cb_fn callback, void *udata)
{
	// check if the fd is already registered..
	if (shash_get(r->g_reactor_callbacks, &fd, NULL) != SHASH_ERR_NOTFOUND) {
		cl_warning("Reactor register fails - already in hash");
		return -1;
	}

	// set socket non-blocking
//	int flags;
//	if (-1 == (flags = fcntl(s, F_GETFL, 0)))
//		flags = 0;

	fcntl(fd, F_SETFL, O_NONBLOCK);

	// add fd to the callback hash
	reactor_cb_info cb_info;
	cb_info.cb_fn = callback;
	cb_info.udata = udata;
	if (shash_put(r->g_reactor_callbacks, &fd, (void **)&cb_info) != 0){
		cl_warning("shash_put fails somehow");
		return -1;
	}

	// put fd on epoll list
	struct epoll_event ev;
    memset(&ev, 0, sizeof ev);
    ev.events = epoll_events;  // level-triggered!
    ev.data.fd = fd;

    if (epoll_ctl(r->g_epoll_fd, EPOLL_CTL_ADD, fd, &ev) <0) {
    	cl_warning("epoll ctl add fails, fd is %d, error %s", fd, strerror(errno));
    	if (errno == EBADF) {
    		cl_warning("Bad f - epoll fd is %d, fd is %d", r->g_epoll_fd, fd);
    	}else if (errno == EINVAL) {
    		cl_warning("Invalid - epoll fd is %d, fd is %d", r->g_epoll_fd, fd);
    	}
    	return -1;
    }

    return 0;
}

int
reactor_deregister(reactor *r, int fd)
{
	// remove from shash...
	if (SHASH_OK != shash_delete(r->g_reactor_callbacks, &fd)) {
		return -1;
	}

	// remove from epoll
    if (epoll_ctl(r->g_epoll_fd, EPOLL_CTL_DEL, fd, NULL) <0) {
    	return -1;
    }

    return 0;
}


int
reactor_modify(reactor *r, int fd, int epoll_events, reactor_cb_fn callback, void *udata)
{
	// check if the fd is already registered..
	if (SHASH_OK != shash_get(r->g_reactor_callbacks, &fd, NULL)) {
		return -1;
	}

	// modify callback, if one is specified
	if (callback) {
		reactor_cb_info *cb_info;
		if (SHASH_OK != shash_get(r->g_reactor_callbacks, &fd, (void **)&cb_info)) {
			return -1;
		}
		cb_info->cb_fn = callback;
		cb_info->udata = udata;
	}

	// modify fd on epoll list
	struct epoll_event ev;
    memset(&ev, 0, sizeof ev);
    ev.events = epoll_events;  // level-triggered!
    ev.data.fd = fd;

    if (epoll_ctl(r->g_epoll_fd, EPOLL_CTL_MOD, fd, &ev) <0) {
    	return -1;
    }

    return 0;
}


uint32_t
reactor_hash_fn(void *fd_p)
{
	return cf_hash_fnv(fd_p, sizeof(int));
}

static void *
main_reactor_thr(void *args)
{
	reactor *r = (reactor *)args;

    struct epoll_event events[EPOLL_SZ];

	// create epoll fd and events queue

	while (r->g_running) {
		int nevents = epoll_wait(r->g_epoll_fd, events, EPOLL_SZ, r->g_epoll_interval / 3);
        for (int i = 0; i < nevents; i++) {
        	int fd = events[i].data.fd;
        	reactor_cb_info cb_info;
        	if( SHASH_OK != shash_get(r->g_reactor_callbacks, &fd, &cb_info) ||
        			!cb_info.cb_fn) {
        		cl_warning("event on fd %d, but has no callback. Ignoring", fd);
        		continue;
        	}
        	cb_info.cb_fn(fd, events[i].events, cb_info.udata);
        }
	}

	return NULL;
}

reactor *
reactor_create()
{
	reactor *r = (reactor *)cf_malloc(sizeof(reactor));
	memset(r, 0, sizeof(reactor));
	r->g_epoll_fd = -1;
	r->g_epoll_interval = 1000;
	r->g_running = true;

	if (SHASH_OK != shash_create(&r->g_reactor_callbacks, reactor_hash_fn, sizeof(int), sizeof(reactor_cb_info), 100, SHASH_CR_MT_MANYLOCK)){
		cl_warning("Could not create hash for reactor callbacks!!");
	    return NULL;
	}

	r->g_epoll_fd = epoll_create(EPOLL_SZ);
	if (r->g_epoll_fd <= 0) {
		cl_warning("Error creating epoll object, error is %s", strerror(errno));
		return NULL;
	}else{
		cl_info("Created epoll object");
	}

	r->g_running = true;
	pthread_create(&r->g_reactor_thr, 0, main_reactor_thr, r);
	return r;
}

void
reactor_destroy(reactor *r)
{
	r->g_running = false;
	pthread_join(r->g_reactor_thr, NULL);
	pthread_detach(r->g_reactor_thr);
	shash_destroy(r->g_reactor_callbacks);
	cf_free(r);
}



