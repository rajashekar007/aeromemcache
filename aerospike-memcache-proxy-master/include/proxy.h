/*
 * proxy.h
 *
 *  Created on: Mar 17, 2012
 *      Author: carolyn
 */

#ifndef PROXY_H_
#define PROXY_H_

#include <stdlib.h>
#include <citrusleaf/cf_ll.h>
#include <reactor.h>

#ifndef NULL
#define NULL (void *)0
#endif

#define cf_malloc malloc
#define cf_free free
#define cf_calloc calloc
#define cf_strdup strdup


typedef enum {
	CLUSTER_STATE_UNINITIALIZED = 0,
	CLUSTER_STATE_DISCONNECTED,
	CLUSTER_STATE_CONNECTED
}cl_cluster_state;

// Memcache is the only type of connector
// currently supported. Feel free to add more!
typedef enum{
	CONNECTOR_MEMCACHE,
	CONNECTOR_UNKNOWN
}cl_connector_type;

struct cl_proxy_connector_s;

typedef void (*cluster_state_change_cb(cl_cluster_state state));
typedef void (*cl_connector_dtor(struct cl_proxy_connector_s *connector));

// Proxy connectors:
// All proxy connectors must subclass the following structure.
// The callback will be called when the state of the cluster
// changes (if we lose contact with the entire cluster, or
// regain contact with it. The callback may be NULL if the
// connector does not want to hear about state changes.

// very very poor man's subclassing
typedef struct cl_proxy_connector_s{
	cl_connector_type type;
	size_t connectorSize;
	cl_connector_dtor *dtor;
	cluster_state_change_cb *cb; // not actually using this yet... XXX
}cl_proxy_connector;

reactor *get_global_reactor();

// utility function - should go in cf_ll!!
inline static void
util_deleteList(cf_ll *list)
{
	cf_ll_element *e = cf_ll_get_head(list);
	while (e){
		cf_ll_element *e_next = e->next;
		cf_ll_delete(list, e);
		e = e_next;
	}
	free(list);
}

inline static void
util_genericLlElementDtor(cf_ll_element *element)
{
	free(element);
}

extern int g_be; // are we big endian or little endian?

#define cl_htonll(x) g_be?(x):__bswap_64(x)
#define cl_ntohll(x) g_be?(x):__bswap_64(x)




#endif /* PROXY_H_ */
