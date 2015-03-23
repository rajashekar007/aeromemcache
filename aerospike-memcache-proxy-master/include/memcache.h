/*
 * memcache.h
 *
 *  Created on: Mar 17, 2012
 *  Copyright 2012 Citrusleaf Inc
 *  - CSW
 */

#ifndef MEMCACHE_H_
#define MEMCACHE_H_

#include <libxml2/libxml/xmlexports.h>
#include <libxml2/libxml/xmlstring.h>
#include <libxml2/libxml/tree.h>

#include <citrusleaf/citrusleaf.h>
#include <transport.h>
#include <proxy.h>
#include <memcache_protocol.h>

typedef struct
{
	cl_proxy_connector base;
	cl_transport_type transportType;
	union{
		cl_unix_socket_transport unix_socket_transport;
		cl_pipe_transport        pipe_transport;
		cl_TCP_transport         TCP_transport;
	}t;
	char *namespace;
	char *set;
	char *version;
	cl_cluster *cluster;
	int32_t read_timeout;
	int32_t write_timeout;
	uint16_t couchbase_admin_port;
}memcache_connector;


memcache_connector *memcacheConnectorCreate(xmlNode *node, cl_cluster *cluster, int read_timeout, int write_timeout);
void memcacheConnectorDestroy(struct cl_proxy_connector_s *c);

void memcache_add_workers(int n_workers);

// utility functions - Translate to and from the network...
inline static void
memcache_header_from_network(memcache_header *hdr)
{
	hdr->CAS      = cl_ntohll(hdr->CAS);
	hdr->body_len = ntohl(hdr->body_len);
	hdr->key_len  = ntohs(hdr->key_len);
	hdr->status   = ntohs(hdr->status);
}

inline static void
memcache_body_from_network(memcache_header *hdr, uint8_t *body)
{
	if (!body)
		return;

	// How we interpret the body depends on which opcode this is,
	// whether it is request or a response, and if the response is
	// okay or an error.
	if (hdr->magic == 0x80) {       // request
		switch (hdr->opcode){
		case MC_op_set:
		case MC_op_setQ:
		case MC_op_replace:
		case MC_op_replaceQ:
		case MC_op_add:
		case MC_op_addQ:
			// four bytes of flags, then 4 bytes of expiration
			*(uint32_t *)body       = ntohl(*(uint32_t *)body);
			*(uint32_t *)(body + 4) = ntohl(*(uint32_t *)(body + 4));
			break;
		case MC_op_increment:
		case MC_op_incrementQ:
		case MC_op_decrement:
		case MC_op_decrementQ:
			// 8 bytes delta, 8 bytes initial value, 4 bytes expiration
			*(uint64_t *)(body    )  = cl_ntohll(*(uint64_t *)(body));
			*(uint64_t *)(body + 8)  = cl_ntohll(*(uint64_t *)(body + 8));
			*(uint32_t *)(body + 16) =     ntohl(*(uint32_t *)(body + 16));
			break;
		case MC_op_flush:
		case MC_op_flushQ:
			// there *may* be a 4 bytes expiration...
			if (hdr->extra_len != 0){
				*(uint32_t *)(body) = *(uint32_t *)(body) = ntohl(*(uint32_t *)(body));
			}
			break;
		case MC_op_verbosity:
			// four bytes of level
			*(uint32_t *)(body) = *(uint32_t *)(body) = ntohl(*(uint32_t *)(body));
			break;
		case MC_op_touch:
		case MC_op_GAT:
		case MC_op_GATQ:
		case MC_op_GATK:
		case MC_op_GATKQ:
			// four bytes of expiration
			*(uint32_t *)(body) = *(uint32_t *)(body) = ntohl(*(uint32_t *)(body));
			break;
		default:
			// no extras, do nothing
			break;
		}
	}else if (hdr->magic == 0x81) { // response
		if (hdr->status == 0) {     // normal response
			switch (hdr->opcode){
			case MC_op_get:
			case MC_op_getQ:
			case MC_op_getK:
			case MC_op_getKQ:
			case MC_op_GATK:
			case MC_op_GATKQ:
			case MC_op_touch:
				// four bytes of flags
				*(uint32_t *)(body) = *(uint32_t *)(body) = ntohl(*(uint32_t *)(body));
				break;
			case MC_op_increment:
			case MC_op_incrementQ:
			case MC_op_decrement:
			case MC_op_decrementQ:
				// 8 bytes numeric value
				*(uint64_t *)(body) = cl_ntohll(*(uint64_t *)(body));
				break;
			default:
				// no extras, do nothing
				break;
			}
		}else{                      // error response
			// do nothing.
		}
	}
}

inline static void
memcache_to_network(memcache_header *hdr, uint8_t *body)
{
	hdr->CAS      = cl_htonll(hdr->CAS);
	hdr->body_len = htonl(hdr->body_len);
	hdr->key_len  = htons(hdr->key_len);
	hdr->status   = htons(hdr->status);

	// How we interpret the body depends on which opcode this is,
	// whether it is request or a response, and if the response is
	// okay or an error.
	if (hdr->magic == 0x80) {       // request
		switch (hdr->opcode){
		case MC_op_set:
		case MC_op_setQ:
		case MC_op_replace:
		case MC_op_replaceQ:
		case MC_op_add:
		case MC_op_addQ:
			// four bytes of flags, then 4 bytes of expiration
			*(uint32_t *)(body    ) = htonl(*(uint32_t *)body);
			*(uint32_t *)(body + 4) = htonl(*(uint32_t *)(body + 4));
			break;
		case MC_op_increment:
		case MC_op_incrementQ:
		case MC_op_decrement:
		case MC_op_decrementQ:
			// 8 bytes delta, 8 bytes initial value, 4 bytes expiration
			*(uint64_t *)(body    ) = cl_htonll(*(uint64_t *)(body));
			*(uint64_t *)(body + 8) = cl_htonll(*(uint64_t *)(body + 8));
			*(uint32_t *)(body + 16) =     htonl(*(uint32_t *)(body + 16));
			break;
		case MC_op_flush:
		case MC_op_flushQ:
			// there *may* be a 4 bytes expiration...
			if (hdr->extra_len != 0){
				*(uint32_t *)(body) = htonl(*(uint32_t *)(body));
			}
			break;
		case MC_op_verbosity:
			// four bytes of level
			*(uint32_t *)(body) = htonl(*(uint32_t *)(body));
			break;
		case MC_op_touch:
		case MC_op_GAT:
		case MC_op_GATQ:
		case MC_op_GATK:
		case MC_op_GATKQ:
			// four bytes of expiration
			*(uint32_t *)(body) = htonl(*(uint32_t *)(body));
			break;
		default:
			// no extras, do htoning
			break;
		}
	}else if (hdr->magic == 0x81) { // response
		if (hdr->status == 0) {     // normal response
			switch (hdr->opcode){
			case MC_op_get:
			case MC_op_getQ:
			case MC_op_getK:
			case MC_op_getKQ:
			case MC_op_touch:
			case MC_op_GATK:
			case MC_op_GATKQ:
			case MC_op_GAT:
			case MC_op_GATQ:
				// four bytes of flags
				*(uint32_t *)(body) = htonl(*(uint32_t *)(body));
				break;
			case MC_op_increment:
			case MC_op_incrementQ:
			case MC_op_decrement:
			case MC_op_decrementQ:
				// 8 bytes numeric value
				*(uint64_t *)(body) = cl_htonll(*(uint64_t *)(body));
				break;
			default:
				// no extras, do nothing
				break;
			}
		}else{                      // error response
			// do nothing.
		}
	}
}



#endif /* MEMCACHE_H_ */
