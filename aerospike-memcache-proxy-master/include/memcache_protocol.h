/*
 * memcache_header.h
 *
 *  Created on: Mar 31, 2012
 *      Author: carolyn
 */

#ifndef MEMCACHE_HEADER_H_
#define MEMCACHE_HEADER_H_

// The memcache binary wire format
// This structure is followed by extras
// of length extra_len, and then a key
// of length key_len, and then the value.
// 'body_len' is the sum of the length of
// the key, the value, and the extras
typedef struct{
	uint8_t  magic;    //0x80 - request, 0x81 - response
	uint8_t  opcode;
	uint16_t key_len;
	uint8_t  extra_len;
	uint8_t  data_type;
	uint16_t status;   // or vbucket, when in a request.
	uint32_t body_len;
	uint32_t opaque;   // set by caller; returned to caller
	uint64_t CAS;
}__attribute__ ((__packed__)) memcache_header;


// For documentation on the memcache binary protocol, see
// http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped

// Status return messages
// I do not know the difference between 'not found' and 'not stored'.
#define MC_OK              0x0000
#define MC_KEY_NOT_FOUND   0x0001
#define MC_KEY_EXISTS      0x0002
#define MC_VALUE_TOO_LARGE 0x0003
#define MC_INVALID_PARAM   0x0004
#define MC_NOT_STORED      0x0005
#define MC_NOT_NUMERIC     0x0006
#define MC_WRONG_SERVER    0x0007
#define MC_AUTH_ERROR      0x0020
#define MC_AUTH_CONTINUE   0x0021
#define MC_UNKNOWN_COMMAND 0x0081
#define MC_OO_MEMORY       0x0082
#define MC_UNSUPPORTED     0x0083
#define MC_INTERNAL_ERROR  0x0084
#define MC_BUSY            0x0085
#define MC_TEMPORARY_FAIL  0x0086
#define MC_UNKNOWN         0x1001 // citrusleaf - other error
#define MC_TIMEOUT         0x1002 // citrusleaf - timeout

typedef enum{
	MC_op_get       = 0,
	MC_op_set       = 1,
	MC_op_add       = 2,
	MC_op_replace   = 3,
	MC_op_delete    = 4,
	MC_op_increment = 5,
	MC_op_decrement = 6,
	MC_op_quit      = 7,
	MC_op_flush     = 8,
	MC_op_getQ      = 9,
	MC_op_nop       = 10,
	MC_op_version   = 11,
	MC_op_getK      = 12,
	MC_op_getKQ     = 13,
	MC_op_append    = 14,
	MC_op_prepend   = 15,
	MC_op_stat      = 16,
	MC_op_setQ      = 17,
	MC_op_addQ      = 18,
	MC_op_replaceQ  = 19,
	MC_op_deleteQ   = 20,
	MC_op_incrementQ = 21,
	MC_op_decrementQ = 22,
	MC_op_quitQ      = 23,
	MC_op_flushQ     = 24,
	MC_op_appendQ    = 25,
	MC_op_prependQ   = 26,
	MC_op_verbosity  = 27,
	MC_op_touch      = 28,
	MC_op_GAT        = 29,
	MC_op_GATQ       = 30,
	MC_op_GATK       = 35,
	MC_op_GATKQ      = 36,
	CL_op_thr_shutdown   = 200, // not an MC_op, but a way to shut down individual worker threads.
}MC_op;

// Flags used by memcache to indicate the type of data
#define MC_EXFLAGS_BLOB		0x00000001
#define MC_EXFLAGS_INT		0x00000002
#define MC_EXFLAGS_LONG		0x00000004
#define MC_EXFLAGS_COMPRESSED	0x00000008


#endif /* MEMCACHE_HEADER_H_ */
