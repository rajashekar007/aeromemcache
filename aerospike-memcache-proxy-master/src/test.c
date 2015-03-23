/*
 * test.c
 *
 *  Created on: Mar 31, 2012
 *      Author: CSW
 *      Copyright Citrusleaf, 2012
 *      Simple read/write test for memcache proxy
 */
#include <sys/socket.h>
#include <netdb.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>

#include "proxy.h"
#include "memcache.h"
#include "memcache_protocol.h"

// There has to be a better way of doing this...
int g_be = true;

static void
init_endian()
{
	uint64_t test = 0x01;
	if (*((char *)&test) == 0x01) {
		g_be = false;
	}else{
		g_be = true;
	}
}

memcache_header *g_write_request;
memcache_header *g_read_request;
memcache_header *g_response;

int g_memcache_fd;

const char *g_key  = "memcache_test_key";
const char *g_key2 = "key2";
const char *g_key3 = "key3";
const char *g_latency_key="latency_key";
const char *g_latency_val="foo";
const char *g_value1 = "first_value";
const char *g_value2 = "second_value";
const char *g_value3 = "third value";
uint32_t g_opaque1 = 0xDE1FE1F0;
uint32_t g_opaque2 = 0xF00BAAA0;
const char *g_numeric_string= "12345";
uint64_t g_numeric_value  = 12345;
uint64_t g_delta         = 32;
const char *g_numeric_string_plus_delta = "12377";
uint64_t g_numeric_value_plus_delta = 12377;


static int
init_test(void *transport, cl_transport_type type)
{
	sleep(2);
	// Set up the connection to the proxy...
	if( type == TRANSPORT_TCP_SOCKET) {
		cl_TCP_transport *tcp_transport = (cl_TCP_transport *)transport;
		g_memcache_fd   = socket(AF_INET, SOCK_STREAM, 0);
		struct sockaddr_in addr_in;
		int retVal=inet_pton(AF_INET, tcp_transport->hostname, &addr_in.sin_addr.s_addr);

		if (0 >= retVal) {
			fprintf(stdout,"%d %s",retVal,"test: inet_pton failure\n");

		}
		memset(&addr_in, 0, sizeof(addr_in));
		addr_in.sin_family = AF_INET;
		addr_in.sin_port   = htons(tcp_transport->port);
		int rv = connect(g_memcache_fd, (struct sockaddr *)&addr_in, sizeof(addr_in));
		if (rv != 0){
			fprintf(stdout, "Test: Error connecting to TCP socket, error %s\n", strerror(errno));
			close(g_memcache_fd);
			return -1;
		}
	}else if (type == TRANSPORT_UNIX_SOCKET) {
		cl_unix_socket_transport *unix_transport = (cl_unix_socket_transport *)transport;
		g_memcache_fd   = socket(AF_UNIX, SOCK_STREAM, 0);
		struct sockaddr_un addr_un;
		memset(&addr_un, 0, sizeof(addr_un));
		addr_un.sun_family = AF_UNIX;
		strcpy(addr_un.sun_path, unix_transport->name);
		int rv = connect(g_memcache_fd, (struct sockaddr *)&addr_un, sizeof(addr_un));
		if (rv != 0){
			fprintf(stdout, "Test: Error connecting to Unix socket, error %s\n", strerror(errno));
			close(g_memcache_fd);
			return -1;
		}
	}

	return 0;
}


static memcache_header *
init_memcache_header(int opcode, uint32_t opaque, int extraLen, int keyLen, int valueLen, uint64_t CAS)
{
	int hdr_size = sizeof(memcache_header) + extraLen + keyLen + valueLen;
	memcache_header *hdr = (memcache_header *)malloc(hdr_size);
	memset(hdr, 0, hdr_size);
	hdr->opcode = opcode;
	hdr->opaque = opaque;
	hdr->magic  = 0x80; // request
	hdr->extra_len = extraLen;
	hdr->key_len   = keyLen;
	hdr->body_len  = extraLen + keyLen + valueLen;
	hdr->CAS       = CAS;

	return hdr;
}

static int
sendRequestQ(memcache_header *request)
{
	// get total length
	int bytes_to_send = sizeof(memcache_header) + request->body_len;
	// now flip into network byte order
	memcache_to_network(request, (uint8_t *)request + sizeof(memcache_header));

	// and send
	int bytes_sent   = write(g_memcache_fd, (void *)request, bytes_to_send);
	if (bytes_sent != bytes_to_send){
		fprintf(stdout, "Did not send full package!\n");
		return -1;
	}
	return 0;
}

static int
readResult(memcache_header **response_header, uint8_t **response_body)
{
	int rv = 0;
	memcache_header *local_response_header = NULL;
	uint8_t *local_response_body = NULL;

	local_response_header = (memcache_header *)malloc(sizeof(memcache_header));
	int bytes_to_receive = sizeof(memcache_header);
//	fprintf(stdout, "reading result\n");
	int bytes_received  = read(g_memcache_fd, local_response_header, bytes_to_receive);
	if (bytes_received != bytes_to_receive){
		fprintf(stdout, "Did not receive the correct number of bytes: expected %d, received %d, error %s\n", bytes_to_receive, bytes_received,strerror(errno));
		rv = -1;
		goto Exit;
	}

	// flip from network to local
	memcache_header_from_network(local_response_header);

	if (local_response_header->body_len > 0) {
		local_response_body = (uint8_t *)malloc(local_response_header->body_len);
		bytes_to_receive = local_response_header->body_len;
		bytes_received = read(g_memcache_fd, local_response_body, bytes_to_receive);
		if (bytes_received != bytes_to_receive) {
			fprintf(stdout, "Did not receive correct number of bytes in body - expected %d, received %d\n", bytes_to_receive, bytes_received);
			rv = -1;
			goto Exit;
		}
	}

	// network to local again
	memcache_body_from_network(local_response_header, local_response_body);

Exit:
	if (rv == 0){
		if (response_header)
			*response_header = local_response_header;
		if (response_body)
			*response_body = local_response_body;
	}else{
		if (local_response_header)
			free(local_response_header);
		if (local_response_body)
			free(local_response_body);
	}
	return rv;
}

bool debug = false;
static int
checkResult(memcache_header *result, uint8_t *data, int expected_status, uint32_t opaque)
{
	int rv = 0;

	// Check that result is what is expected...
	if (result->magic != 0x81) {
		fprintf(stdout, " Result did not return correct magic code\n");
		rv = -1;
		goto Exit;
	}

	if (result->opaque != opaque) {
		fprintf(stdout, " Opaque values do not match, expected %d have %d\n", opaque, result->opaque);
		rv = -1;
		goto Exit;
	}

	if (result->status != expected_status){
		if (result->status != MC_OK ){
			char errStr[256];
			memcpy(errStr, data, result->body_len);
			errStr[result->body_len] = '\0';
			if (debug) {
				fprintf(stdout, " Unexpected result %d,  %s\n", result->status, errStr);
			}
		}else{
			if (debug) {
				fprintf(stdout, " Unexpected result %d, MC_OK\n",result->status);
			}
		}
		rv = -1;
		goto Exit;
	}

Exit:
	return rv;
}

static int
sendRequest(memcache_header *request, int expected_status, uint32_t opaque, memcache_header **response_header, uint8_t **response_body)
{
	memcache_header *result = NULL;
	uint8_t *data = NULL;
	int rv = 0;

	// send to proxy
	if (0 != sendRequestQ(request)) {
		rv = -1;
		goto Exit;
	}

	// get result
	if ((rv = readResult(&result, &data)) != 0) {
		goto Exit;
	}

	if ((rv = checkResult(result, data, expected_status, opaque)) != 0) {
		goto Exit;
	}

Exit:

	if (response_header) {
		*response_header = result;
	}else if (result) {
		free(result);
	}

	if (response_body) {
		*response_body = data;
	}else if (data) {
		free(data);
	}

	return rv;
}

static void
freeResponse(void *hdr, void *response_header, void *response_body)
{
	if (hdr)
		free(hdr);
	if (response_header)
		free(response_header);
	if (response_body)
		free(response_body);
}

static int
writeKeyQ(const char *key, const char *value, uint32_t opaque, int expiration, uint64_t CAS)
{
	// set up write request header
	memcache_header *hdr = init_memcache_header(MC_op_setQ, opaque, 8, strlen(key), strlen(value), CAS);

	// set up extra, key, and data
	uint32_t *extraPtr = (uint32_t *)((uint8_t *)hdr + sizeof(memcache_header));
	uint8_t  *keyPtr   = ((uint8_t *)hdr + sizeof(memcache_header) + 8);
	uint8_t  *dataPtr  = ((uint8_t *)hdr + sizeof(memcache_header) + 8 + hdr->key_len);

	*extraPtr = 0x0;
	*(extraPtr + 1) = expiration;

	memcpy(keyPtr, key, hdr->key_len);
	memcpy(dataPtr, value, hdr->body_len - hdr->key_len - hdr->extra_len);

	int rv = sendRequestQ(hdr);

	free(hdr);
	return rv;
}

static int
doNOOP(uint32_t opaque)
{
	int rv = 0;
	// set up write request header
	memcache_header *hdr = init_memcache_header(MC_op_nop, opaque, 0, 0, 0, 0);

	rv = sendRequestQ(hdr);

	free(hdr);

	return rv;
}

static int
append_prepend_impl(const char *key, const char * value, uint32_t opaque, uint64_t CAS, bool is_append)
{
	// set up write request header
	memcache_header *hdr = init_memcache_header(is_append?MC_op_append:MC_op_prepend, opaque,0, strlen(key), strlen(value), CAS);

	// set up key, and data
	uint8_t  *keyPtr   = ((uint8_t *)hdr + sizeof(memcache_header));
	uint8_t  *dataPtr  = ((uint8_t *)hdr + sizeof(memcache_header) + hdr->key_len);
	
	memcpy(keyPtr,  key, hdr->key_len);
	memcpy(dataPtr, value, hdr->body_len - hdr->key_len - hdr->extra_len);

	memcache_header *response_header;
	uint8_t *response_body;
	int rv = sendRequest(hdr, MC_OK, opaque, &response_header, &response_body);

	if (rv != 0 || response_body != NULL){
		fprintf(stdout, " Write key fails, error %d\n", response_header?response_header->status:0);
		rv = -1;
	}

	freeResponse(hdr, response_header, response_body);

	return rv;
}

static int
appendKey(const char *key, const char * value, uint32_t opaque, uint64_t CAS)
{
	return append_prepend_impl(key, value, opaque, CAS, true);
}

static int
prependKey(const char *key, const char * value, uint32_t opaque, uint64_t CAS)
{
	return append_prepend_impl(key, value, opaque, CAS, false);
}
static int
writeKey(const char *key, const char *value, uint32_t opaque, int expiration, uint64_t CAS)
{
	// set up write request header
	memcache_header *hdr = init_memcache_header(MC_op_set, opaque, 8, strlen(key), strlen(value), CAS);

	// set up extra, key, and data
	uint32_t *extraPtr = (uint32_t *)((uint8_t *)hdr + sizeof(memcache_header));
	uint8_t  *keyPtr   = ((uint8_t *)hdr + sizeof(memcache_header) + 8);
	uint8_t  *dataPtr  = ((uint8_t *)hdr + sizeof(memcache_header) + 8 + hdr->key_len);

	*extraPtr = 0x0;
	*(extraPtr + 1) = expiration;

	memcpy(keyPtr,  key, hdr->key_len);
	memcpy(dataPtr, value, hdr->body_len - hdr->key_len - hdr->extra_len);

	memcache_header *response_header;
	uint8_t *response_body;
	int rv = sendRequest(hdr, MC_OK, opaque, &response_header, &response_body);

	if (rv != 0 || response_body != NULL){
		fprintf(stdout, " Write key fails, error %d\n", response_header?response_header->status:0);
		rv = -1;
	}

	freeResponse(hdr, response_header, response_body);

	return rv;
}

static int
readKeyQ(const char *key, const char *expected_value, uint32_t opaque, bool getK)
{
	// set up write request header
	memcache_header *hdr = init_memcache_header(getK?MC_op_getKQ:MC_op_getQ, opaque, 0, strlen(key), 0, 0);

	// set up extra, key, and data
	uint8_t  *keyPtr   = ((uint8_t *)hdr + sizeof(memcache_header));
	memcpy(keyPtr, key, hdr->key_len);

	int rv = sendRequestQ(hdr);
	free(hdr);

	return rv;
}

static int
readKey(const char *key, const char *expected_value, uint32_t opaque, bool getK, bool GAT)
{
	// set up write request header
	MC_op op = MC_op_get;
	if (getK && !GAT) {
		op = MC_op_getK;
	}else if (!getK && GAT) {
		op = MC_op_GAT;
	}else if (getK && GAT) {
		op = MC_op_GATK;
	}
	memcache_header *hdr = init_memcache_header(op, opaque, GAT?4:0, strlen(key), 0, 0);

	// set up extra, key, and data
	uint32_t extra_len = 0;
	uint8_t *body_ptr = (uint8_t *)hdr + sizeof(memcache_header);
	if (GAT) {
		extra_len = 4;
		uint32_t *body_ptr_32 = (uint32_t *)body_ptr;
		*body_ptr_32 = 3800; // default value for expiration...
	}
	uint8_t  *keyPtr   = body_ptr + extra_len;
	memcpy(keyPtr, key, hdr->key_len);

	// send
	memcache_header *response_header = NULL;
	uint8_t *response_body = NULL;
	int rv = sendRequest(hdr, MC_OK, opaque, &response_header, &response_body);

	// validate response
	if (rv != 0 || response_body == NULL){
		fprintf(stdout, " Read Key - request fails %d   \n",rv);
		rv = -1;
		goto Exit;
	}
	keyPtr = response_body + response_header->extra_len;
	//uint8_t *valuePtr = keyPtr + response_header->key_len;
	/*
	if (memcmp((char *)valuePtr, expected_value, strlen(expected_value))) {
		fprintf(stdout, " Read Key - value returned (%s) is not expected value\n", valuePtr);
		rv = -1;
		goto Exit;
	}else if (getK) {
		if (response_header->key_len == 0 ) {
			fprintf(stdout, " Read Key - GetK op did not return key\n");
			rv = -1;
			goto Exit;
		}else if (strlen(key) != response_header->key_len || strncmp(key, (char *)keyPtr, strlen(key))) {
			fprintf(stdout, " Read key - GetK returns invalid key\n");
			rv = -1;
			goto Exit;
		}
	}
	*/

Exit:
	freeResponse(hdr, response_header, response_body);
	return rv;
}

/*
static int
doTransaction(int mc_op, uint64_t opaque, const char *key, const char *value, uint8_t *extra, uint32_t extra_len, uint64_t CAS,
		int rsp_expected, bool rsp_has_extra, bool rsp_has_key, bool rsp_has_value)
{
	memcache_header *hdr = init_memcache_header(op, opaque, extra_len, key?strlen(key):0, value?strlen(value):0);
	hdr->CAS = CAS;

	// set up extra, key, and data
	uint8_t *extraPtr = ((uint8_t *)hdr + sizeof(memcache_header));
	uint8_t  *keyPtr  = extraPtr = extra_len;
	uint8_t *valuePtr = keyPtr + strlen(key);
	if (extra)
		memcpy(extraPtr, extra, extra_len);
	if (key)
		memcpy(keyPtr, key, strlen(key));
	if (value)
		memcpy(valuePtr, value, strlen(value));

	// send
	memcache_header *response_header = NULL;
	memcache_header *response_body   = NULL;
	int rv = sendRequest(hdr, MC_OK, opaque, &response_header, &response_body);

	if (rv != 0 || response_header->status != rsp_expected){
		freeResponse(hdr, response_header, response_body);
		return -1;
	}

	if (rv == 0 && response_body != NULL){

}
*/
static int
deleteKey(const char *key, uint32_t opaque)
{
	memcache_header *hdr = init_memcache_header(MC_op_delete, opaque, 0, strlen(key), 0, 0);

	// set up extra, key, and data
	uint8_t  *keyPtr   = ((uint8_t *)hdr + sizeof(memcache_header));
	memcpy(keyPtr, key, hdr->key_len);

	// send
	memcache_header *response_header;
	uint8_t *response_body = NULL;
	int rv = sendRequest(hdr, MC_OK, opaque, &response_header, &response_body);

	if (rv == 0 && response_body != NULL){
		fprintf(stdout, " Delete key error: %s\n", response_body);
		rv = -1;
	}

	freeResponse(hdr, response_header, response_body);
	return rv;
}

static int
incrementKey(const char *key, uint64_t delta, uint32_t opaque,uint64_t initial_value, uint32_t expiration, uint64_t CAS, uint64_t expected_value)
{
	memcache_header *hdr = init_memcache_header(MC_op_increment, opaque, 20, strlen(key), 0, CAS);

	// set up extra, key, and data
	uint8_t  *extraPtr = (uint8_t *)hdr + sizeof(memcache_header);
	uint8_t  *keyPtr   = extraPtr + hdr->extra_len;
	memcpy(keyPtr, key, hdr->key_len);
	*(uint64_t *)extraPtr = delta;
	*(uint64_t *)(extraPtr + 8)  = initial_value;
	*(uint32_t *)(extraPtr + 16) = expiration;

	// send
	memcache_header *response_header;
	uint8_t *response_body;
	int rv = sendRequest(hdr, MC_OK, opaque, &response_header, &response_body);

	if (rv != 0) {
		goto Exit;
	}

	if (response_body == NULL || response_header->body_len != 8){
		fprintf(stdout, " increment key returns no body\n");
		rv = -1;
		goto Exit;
	}

	uint64_t response_value = *(uint64_t *)response_body;
	if (response_value != expected_value) {
		fprintf(stdout, " increment does not return expected value actual: %"PRIu64", expected: %"PRIu64"\n", response_value, expected_value);
		rv = -1;
		goto Exit;
	}

Exit:
	freeResponse(hdr, response_header, response_body);
	return rv;
}

static int
addKey(const char *key, const char *value, uint32_t opaque, uint64_t expiration)
{
	// set up write request header
	memcache_header *hdr = init_memcache_header(MC_op_add, opaque, 8, strlen(key), strlen(value), 0);

	// set up extra, key, and data
	uint32_t *extraPtr = (uint32_t *)((uint8_t *)hdr + sizeof(memcache_header));
	uint8_t  *keyPtr   = ((uint8_t *)hdr + sizeof(memcache_header) + 8);
	uint8_t  *dataPtr  = ((uint8_t *)hdr + sizeof(memcache_header) + 8 + hdr->key_len);

	*extraPtr = 0x0;
	*(extraPtr + 1) = expiration;

	memcpy(keyPtr, key, hdr->key_len);
	memcpy(dataPtr, value, hdr->body_len - hdr->key_len - hdr->extra_len);

	memcache_header *response_header;
	uint8_t *response_body;
	int rv = sendRequest(hdr, MC_OK, opaque, &response_header, &response_body);

	if (rv == 0 && response_body == NULL){
		rv = 0;
	}else{
		fprintf(stdout, " Add key fails, error %d\n", response_header->status);
		rv = -1;
	}

	freeResponse(hdr, response_header, response_body);

	return rv;
}


int g_nTests;
int g_nSuccesses;

static int
getAndValidate(int status, uint32_t opaque, const char *value, bool allow_queue_terminators)
{
	int rv = 0;
	memcache_header *hdr = NULL;
	uint8_t *data = NULL;

	if (allow_queue_terminators) {
		bool b_is_terminator = true;
		while (b_is_terminator) {
			rv = readResult(&hdr, &data);
			if (rv != 0) {
				goto Exit;
			}
			b_is_terminator = (hdr->opcode == MC_op_nop || hdr->opcode == MC_op_set || hdr->opcode == MC_op_get);
			if (b_is_terminator) {
				fprintf(stdout, "Ignoring terminator opaque %d\n", hdr->opaque);
			}
		}
	}else{
		rv = readResult(&hdr, &data);
		if (rv != 0) {
			goto Exit;
		}
	}

	rv = checkResult(hdr, data, status, opaque);
	if (rv != 0) {
		goto Exit;
	}

	// now validate data
	if (value) {
		uint8_t *dataPtr = data + hdr->extra_len + hdr->key_len;
		int dataLen = hdr->body_len - hdr->extra_len - hdr->key_len;

		if (dataLen != strlen(value) || strncmp((char *)dataPtr, value, strlen(value))) {
			fprintf(stdout, " data returned does not match expected value\n");
			rv = -1;
			goto Exit;
		}
	}

Exit:
	if (hdr){
		free(hdr);
	}
	if (data) {
		free(data);
	}
	return rv;
}


static bool
writeReadBackTest()
{
	g_nTests++;
	if (writeKey(g_key, g_value1, g_opaque1, 3600, 0) != 0) {
		fprintf(stdout, "*** ERROR: writeReadBack test fails write\n");
		return false;
	}

	if (readKey(g_key, g_value1, g_opaque2, false, false) != 0) {
		fprintf(stdout, "*** ERROR: writeReadBack test readback fails\n");
		return false;
	}
	g_nSuccesses++;
	return true;
}

static bool
addUniqueTest()
{
	g_nTests++;
	deleteKey(g_key, g_opaque2);

	if (addKey(g_key, g_value1, g_opaque1, 3600) != 0) {
		fprintf(stdout, "*** ERROR: Add unique test fails to add unique key\n");
		return false;
	}

	if (readKey(g_key, g_value1, g_opaque1, false, false) != 0) {
		fprintf(stdout, "*** ERROR: Add unique test fails - set value not returned correctly\n");
		return false;
	}

	if (addKey(g_key, g_value2, g_opaque1, 3600) == 0) {
		fprintf(stdout, "*** ERROR : Add unique succeeds when key exists\n");
		return false;
	}

	g_nSuccesses++;
	return true;
}

static bool
incrementTest()
{
	g_nTests++;
	writeKey(g_key, &g_numeric_value, g_opaque1, 3600, 0);
	if (readKey(g_key, g_numeric_value, g_opaque1, false, false) != 0) {
		fprintf(stdout, "error - Could not read back numeric result\n");
	}

	if (incrementKey(g_key, g_delta, g_opaque1, g_numeric_value, 0xffffffff, 0, g_numeric_value + g_delta) != 0) {
		fprintf(stdout, "*** ERROR: Increment key with value not set fails 2\n");
/*		if (readKey(g_key, g_numeric_string_plus_delta, g_opaque1, false, false) != 0) {
			fprintf(stdout, "error - Could not read back numeric result\n");
		}
*/		return false;
	}

/*	deleteKey(g_key, g_opaque2);
	if (incrementKey(g_key, g_delta, g_opaque1, g_numeric_value, 3600, 0, g_numeric_value) != 0) {
		fprintf(stdout, "*** ERROR: Increment key with value not set fails\n");
		return false;
	}

	if (readKey(g_key, g_numeric_string, g_opaque1, false, false) != 0) {
		fprintf(stdout, "*** ERROR: Increment key when key not found sets incorrect value\n");
		return false;
	}

	if (writeKey(g_key, g_numeric_string, g_opaque2, 3600, 0) != 0){
		fprintf(stdout, "*** Error: Could not set numeric string\n");
		return false;
	}

	if (incrementKey(g_key, g_delta, g_opaque1, 0, 0xffffffff, 0, g_numeric_value + g_delta) != 0) {
		fprintf(stdout, "*** ERROR: Increment key when value exists fails\n");
		return false;
	}

	if (readKey(g_key, g_numeric_string_plus_delta, g_opaque2, false, false) != 0){
		fprintf(stdout, "*** ERROR: Read of incremented key returns wrong value\n");
		return false;
	}
*/
	g_nSuccesses++;
	return true;
}

static bool
getKTest()
{
	g_nTests++;
	deleteKey(g_key, g_opaque1);
	writeKey(g_key, g_numeric_string, g_opaque2, 3600, 0);
	if (readKey(g_key, g_numeric_string, g_opaque2, true, false) != 0) {
		fprintf(stdout, "*** ERROR: Get with key return fails\n");
		return false;
	}

	g_nSuccesses++;
	return true;
}

static bool
CASTest()
{
	g_nTests++;
	deleteKey(g_key, g_opaque1);
	writeKey(g_key, g_value1, g_opaque1, 3600, 0);
//	writeKey(g_key, g_value1, g_opaque1, 3600, 0);

	if (writeKey(g_key, g_value2, g_opaque1, 3600, 2) == 0) {
		fprintf(stdout, "*** ERROR: Bad CAS write succeeds\n");
		return false;
	}

	if (writeKey(g_key, g_value3, g_opaque1, 3600, 1) != 0) {
		fprintf(stdout, "*** ERROR: Matched CAS write fails\n");
		return false;
	}

	if (readKey(g_key, g_value3, g_opaque2, false, false) != 0) {
		fprintf(stdout, "*** ERROR: CAS write wrote wrong value\n");
		return false;
	}

	g_nSuccesses++;
	return true;
}

static bool
deleteTest()
{
	g_nTests++;

	if (writeKey(g_key, g_value1, 3600, g_opaque1, 0) != 0) {
		fprintf(stdout, "*** ERROR: Delete - could not write key\n");
		return false;
	}

	if (deleteKey(g_key, g_opaque2) != 0){
		fprintf(stdout, "*** ERROR: Delete fails\n");
		return false;
	}

	if (readKey(g_key, g_value1, g_opaque1, false, false) == 0) {
		fprintf(stdout, "*** ERROR: Delete did not delete key\n");
		return false;
	}

	g_nSuccesses++;
	return true;
}

static bool
GATTest()
{
	g_nTests++;
	if (writeKey(g_key, "GAT_test", g_opaque1, 3600, 0) != 0) {
		fprintf(stdout, "*** ERROR: GAT - could not write key\n");
		return false;
	}

	if (readKey(g_key, "GAT_test", g_opaque2, false, true) != 0) {
		fprintf(stdout, "*** ERROR: GAT fails\n");
		return false;
	}

	g_nSuccesses++;
	return true;
}

/*static void
stupidTest()
{
	writeKey(g_key2, g_value2, 2, 3600, 0);
//	writeKey(g_key3, g_value3, 3, 3600, 0);
	sleep(1);
	readKey(g_key2, g_value2, 4, false, false);
//	readKeyQ(g_key3, g_value3, 5, false);
	doNOOP(10);
	getAndValidate(MC_OK, 2, NULL, true);
//	getAndValidate(MC_OK, 3, NULL, true);

	getAndValidate(MC_OK, 4, g_value2, true);
//	getAndValidate(MC_OK, 5, g_value3, true);

}*/

static bool
appendTest()
{
	char *key = "appendKey";
	deleteKey(key, 1);


	// first - standard string append
	writeKey(key, "string1", 1, 3600, 0);
	appendKey(key, "tail", 1, 0);
	prependKey(key, "head", 1, 0);
	if (readKey(key, "headstring1tail", 1, false, false) != 0) {
		fprintf(stdout, "append test fails - string append fails\n");
		return false;
	}

	// now let's append two numbers
	/*deleteKey(key,1);
	writeKey(key, "1", 1, 3600, 0);
	appendKey(key, "2", 1, 0);
	if (incrementKey(key, 3, 1, 0, 3600, 0, 15) != 0) {
		fprintf(stdout, "append test fails - numeric increment after append fails\n");
		return false;
	}

	// and append a string
	prependKey(key, "non-numeric", 1, 0);
	if (readKey(key, "non-numeric15", 1, false, false) != 0) {
		fprintf(stdout, "append test fails - numeric append fails\n");
		return false;
	}*/

	deleteKey(key, 1);
	return true;
}

static bool
quietTest()
{
	const char *dne = "I dont exist";
	g_nTests++;

	// write three keys
	writeKeyQ(g_key,  g_value1, 1, 3600, 0);
	writeKeyQ(g_key2, g_value2, 2, 3600, 0);
	writeKeyQ(g_key3, g_value3, 3, 3600, 0);
	writeKeyQ(g_key,  g_value1, 4, 3600, 0);
	writeKeyQ(g_key,  g_value2, 5, 3600, 1); // this one should fail

	// wait a moment - serialization not currently guaranteed
//	sleep(1);

	// get one back
	readKeyQ(g_key, g_value1, 6, false);

	// read a non-existent value
	readKeyQ(dne, g_value2, 7, false);

	// read the keys
	readKeyQ(g_key, g_value1, 8, false);
	readKeyQ(g_key2, g_value2, 9, false);
	// read a non-existent value
	readKeyQ(dne, g_value2, 10, false);
	// and another key
	readKeyQ(g_key3, g_value3, 11, false);

	sleep(1);
	// and the nop
	doNOOP(12);

	// now get the results back. Should be - 5 (error), 6, 7(error), 8, 9, 10 (error),11
	// success responses from 1, 2, and 3, then values from 1, 1, 2, and 3.
	if (0 != getAndValidate(MC_KEY_EXISTS, 5, NULL, true)){
		fprintf(stdout, " Queue Test: First result is not first error\n");
		return false;
	}

	if (0 != getAndValidate(MC_OK, 6, g_value1, true)){
		fprintf(stdout, " Queue Test: Second result is not first get\n");
		return false;
	}

	/*
	// Actually, according to the official memcache suite, this not supposed to
	// return anything.
	if (0 != getAndValidate(MC_KEY_NOT_FOUND, 7, NULL, true)){
		fprintf(stdout, " Queue Test: Third result bad\n");
		return false;
	}
    */

	if (0 != getAndValidate(MC_OK, 8, g_value1, true)){
		fprintf(stdout, " Queue Test: Fourth result bad\n");
		return false;
	}

	if (0 != getAndValidate(MC_OK, 9, g_value2, true)){
		fprintf(stdout, " Queue Test: Fifth result bad\n");
		return false;
	}

	/* again, errors do not return in this mode
	if (0 != getAndValidate(MC_KEY_NOT_FOUND, 10, NULL, true)){
		fprintf(stdout, " Queue Test: Sixth result bad\n");
		return false;
	}
	*/

	if (0 != getAndValidate(MC_OK, 11, g_value3, true)){
		fprintf(stdout, " Queue Test: Seventh result bad\n");
		return false;
	}

	g_nSuccesses++;
	return true;
}

static void
latencyTest(){
	// single threaded test of latency against one key... simplest possible test...
	writeKey(g_latency_key,g_latency_val,12,6000000, 0);

	uint32_t n_lt_one = 0;
	uint32_t n_lt_two = 0;
	uint32_t n_lt_four = 0;
	uint32_t n_lt_eight = 0;
	uint32_t n_gt_eight = 0;
	// read key back
	for (int i=0; i<20000; i++) {
		struct timespec ts_now;
		clock_gettime( CLOCK_MONOTONIC, &ts_now);
		uint32_t now = (uint32_t)((((uint64_t)ts_now.tv_sec) * 10000) + (ts_now.tv_nsec/100000));
		readKey(g_latency_key,g_latency_val,i,6000000, 0);
		clock_gettime( CLOCK_MONOTONIC, &ts_now);
		uint32_t later = (uint32_t)((((uint64_t)ts_now.tv_sec) * 10000) + (ts_now.tv_nsec/100000)); // yes, this is inefficient. I don't care
		uint32_t delta = later-now;
		//if(i%5==0)
		if (delta < 10) {
			n_lt_one++;
		} else if (delta < 20) {
			n_lt_two++;
		} else if (delta < 40) {
			n_lt_four++;
		} else if (delta < 80) {
			n_lt_eight++;
		} else {
			n_gt_eight++;
		}
	}
	printf("Read Latency: <1: %d, <2: %d, <4 %d, <8 %d, >8 %d\n", n_lt_one, n_lt_two, n_lt_four, n_lt_eight, n_gt_eight);
        deleteKey(g_latency_key,12);

}

static
void *test_thread(void* udata)
{
	sleep(3); //sleep a couple of seconds - just make sure the cluster has 'settled'

	g_nTests = 0;
	g_nSuccesses = 0;

	// first, let's just delete the keys we're using
	deleteKey(g_key, g_opaque1);
	deleteKey(g_key2, g_opaque1);
	deleteKey(g_key3, g_opaque1);

	if (writeReadBackTest()) {
		fprintf(stdout, "* Write and read back test succeeds\n");
	}

	if (deleteTest()) {
		fprintf(stdout, "* Delete test succeeds\n");
	}

	if (addUniqueTest()) {
		fprintf(stdout, "* Add unique test succeeds\n");
	}

	/*if (incrementTest()) {
		fprintf(stdout, "* Increment test succeeds\n");
	}*/

	// XXX - same thing with the decrements?

	if (getKTest()) {
		fprintf(stdout, "* GetK test succeeds\n");
	}

	if (CASTest()) {
		fprintf(stdout, "* CAS test succeeds\n");
	}

	if (GATTest()) {
		fprintf(stdout, "* GAT test succeeds\n");
	}

	if (appendTest()) {
		fprintf(stdout, "* Append test succeeds\n");
	}

	//stupidTest();

	if (quietTest()) {
		fprintf(stdout, "* Quiet (queued) test succeeds\n");
	}

	//latencyTest();

	fprintf(stdout, "%d of %d tests pass\n", g_nSuccesses, g_nTests);
	return NULL;
}

int run_tests(void *transport, cl_transport_type type)
{
	printf("*** Running Memcache Tests ***\n");
	if (0 != init_test(transport, type)){
		return -1;
	}
	pthread_t rw_thread;
	pthread_create(&rw_thread,NULL, test_thread, NULL);
	pthread_join(rw_thread,NULL);
	return 0;
}

static void
printUsage()
{
	fprintf(stdout, "Usage: proxy_test -h <hostname> -p <port> -s <local_socket_name>\n");
	fprintf(stdout, "  If you specify both a host/port and a local socket, the program\n ");
	fprintf(stdout, "  will use the local socket\n");
}

int
main(int argc, char **argv)
{
	// because we have to write our own htonll, initialize
	// endian-ness.
	init_endian();

	int c;

	char *hostname = NULL;
	char *portStr  = NULL;
	uint16_t port;
	char *socketName = NULL;

	while ((c = getopt(argc, argv, "h:p:s:")) != -1){
		switch (c) {
		case 'h':
			hostname = strdup(optarg);
			break;
		case 'p':
			portStr  = strdup(optarg);
			break;
		case 's':
			socketName = strdup(optarg);
			break;
		}
	}

	if (portStr) {
		port = atoi(portStr);
		if (port <= 0) {
			fprintf(stdout, "Port value must be numeric\n");
			exit(-1);
		}
	}
	if (portStr && !hostname) {
		fprintf(stdout, "Hostname not specified, will use localhost\n");
		hostname = strdup("localhost");
	}

	if (!portStr && !hostname && !socketName) {
		printUsage();
		exit(-1);
	}

	cl_transport_type type = socketName?TRANSPORT_UNIX_SOCKET:TRANSPORT_TCP_SOCKET;
	if (type == TRANSPORT_TCP_SOCKET) {
		cl_TCP_transport transport;
		transport.hostname = hostname;
		transport.port     = port;
		transport.sockfd   = -1;
		run_tests(&transport, type);
	}else{
		cl_unix_socket_transport transport;
		transport.name   = socketName;
		transport.sockfd = -1;
		run_tests(&transport, type);
	}

}


// tests I have not yet run:
// expiration test
// decrement
// increment on non-numeric value
// GAT
// touch
// version
// quit
// non-supported commands: append, prepend, replace, stat, verbosity(?)

// and would like to be able to run these tests against an actual memcache server...






