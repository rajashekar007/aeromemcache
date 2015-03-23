/*
 * Proxy for Citrusleaf client <-> server communication.
 * Speaks directly to a Citrusleaf cluster or clusters on one side,
 * and talks a variety of protocols to applications on the other side.
 * (Where 'variety' is memcache, at least for the moment)
 *
 * Copyright 2012 Citrusleaf Inc
 *
 * CSW
 */
 
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <signal.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>
#include <libxml2/libxml/xmlexports.h>
#include <libxml2/libxml/xmlstring.h>
#include <libxml2/libxml/tree.h>
 
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cf_ll.h>
#include <citrusleaf/cf_service.h>

#include <proxy.h>
#include <memcache.h>
#include <xmlUtils.h>
#include <reactor.h>
#include <log.h>

// NB - for some reason, the compiler really hates
// the libxml2 parser.h file. I'm just going to extern the
// few functions that I need, rather than fighting the compiler
//#include <libxml2/libxml/parser.h>
extern xmlDoc *xmlReadFile(const char *filename, const char *stuff, int otherStuff);
extern void xmlInitParser();
extern void xmlCleanupParser();

static reactor *g_reactor = NULL; // available to connectors
reactor *get_global_reactor() { return g_reactor; }

// Proxy cluster:
// Object representing the citrusleaf cluster and
// the connectors associated with it.
typedef struct {
	cf_ll *serverList;     // seed servers for contacting the cluster
	cf_ll *connectorList;  // connectors for the proxy
	cl_cluster *cluster;   // standard cluster from client code
	char *name;
	cl_cluster_state state;
	int readTimeout;
	int writeTimeout;
}cl_proxy_cluster;

typedef struct {
	char *host;
	int   port;
}cl_server;
 
static cl_connector_type
getConnectorType(const char *name)
{
	if (!strcmp(name, "memcache") )
		return CONNECTOR_MEMCACHE;
	else
		return CONNECTOR_UNKNOWN;
}
 
typedef struct {
	cf_ll_element *next;
	cf_ll_element *prev;
	cl_server server;
}cl_server_list_elem;
 
typedef struct {
	cf_ll_element *next;
	cf_ll_element *prev;
	cl_proxy_connector  *connector;
}cl_connector_list_elem;
 
static void
serverListDtor(cf_ll_element *e)
{
	cl_server_list_elem *server_elem = (cl_server_list_elem *)e;
	if (server_elem->server.host) {
		cf_free(server_elem->server.host);
	}
	cf_free(e);
}
 
static void
connectorListDtor(cf_ll_element *e)
{
	cl_proxy_connector *connector = ((cl_connector_list_elem *)e)->connector;
	if (connector->dtor) {
		connector->dtor((struct cl_proxy_connector_s *)connector);
	}
	cf_free(((cl_connector_list_elem *)e)->connector);
	cf_free(e);
}

typedef struct {
	cf_ll_element *next;
	cf_ll_element *prev;
	cl_proxy_cluster *cluster;
}cl_cluster_list_element;

// and some forward references
static void cluster_Destroy(cl_proxy_cluster *cluster);

static void
clusterListDtor(cf_ll_element *e)
{
	cl_cluster_list_element *list_elem = (cl_cluster_list_element *)e;
	cluster_Destroy(list_elem->cluster);
	cf_free(list_elem);
}

static cf_ll *g_clusterList = NULL;
static int g_readTimeout  = 0;
static int g_writeTimeout = 0;


// Factory for creating proxy connectors
static cl_proxy_connector *
createConnector(xmlNode *connectorNode, cl_cluster *cluster, int readTimeout, int writeTimeout)
{
	cl_proxy_connector *newConnector = NULL;
	xmlNode *protocolNode = xmlNodeGetFirstChild(connectorNode, (xmlChar*)"type");
	if (!protocolNode)
		return NULL;

	const xmlChar *protocol = protocolNode->children->content;
	cl_connector_type protocol_type = getConnectorType((const char *)protocol);
	if (protocol_type == CONNECTOR_MEMCACHE) {
		newConnector = (cl_proxy_connector *)memcacheConnectorCreate(connectorNode, cluster, readTimeout, writeTimeout );
	}
 	 
	return newConnector;
}
 
static void
cluster_Destroy(cl_proxy_cluster *cluster)
{
	if (cluster->name) {
		cf_free(cluster->name);
	}

	if (cluster->serverList) {
		util_deleteList(cluster->serverList);
	}

	if (cluster->connectorList) {
		util_deleteList(cluster->connectorList);
	}

	if (cluster->cluster) {
		citrusleaf_cluster_destroy(cluster->cluster);
	}

	cf_free(cluster);
}
 
static void
cluster_addServer(cl_proxy_cluster *cluster, const char *host, int port)
{
	cl_server_list_elem *serverElem = (cl_server_list_elem *)cf_malloc(sizeof(cl_server_list_elem));
	serverElem->server.host = cf_strdup(host);
	serverElem->server.port = port;
	cf_ll_append(cluster->serverList, (cf_ll_element *)serverElem);

	if (cluster->cluster) {
		citrusleaf_cluster_add_host(cluster->cluster, serverElem->server.host, serverElem->server.port, 0);
	}
}

static void
cluster_addConnector(cl_proxy_cluster *cluster, cl_proxy_connector *connector)
{
	cl_connector_list_elem *connectorElem = (cl_connector_list_elem *)cf_malloc(sizeof(cl_connector_list_elem));
	connectorElem->connector = connector;
	cf_ll_append(cluster->connectorList, (cf_ll_element *)connectorElem);
}
 
static int
cluster_getNumServers(cl_proxy_cluster *cluster)
{
	return cf_ll_size(cluster->serverList);
}
 
// Create cluster object - contains citrusleaf server cluster
// and the connectors that talk to it.
static cl_proxy_cluster *
createCluster(xmlNode *node)
{
	bool rv = true;

	// Allocate structures for new cluster
	cl_proxy_cluster *cluster = (cl_proxy_cluster *)cf_malloc(sizeof(cl_proxy_cluster));
	cluster->name = NULL;
	cluster->serverList    = (cf_ll *)cf_malloc(sizeof(cf_ll));
	cluster->connectorList = (cf_ll *)cf_malloc(sizeof(cf_ll));
	cf_ll_init(cluster->serverList, serverListDtor, false);
	cf_ll_init(cluster->connectorList, connectorListDtor, false);

	// Create the cluster as represented by the citrusleaf client
	cluster->cluster = citrusleaf_cluster_create();

	// If there's a name associated with the cluster, set that
	xmlNode *nameNode = xmlNodeGetFirstChild(node, (xmlChar *)"name");
	if (nameNode && nameNode->children && nameNode->children->type == XML_TEXT_NODE) {
		cluster->name = cf_strdup((char *)nameNode->children->content);
	}

	// Set the timeouts
	xmlNode *timeoutNode;
	timeoutNode = xmlNodeGetFirstChild(node, (xmlChar *)"readTimeout");
	if (timeoutNode && timeoutNode->children && timeoutNode->children->type == XML_TEXT_NODE
			&& (atoi((char *)(timeoutNode->children->content)) >= 0) ) {
		cluster->readTimeout = atoi((char *)(timeoutNode->children->content));
	}else{
		cluster->readTimeout = g_readTimeout;
	}

	timeoutNode = xmlNodeGetFirstChild(node, (xmlChar *)"writeTimeout");
	if (timeoutNode && timeoutNode->children && timeoutNode->children->type == XML_TEXT_NODE
			&& (atoi((char *)(timeoutNode->children->content)) >= 0) ) {
		cluster->writeTimeout = atoi((char *)(timeoutNode->children->content));
	}else{
		cluster->writeTimeout = g_readTimeout;
	}

	// Get the servers associated with the cluster
	xmlNode *serverNode = xmlNodeGetFirstChild(node, (xmlChar *)"seed");
	while (serverNode) {
		xmlChar *host;
		xmlChar *portStr;
		int port;
		host = xmlGetProp(serverNode, (xmlChar *)"host");
		portStr = xmlGetProp(serverNode, (xmlChar *)"port");
		if (!host) {
			cl_warning("server hostname not specified");
			serverNode = xmlNodeGetNextChild(serverNode);
			continue;
		}
		if (!portStr) {
			cl_warning("server port not specified");
			serverNode = xmlNodeGetNextChild(serverNode);
			continue;
		}
		 
		port = atoi((char*)portStr);
		cf_free(portStr);

		if (port <= 0 ){
			cl_warning("port value %s is invalid", portStr);
			serverNode = xmlNodeGetNextChild(serverNode);
			continue;
		}
		cluster_addServer(cluster, (char *)host, port);
		cf_free(host);
		serverNode = xmlNodeGetNextChild(serverNode);

	}


	if (cluster_getNumServers(cluster) == 0) {
		cl_warning("No valid servers in the cluster");
		rv = false;
		goto Exit;
	}

	// last, create the connector(s) for this cluster
	xmlNode *connectorNode = xmlNodeGetFirstChild(node, (xmlChar *)"connector");
	while (connectorNode) {
		cl_proxy_connector *connector = createConnector(connectorNode, cluster->cluster,
				cluster->readTimeout, cluster->writeTimeout);
		if (connector) {
 	 		cluster_addConnector(cluster, connector);
		}
		connectorNode = xmlNodeGetNextChild(connectorNode);
	}

	cluster->state = CLUSTER_STATE_UNINITIALIZED;

Exit:
	if ( (rv == false) && cluster) {
		cluster_Destroy(cluster);
		cluster = NULL;
	}
	return cluster;
}

static void
setGlobalEnvironment(xmlNode *node)
{
	// First things first - set up the file to log to
	xmlNode *logNode;
	logNode = xmlNodeGetFirstChild(node, (xmlChar *)"log");
	if (logNode){
		cl_event_severity severity = CL_INFO;
		char *logFile = (char *)xmlGetProp(logNode, (xmlChar *)"filename");
		char *level   = (char *)xmlGetProp(logNode, (xmlChar *)"level");
		if (level){
			if (!strcasecmp(level, "info")){
				severity = CL_INFO;
			}else if (!strcasecmp(level, "critical")){
				severity = CL_CRITICAL;
			}else if (!strcasecmp(level, "warning")){
				severity = CL_WARNING;
			}else if (!strcasecmp(level, "debug")){
				severity = CL_DEBUG;
			}
		}
		cl_log_setup(logFile, severity);

		if (logFile)
			cf_free(logFile);
		if (level)
			cf_free(level);
	}

	// Set up timeouts

	xmlNode *timeoutNode;
	timeoutNode = xmlNodeGetFirstChild(node, (xmlChar *)"readTimeout");
	if (timeoutNode && timeoutNode->children && timeoutNode->children->type == XML_TEXT_NODE
			&& (atoi((char *)(timeoutNode->children->content)) >= 0) ) {
		g_readTimeout = atoi((char *)(timeoutNode->children->content));
	}else{
		g_readTimeout = 0; // XXX is this the right default timeout?
	}

	timeoutNode = xmlNodeGetFirstChild(node, (xmlChar *)"writeTimeout");
	if (timeoutNode && timeoutNode->children && timeoutNode->children->type == XML_TEXT_NODE
			&& (atoi((char *)(timeoutNode->children->content)) >= 0) ) {
		g_writeTimeout = atoi((char *)(timeoutNode->children->content));
	}else{
		g_writeTimeout = 0; // XXX is this the right default timeout?
	}

	// daemonize, if desired
	char *b_daemonize = (char *)xmlGetProp(node, (xmlChar *)"daemonize");
	if (b_daemonize) {
		if (!strcasecmp(b_daemonize, "true")) {
			int ignore_list;
			ignore_list = cl_log_get_fd();
			cf_process_daemonize("/tmp/cl_proxy_console", &ignore_list, 1);
		}else{
			if (strcasecmp(b_daemonize, "false")) {
				cl_warning("Unknown option for daemonization - ignoring");
			}
		}
		free(b_daemonize);
	}

	// set user and group, if desired
	int uid = getuid();
	struct passwd *pwd;
	char *user_name = (char *)xmlGetProp(node, (xmlChar *)"user");
	if (user_name) {
		if (NULL == (pwd = getpwnam(user_name))){
			cl_assert(false, "specified user not found in the system");
		}
		uid = pwd->pw_uid;
		endpwent();
		cf_free(user_name);
	}

	int gid = getgid();
	struct group *grp;
	char *grp_name = (char *)xmlGetProp(node, (xmlChar *)"group");
	if (grp_name) {
		if (NULL == (grp = getgrnam(grp_name))){
			cl_assert(false, "specified group not found in the system");
		}
		gid = grp->gr_gid;
		endgrent();
		cf_free(grp_name);
	}

	// switch user and group
	cf_process_privsep(uid, gid);

	// initialize cluster list
	g_clusterList = (cf_ll *)cf_malloc(sizeof(cf_ll));
	cf_ll_init(g_clusterList, clusterListDtor, false);

}

 /* Cluster monitor thread */
static int
cluster_Monitor(void *arg)
{
	// check on our little clusters...
	cl_cluster_list_element *cl_list_elem = (cl_cluster_list_element *)cf_ll_get_head(g_clusterList);
	while (cl_list_elem) {
		cl_proxy_cluster *cluster = cl_list_elem->cluster;
		int foo;
		switch (cluster->state){
		case CLUSTER_STATE_UNINITIALIZED:
			// set cluster servers!
			foo = 2; // NB - because the compiler has gone completely mental and won't let the next line succeed without this.
			cl_server_list_elem *server_elem;
			server_elem = (cl_server_list_elem *)cf_ll_get_head(cluster->serverList);
			while (server_elem){
				cl_rv rv = citrusleaf_cluster_add_host(cluster->cluster, server_elem->server.host, server_elem->server.port, 0);
				if (rv != CITRUSLEAF_OK) {
					cl_warning("Error adding host %s:%d", server_elem->server.host, server_elem->server.port);
				}
				server_elem = (cl_server_list_elem *)cf_ll_get_next((cf_ll_element *)server_elem);
			}
			cluster->state = CLUSTER_STATE_DISCONNECTED;
			break;
		case CLUSTER_STATE_DISCONNECTED:
			if (citrusleaf_cluster_settled(cluster->cluster)) {
				cluster->state = CLUSTER_STATE_CONNECTED; // XXX except of course that this doesn't work.
				cl_connector_list_elem *conn_elem = (cl_connector_list_elem *)cf_ll_get_head(cluster->connectorList);
				while (conn_elem) {
					if (conn_elem->connector->cb)
						conn_elem->connector->cb(CLUSTER_STATE_CONNECTED);
					conn_elem = (cl_connector_list_elem *)cf_ll_get_next((cf_ll_element *)conn_elem);
				}
			}
			break;
		case CLUSTER_STATE_CONNECTED:
			// check for problems
			// call callback if there are problems.. XXX TODO
			break;
		}
		cl_list_elem = (cl_cluster_list_element *)cf_ll_get_next((cf_ll_element*)cl_list_elem);
	}

	return 0;
}

static void
clusterMonitor_Register(cl_proxy_cluster *cluster)
{
	cl_cluster_list_element *cl_list_elem = (cl_cluster_list_element *)cf_malloc(sizeof(cl_cluster_list_element));
	cl_list_elem->cluster = cluster;

	cf_ll_append(g_clusterList,(cf_ll_element *)cl_list_elem);
}


static bool
parseConfig(char *filename)
{
	xmlDoc *doc = xmlReadFile(filename, NULL, 0);
	bool rv = true;

	if (!doc) {
		return false;
	}
 	 
	// validate against DTD... XXX TODO
	xmlNode *root = xmlDocGetRootElement(doc);
 	 
	// From the high level nodes, set up the default environment
	setGlobalEnvironment(root);

	// Now that we have the global environment, which includes the log file,
	// set up any system-wide resources (like the global reactor)
	cl_info("****** Citrusleaf Proxy Running ******");
	g_reactor = reactor_create();
 	 
	// From the cluster nodes, create cluster objects
	xmlNode *clusterNode = xmlNodeGetFirstChild(root, (xmlChar *)"cluster");
	while (clusterNode) {
		cl_proxy_cluster *cluster = createCluster(clusterNode);
		clusterMonitor_Register(cluster);
		clusterNode = xmlNodeGetNextChild(clusterNode);
	}
 	 
	if (doc)
		xmlFreeDoc(doc);
 	 
	return rv;
}
 	 

static pthread_mutex_t g_NONSTOP;

static void
handle_int(int sig_num)
{
	pthread_mutex_unlock(&g_NONSTOP);
}

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

int
main(int argc, char **argv)
{
	char *configFile = "cl_proxy.xml";

	// because we have to write our own htonll, initialize
	// endian-ness.
	init_endian();

	// set up logging...
	cl_log_init();

	while (1) {
		int option_idx = 0;
		int c;
		static struct option long_options[] = {
			{"config-file", 1, 0, 0},
			{0, 0, 0, 0}
		};

		c = getopt_long(argc, argv, "", long_options, &option_idx);
		if (c == 0) {
			switch (option_idx){
			case 0:
				configFile = cf_strdup(optarg);
				break;
			default:
				break;
			}
		}else{
			break;
		}
	}

	signal(SIGINT, handle_int);
	signal(SIGTERM, handle_int);
	signal(SIGPIPE, SIG_IGN);
 
	// and initialize the citrusleaf client
	citrusleaf_init();

	// now parse the configuration file
	xmlInitParser();
	parseConfig(configFile);
	xmlCleanupParser();

	/* Stop this thread from halting.  There are a few different approaches,
	 * most of which are absolutely no good.  Intentionally deadlocking on a
	 * mutex is actually remarkably efficient */
	pthread_mutex_init(&g_NONSTOP, NULL);
#ifdef RUN_TESTS
	sleep(20);
#else
	pthread_mutex_lock(&g_NONSTOP);
	pthread_mutex_lock(&g_NONSTOP);

	// When the service is running, you are here (deadlocked) - the signals that
	// stop the service (yes, these signals always occur in this thread) will
	// unlock the mutex, allowing us to continue.
	pthread_mutex_unlock(&g_NONSTOP);
#endif // RUN_TESTS
	pthread_mutex_destroy(&g_NONSTOP);

	util_deleteList(g_clusterList);

	cl_info("*** Shutting down Citrusleaf proxy ****");
	citrusleaf_shutdown();

	reactor_destroy(g_reactor);

	cl_log_shutdown();
}
 

 /*
 DTD 
 
 <!DOCTYPE citrusleafclientproxy[
 <ELEMENT citrusleafclientproxy (cluster+|log?|readTimeout?|writeTimeout?|nthreads?)>
 <ELEMENT cluster (seed+|connector+|name?|readTimeout?|writeTimeout?)>
 <ELEMENT seed EMPTY>
 <ELEMENT log EMPTY>
 <ELEMENT connector (type|version|transport|namespace?|set?)>
 <ELEMENT type #CDATA>
 <ELEMENT version #CDATA>
 <ELEMENT transport (type, #PCDATA?)>
 <ELEMENT namespace #CDATA>
 <ELEMENT readTimeout #CDATA>
 <ELEMENT writeTimeout #CDATA>
 <ELEMENT nthreads #CDATA>
 <ELEMENT name #CDATA>

 <!ATTLIST citrusleafclientproxy daemonize>
 <!ATTLIST citrusleafclientproxy user>
 <!ATTLIST citrusleafclientproxy group>
 <!ATTLIST log filename #REQUIRED>
 <!ATTLIST seed host "127.0.0.1">
 <!ATTLIST seed port #REQUIRED>
 ]>

 */


