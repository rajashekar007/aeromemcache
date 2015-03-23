/*
 * log.c
 *
 *  Created on: Apr 4, 2012
 *      Author: CSW
 *      Copyright 2012, Citrusleaf Inc
 *
 *  This is pretty much a simpler version of the fault code in the server.
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdbool.h>
#include <string.h>
#include <execinfo.h>
#include <stdarg.h>

#include "log.h"

#define cf_strdup strdup
#define cf_malloc malloc
#define cf_free free
#define cf_calloc calloc


typedef struct {
	int fd;
	char *path;
	int limit;
}cl_log_sink;

#ifndef NULL
#define NULL (void *)0;
#endif

static const char *event_severity_strings[] = {"CRITICAL", "WARNING", "INFO", "DEBUG", NULL};

#define CL_LOG_BACKTRACE_DEPTH 16

static cl_log_sink g_sink;

bool
cl_log_setup(char *path, cl_event_severity level)
{
	if (g_sink.fd > 0 && g_sink.fd != STDOUT_FILENO && g_sink.fd != STDERR_FILENO)
		close(g_sink.fd);
	if (g_sink.path) {
		 free(g_sink.path);
	}

	g_sink.path = cf_strdup(path);

	if (0 == strncmp(path, "stderr", 6))
		g_sink.fd = STDERR_FILENO;
	else if ( 0 == strncmp(path, "stdout", 6))
		g_sink.fd = STDOUT_FILENO;
	else {
		g_sink.fd = open(path, O_WRONLY|O_CREAT|O_APPEND|O_NONBLOCK, S_IRUSR|S_IWUSR);
		if (g_sink.fd < 0){
			return false;
		}
		g_sink.limit = level;
	}

	return true;
}

void
cl_log_event(const cl_event_severity severity, const char *filename, const int line, char *msg, ...)
{
	if (severity > g_sink.limit)
		return;

	va_list argp;
	char mbuf[1024];
	time_t now;
	struct tm nowtm;
	void *bt[CL_LOG_BACKTRACE_DEPTH];
	char **btstr;
	int btn;

	// set timestamp.
	now = time(NULL);
	gmtime_r(&now, &nowtm);
	size_t pos = strftime(mbuf, sizeof(mbuf), "%b %d %Y %T %Z: ", &nowtm);

	// set the severity tag
	pos += snprintf(mbuf + pos, sizeof(mbuf) - pos, "%s", event_severity_strings[severity]);

	// set the location
	if (filename){
		pos += snprintf(mbuf + pos, sizeof(mbuf) - pos, "(%s:%d) ", filename, line);
	}

	// append message
	va_start(argp, msg);
	pos += vsnprintf(mbuf + pos, sizeof(mbuf) - pos, msg, argp);
	va_end(argp);
	pos += snprintf(mbuf + pos, sizeof(mbuf) - pos, "\n");

	// write
	write(g_sink.fd, mbuf, pos);

	if (CL_CRITICAL == severity) {
		fflush(NULL);

		int wb = 0;

		btn = backtrace(bt, CL_LOG_BACKTRACE_DEPTH);
		btstr = backtrace_symbols(bt, btn);
		if (!btstr) {
			char *no_bkstr = " --- NO BACKTRACE AVAILABLE --- \n";
			wb += write(g_sink.fd, no_bkstr, strlen(no_bkstr));
		}
		else {
			for (int j=0; j < btn; j++) {
				char line[60];
				sprintf(line, "critical error: backtrace: frame %d ",j);
				wb += write(g_sink.fd, line, strlen(line));
				wb += write(g_sink.fd, btstr[j], strlen(btstr[j]));
				wb += write(g_sink.fd, "\n", 1);
			}
		}

		abort();
	}

	return;
}

void
cl_log_init()
{

	/* Initialize the fault filter while we're here */
	memset(&g_sink, 0, sizeof(g_sink));
	g_sink.fd = STDOUT_FILENO;
    g_sink.limit = CL_INFO;

	return;
}

void
cl_log_shutdown()
{
	if (g_sink.fd > 0 && g_sink.fd != STDOUT_FILENO && g_sink.fd != STDERR_FILENO)
		close(g_sink.fd);
	if( g_sink.path)
		cf_free(g_sink.path);
}

int
cl_log_get_fd()
{
	return g_sink.fd;
}


