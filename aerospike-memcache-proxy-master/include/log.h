/*
 * log.h
 *
 *  Created on: Apr 4, 2012
 *      Author: CSW
 *
 *      Copyright Citrusleaf, 2012
 */

#ifndef LOG_H_
#define LOG_H_

typedef enum {
	CL_CRITICAL = 0,
	CL_WARNING  = 1,
	CL_INFO     = 2,
	CL_DEBUG    = 3,
	CL_UNDEF    = 4,
}cl_event_severity;


#define cl_assert(a, __msg, ...) ((void)((a) ? (void)0 : cl_log_event( CL_CRITICAL, __func__, __LINE__, (__msg), ##__VA_ARGS__)))
#define cl_crash(   __msg, ...) (cl_log_event(CL_CRITICAL, __FILE__, __LINE__, (__msg), ##__VA_ARGS__))
#define cl_warning( __msg, ...) (cl_log_event(CL_WARNING,  __FILE__, __LINE__, (__msg), ##__VA_ARGS__))
#define cl_info(    __msg, ...) (cl_log_event(CL_INFO,     __FILE__, __LINE__, (__msg), ##__VA_ARGS__))
#define cl_debug(   __msg, ...) (cl_log_event(CL_DEBUG,    __FILE__, __LINE__, (__msg), ##__VA_ARGS__))

bool cl_log_setup(char *path, cl_event_severity level);
void cl_log_event(const cl_event_severity severity, const char *filename, const int line, char *msg, ...);
void cl_log_init();
void cl_log_shutdown();
int cl_log_get_fd();


#endif /* LOG_H_ */
