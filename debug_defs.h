#pragma once

#ifdef _DEBUG_THREADS_
  #define __DBG_THREAD_STR__ " [%u] "
  #define __DBG_THREAD_PARAM__ pthread_self(),
#else
  #define __DBG_THREAD_STR__ 
  #define __DBG_THREAD_PARAM__ 
#endif

#define __LOGOUT_TO__(fh, format, ...) do { \
    fprintf(\
        fh, \
        __DBG_THREAD_STR__  __FILE__ "[%u]:\t" format "\n", \
        __DBG_THREAD_PARAM__ \
        __LINE__, \
        __VA_ARGS__ \
    ); fflush(fh); } while(0)


#ifdef _DEBUGGING_
  #include <stdio.h>

  #define __DEBUGGER__(type, format, ...) __LOGOUT_TO__(logfile, "DEBUG " type "\t" format, __VA_ARGS__)

  #define _debug(...) __DEBUGGER__("", __VA_ARGS__)
  #define _debugger(...) _debug(__VA_ARGS__)
    
  #ifdef _DEBUG_FLOW_
    #define _debug_flow(format, ...) __DEBUGGER__("Flow", format, __VA_ARGS__)
  #endif
  
  #ifdef _DEBUG_MORE_FLOW_
    #define _debug_more_flow(...) _debug_flow(__VA_ARGS__)
  #endif
    
  #ifdef _DEBUG_POINTERS_
    #define _debug_pointers(format, ...) __DEBUGGER__("Pointers", format, __VA_ARGS__)
  #endif
    
  #ifdef _DEBUG_LOCKS_
    #define _debug_locks(format, ...) __DEBUGGER__("Locks", format, __VA_ARGS__)
  #endif
  
  #ifdef _DEBUG_FAILURES_
    #define _debug_failure(format, ...) __DEBUGGER__("Failure", format, __VA_ARGS__)
  #endif

  #ifdef _DEBUG_VERBOSE_
    #define _debug_verbose(format, ...) __DEBUGGER__("Verbose", format, __VA_ARGS__)
  #endif
#endif

#define _show_error(format, ...) do { \
    __LOGOUT_TO__(stderr, "ERROR: " format, __VA_ARGS__); \
    __DEBUGGER__("ERROR", format, __VA_ARGS__); } while (0)

#ifndef __DEBUGGER__
  #define __DEBUGGER__(...)
#endif

#ifndef _debug
  #define _debug(...)  
#endif

#ifndef _debugger
  #define _debugger(...) 
#endif

#ifndef _debug_flow
  #define _debug_flow(...) 
#endif

#ifndef _debug_more_flow
  #define _debug_more_flow(...) 
#endif

#ifndef _debug_pointers
  #define _debug_pointers(...)
#endif

#ifndef _debug_locks
  #define _debug_locks(...)
#endif

#ifndef _debug_failure
  #define _debug_failure(...)
#endif

#ifndef _debug_verbose
  #define _debug_verbose(...) 
#endif

