#pragma once
#define _GNU_SOURCE
#include <fuse.h>
#include <errno.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include "../debug_defs.h"
#include "../cachedfs.h"

extern FILE *logfile;

typedef struct {
    uint32_t flags;
    union {
        blockcache_record *cache;
        int fd;
        char *stat;
    } rec;
} fileinfo_t;

extern DIR *mount_dir;
extern cachedfs_info *cachedfs;
extern int errno;

#define STATS_INFO 1
#define BYTES_INFO 2
#define FILES_INFO 3
#define HIWM_INFO 4
#define LOWM_INFO 5
#define BLKSZ_INFO 6
#define USAGE_INFO 7

#define PEEK_TYPE 1
#define PASS_TYPE 2
#define INFO_TYPE 3

static inline int get_info_type (const char *path) {
    if (strlen(path) != 5) {
        return 0;
    }

    char f = *path++;
    uint32_t s = ((uint32_t*)path)[0];
    
    if (f == 'b') { /* bytes, blksz */
        if (s == *((uint32_t*)"ytes")) return BYTES_INFO;
        if (s == *((uint32_t*)"lksz")) return BLKSZ_INFO;
    } else if (f == 'f') { /* files */
        if (s == *((uint32_t*)"iles")) return FILES_INFO;
    } else if (f == 'h') { /* hi_wm */
        if (s == *((uint32_t*)"i_wm")) return HIWM_INFO;
    } else if (f == 'l') { /* lo_wm */
        if (s == *((uint32_t*)"o_wm")) return LOWM_INFO;
    } else if (f == 's') { /* stats */
        if (s == *((uint32_t*)"tats")) return STATS_INFO;
    } else if (f == 'u') { /* usage */
        if (s == *((uint32_t*)"sage")) return USAGE_INFO;
    }

    return 0;

}

static inline int get_magic_dir_type (const char **pathref) {
    const char *path = *pathref;
    if (strnlen(path, 6) < 6) return 0;
    if (*((uint16_t*)path) != *((uint16_t*)"##")) return 0;
    if (path[6] != '\0' && path[6] != '/') return 0;

    uint32_t name = *((uint32_t*)(path+2));

    *pathref += 6;

    if (name == *((uint32_t*)"peek")) return PEEK_TYPE;
    if (name == *((uint32_t*)"pass")) return PASS_TYPE;
    if (name == *((uint32_t*)"info")) return INFO_TYPE;

    *pathref -= 6;

    return 0;
}

