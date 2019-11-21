#include "main.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cachedfs.h"

//char *cache_path = NULL;
//char *source_path = NULL;

//unsigned long default_block_size = DEFAULT_BLOCK_SIZE;

//unsigned long low_watermark = 0; //DEFAULT_LOW_WATERMARK;
//unsigned long high_watermark = 0; //DEFAULT_HIGH_WATERMARK;

//DIR *source_dir = NULL;
//DIR *cache_dir = NULL;
DIR *mount_dir;
FILE *logfile;

cachedfs_info *cachedfs;

void *blockcache_sync_loop (void *cfs);
pthread_t blockcache_sync_thread;
pthread_attr_t thread_attr;

void *cachedfs_init_sync_loop () {
    pthread_attr_init(&thread_attr);
    pthread_create(&blockcache_sync_thread, &thread_attr, blockcache_sync_loop, (void*)cachedfs);
    return NULL;
}

void cachedfs_destroy (void *data) {
    void *ret;
    pthread_cancel(blockcache_sync_thread);
    pthread_join(blockcache_sync_thread, &ret);
    write_all_cachedfs_changes(cachedfs);
    return;
}


/*************************************************************/

static struct fuse_operations hook_oper = {
    /* Construct/Destruct Functions */
//    void *(*init) (struct fuse_conn_info *conn);
//    void (*destroy) (void *);

    /* the init method here is for starting the sync loop */
    .init   = cachedfs_init_sync_loop,
    /* Attribute functions */
    .getattr= hook_getattr,
    .fgetattr=hook_fgetattr,
    .chmod  = hook_chmod,
    .chown  = hook_chown,
    //.utime = hook_utimes,
//    int (*statfs) (const char *, struct statvfs *);

    /* Dir functions */
    .readdir= hook_readdir,
    .mkdir  = hook_mkdir,
    .rmdir  = hook_rmdir,
//    int (*opendir) (const char *, struct fuse_file_info *);
//    int (*releasedir) (const char *, struct fuse_file_info *);
//    int (*fsyncdir) (const char *, int, struct fuse_file_info *);

    /* File functions */
    .open   = hook_open,
    .release= hook_release,

    .create = hook_create,
    .mknod  = hook_mknod,

    .unlink = hook_unlink,
    .rename = hook_rename,

    .read   = hook_read,
    .write  = hook_write,

    .statfs = hook_statfs,

    .truncate=hook_truncate,
    .ftruncate=hook_ftruncate,

    //.lock   =hook_lock,
//    int (*fsync) (const char *, int, struct fuse_file_info *);
//    int (*flush) (const char *, struct fuse_file_info *);
//    int (*access) (const char *, int);

    /* xAttribute Functions */
//    int (*setxattr) (const char *, const char *, const char *, size_t, int);
//    int (*getxattr) (const char *, const char *, char *, size_t);
//    int (*listxattr) (const char *, char *, size_t);
//    int (*removexattr) (const char *, const char *);

    /* Link functions */
    .readlink=hook_readlink,
    .symlink= hook_symlink,
    .link   = hook_link

    /* Advanced device mapping function */
//    int (*bmap) (const char *, size_t blocksize, uint64_t *idx);

};

/*************************************************************/

void usage(int argc, char *argv[]) {
    printf(
        "USAGE: %s -source=SOURCE_PATH -cache=CACHE_PATH [-lowwm=LOW_WATERMARK] [-highwm=HIGH_WATERMARK] [-bs=BLOCK_SIZE] [more mount options...]\n"
        "         SOURCE_PATH: Path to directory being cached\n"
        "          CACHE_PATH: Path to mountpoint / local cache\n"
        "       LOW_WATERMARK: Size (in MB) to trim cache to.\n"
        "      HIGH_WATERMARK: Max cache size (in MB). When cache size reaches\n"
        "                      this size it begins trimming data from the cache\n"
        "                      until it reaches the LOW_WATERMARK.\n"
        "          BLOCK_SIZE: caching block size.\n\n", argv[0]);
    #ifdef DEBUG
        int i;
        printf("%d params:\n", argc);
        for (i = 0; i < argc; i++) 
            printf("param %d: '%s'\n", i, argv[i]);
    #endif
    return;
}

/*************************************************************/

void *blockcache_sync_loop (void *cfs) {
    _debug_flow("Cachefs sync loop entered", 0);
    int inval;
    resync:
        sleep(30);

        _debug_flow("...syncing cachedfs changes", 0);
        write_all_cachedfs_changes((cachedfs_info*)cfs);

        _debug_flow("...checking usage", 0);
        cachedfs_check_usage((cachedfs_info*)cfs);
        
        _debug_flow("...validating records", 0);
        inval = validate_records((cachedfs_info*)cfs);
        if (inval) {
            _show_error("NOTE: %d records invalidated.", inval);
        }

        goto resync;
}

int main(int argc, char *argv[]) {
    logfile = fopen(LOGFILE, "a");

    //verify number of arguments
    if (argc < 3) {
        usage(argc, argv);
        return 1;
    }

    /************************************************************************
     * SYNOPSYS:
     *  cachedfs -source=SOURCE_PATH -cache=CACHE_PATH [-lowwm=LOW_WATERMARK] [-highwm=HIGH_WATERMARK] [-bs=BLOCK_SIZE] [other mount options]
     *
     *          CACHE_PATH: Path to mountpoint / local cache
     *         SOURCE_PATH: Path to directory being cached
     *       LOW_WATERMARK: Size (in bytes) to trim cache to.
     *      HIGH_WATERMARK: Max cache size (in bytes). When cache size reaches
     *                      this size it begins trimming data from the cache
     *                      until it reaches the LOW_WATERMARK.
     *          BLOCK_SIZE: caching block size.
     */

    //ARGS
    // - 0 - Executable name
    // - 1 - cache_path / mount path
    // - 2 - source_path
    // - 3 - low_watermark  (MB)
    // - 4 - high_watermark (MB)


    char *fuse_argv[20];
    fuse_argv[0] = argv[0];

    char *src = NULL, *mnt = NULL, *lwm = NULL, *hwm = NULL, *bs = NULL;
    char *tmp;
    int i = 1, fuse_argc = 2;
    while (i < argc) {
        tmp = argv[i];
        if (tmp[0] != '-') {
            fprintf(stderr, "ERROR: malformed parameter list!\n");
            usage(argc, argv);
            return 1;
        }
        if (tmp[1] == 's' && !strncmp(tmp+2, "ource=", 6)){
            src = tmp + 8;
        }else if (tmp[1] == 'c' && !strncmp(tmp+2, "ache=", 5)){
            mnt = tmp + 7;
        }else if (tmp[1] == 'l' && !strncmp(tmp+2, "owwm=", 5)){
            lwm =  tmp + 7;
        }else if (tmp[1] == 'h' && !strncmp(tmp+2, "ighwm=", 6)){
            hwm = tmp + 8;
        }else if (tmp[1] == 'b' && tmp[2] == 's' && tmp[3] == '='){
            bs = tmp + 4;
        }else
            fuse_argv[fuse_argc++] = argv[i];

        i++;
        continue;
    }

    fuse_argv[1] = mnt;
    fuse_argv[fuse_argc++] = "-ononempty";
    fuse_argv[fuse_argc++] = "-oallow_other";

    cachedfs = init_cachedfs(mnt, src);

    if (lwm) cachedfs->low_watermark = atoll(lwm)*1024*1024;
    if (hwm) cachedfs->high_watermark = atoll(hwm)*1024*1024;
    if (bs) cachedfs->default_block_size = atoi(bs);

    if (cachedfs->low_watermark >= cachedfs->high_watermark) {
        fprintf(stderr, "ERROR: Low watermark (%d) >= High watermark (%d)\n", cachedfs->low_watermark, cachedfs->high_watermark);
        usage(argc, argv);
        return 2;
    }

    mount_dir = cachedfs->cache_dir;

    fprintf(stderr, 
        "Params:\n"
        "\tSource: '%s'\n"
        "\t Cache: '%s'\n"
        "\tWatermarks\n"
        "\t\tHigh: %lld\n"
        "\t\t Low: %lld\n"
        "\n", src, mnt, cachedfs->high_watermark, cachedfs->low_watermark);

    return fuse_main(fuse_argc, fuse_argv, &hook_oper, NULL);

}

/*************************************************************/



