#include "attr.h"
#include <string.h>
#include <stdint.h>
#include <unistd.h>

//extern unsigned long default_block_size;

extern FILE *logfile;
static int cachedfs_info_stat (const char *path, struct stat *stbuf);

/*************************************************************/

int hook_statfs (const char *path, struct statvfs *fsstat) {

    /***************************************************
     * here we want to return the statfs for the target
     * filesystem, so the reads are aligned with the 
     * block size of the filesystem which we are caching.
     * This eliminates the need for excess remote reads
     * for a local cache read.
     *      -- Stephen
     */

    int ret = fstatvfs(dirfd(cachedfs->source_dir), fsstat);
    if (ret) return -errno;
    fsstat->f_bsize = cachedfs->default_block_size;
    fsstat->f_bsize = fsstat->f_frsize = cachedfs->default_block_size;
    fsstat->f_blocks = cachedfs->high_watermark / cachedfs->default_block_size;
    fsstat->f_bavail = fsstat->f_bfree = fsstat->f_blocks - (cachedfs->bytes / cachedfs->default_block_size);
    fsstat->f_files = cachedfs->files;
    fsstat->f_favail = fsstat->f_ffree = 0xffff;
    fsstat->f_fsid = 0x42;
    fsstat->f_namemax = PATH_BUFSIZE;


    return 0;
/*
         struct statvfs {
           unsigned long  f_bsize;    // file system block size 
           unsigned long  f_frsize;   // fragment size 
           fsblkcnt_t     f_blocks;   // size of fs in f_frsize units 
           fsblkcnt_t     f_bfree;    // # free blocks 
           fsblkcnt_t     f_bavail;   // # free blocks for non-root 
           fsfilcnt_t     f_files;    // # inodes 
           fsfilcnt_t     f_ffree;    // # free inodes 
           fsfilcnt_t     f_favail;   // # free inodes for non-root 
           unsigned long  f_fsid;     // file system ID 
           unsigned long  f_flag;     // mount flags 
           unsigned long  f_namemax;  // maximum filename length 
         };
*/
}

/*************************************************************/

int hook_fgetattr (const char *path, struct stat *buf, struct fuse_file_info *fi) {
    _debug_flow("entered fgetattr on '%s'", path);

    #if __WORDSIZE == 64
        fileinfo_t *inf = (fileinfo_t*)fi->fh;
    #else 
        fileinfo_t *inf = (fileinfo_t*)&(fi->fh);
    #endif
    return blockcache_stat(inf->rec.cache, buf);
}

/**********************************************************/

int hook_chmod(const char *path, mode_t fmode) {
    _debug_flow("entered chmod on '%s'", path);
//  fchdir(dirfd(mount_dir));
//  if (chmod(path+1, fmode)) return (errno*-1);
    return -ENOSYS;
}

/**********************************************************/

int hook_chown(const char *path, uid_t fuid, gid_t fgid) {
    _debug_flow("entered chown on '%s'", path);
//  fchdir(dirfd(mount_dir));
//  if (chown(path+1, fuid, fgid)) return (errno*-1);
    return -ENOSYS;
}

/**********************************************************/

/**********************************************************/

int hook_getattr(const char *path, struct stat *stbuf) {
    _debug_flow("entered getattr on '%s'", path);

    path++;

    /* if statting base dir, return it now */
    if (path[0] == '\0')
        return fstat(dirfd(cachedfs->source_dir), stbuf);

    /****************************************************
     * internal filehandles, settings, status, ect...
     */
    int dtype = get_magic_dir_type(&path);
    if (dtype) {
        if (dtype == INFO_TYPE) {
            /********************************************
             * info path (/##info/)
             */
            if (path[0] == '\0')
                fstat(dirfd(cachedfs->cache_dir), stbuf);
            else if (path[0] != '/') return -ENOENT;
            else if (cachedfs_info_stat(path + 1, stbuf)) return -errno;
            stbuf->st_mode &= 0777555;
            return 0;

        }
        
        /********************************************
         * /##peek and /##pass dirs
         * peek accesses underlying local fs
         * pass accesses underlying remote fs
         */

        if (dtype == PEEK_TYPE)
            fchdir(dirfd(cachedfs->cache_dir));
        else
            fchdir(dirfd(cachedfs->source_dir));

        if (path[0] == '\0') path = ".";
        else if (path[0] == '/') path++;
        else return -ENOENT;

        if (lstat(path, stbuf)) return -errno;
        stbuf->st_mode &= 0777555;

        return 0;
    }

    /* attempt to open, but do not create */
    blockcache_record * rec = open_blockcache_record(cachedfs, path, 0);

    if (rec == NULL) {
        /* record does not exist, stat the source file... */
        fchdir(dirfd(cachedfs->source_dir));
        if (lstat(path, stbuf)) return -errno;
        stbuf->st_blksize = cachedfs->default_block_size;
        return 0;
    }

    /* cache record found, use cached stat */
    if (blockcache_stat(rec, stbuf)) return -errno;
        
    return 0;
}

/**********************************************************/

static int cachedfs_info_stat (const char *path, struct stat *stbuf) {
    if (!get_info_type(path)) {
        errno = ENOENT;
        return -1;
    }

    fstat(cachedfs->cachelist_fd, stbuf);
    stbuf->st_size = 2000;
    return 0;
}


