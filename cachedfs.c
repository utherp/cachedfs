/********************************************************************
 * Synopsis:
 *
 * blockcache_record *rec = open_blockcache_record(const char *path_to_cached_file, int create);
 *
 * will return a blockcache_record structure after potentially performing
 * the following operations:
 *
 *        load the blockcache info data (rec->info)
 * OR
 *         create the blockcache info data (rec->info)
 * AND MAYBE
 *         creating the local cache file (rec->local_fd)
 *
 *     The only percevable time this call should fail is if the source file
 *     does not exist or cannot be stat'd for whatever reason.
 *
 * blockcache info data is accessable via:
 *         blockcache_t *info = rec->info;
 * 
 * file descriptors:
 *     rec->source_fd:        source file
 *     rec->local_fd:        local cached file
 *     rec->info_fd:        blockcache_t persistent info file
 *
 *
 * Useful function calls and macros:
 *
 * block address:
 *     block_addr(blockcache_t*, int block_number)
 *
 * block # of an address:
 *   block_num(blockcache_t*, off_t offset)
 *
 * # of bytes from an offset to the end of the block its contained in:
 *   bytes_in_block_from (blockcache_t*, off_t offset)
 *
 * mark a block as cached:
 *   mark_block_cached (blockcache_t*, int block_number)
 *
 * mark a block as not cached:
 *   mark_block_uncached (blockcache_t*, int block_number)
 *
 * determine if a block is cached:
 *   block_cached (blockcache_t*, int block_number)
 *
 * determine how many of the bytes requested, if any, are cached at the given offset:
 *   range_cached (blockcache_t*, off_t offset, size_t bytes_requested);
 *   ... returns int from 0 to bytes_requested
 *
 * free memory, close file descriptors and remove blockcache record from link list:
 *     free_blockcache_record (blockcache_record *)
 *
 * write blockcache info data to info data file (dirname(path) . '/.' . basename(path) . '.blockcache')
 *      sync_blockcache_info (blockcache_record *);
 *
 *
 * any other functions you see here should probably not be used directly
 *
 */

#define _ATFILE_SOURCE
#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>
#include <libgen.h>
#include <errno.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>
#include "cachedfs.h"
#include "debug_defs.h"

#include "config.h"

extern FILE *logfile;

/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/

static inline int make_blockcache (blockcache_record *rec, ino_t source_inode, ino_t local_inode, size_t filesize, size_t block_size);
static inline blockcache_record *make_blockcache_record (cachedfs_info *cachedfs, const char *path);
static blockcache_record *create_cache (cachedfs_info *cachedfs, const char *path);
static int read_cache_info (blockcache_record *listent);
static blockcache_record *load_cache_info (cachedfs_info *cachedfs, const char *path, struct stat *stbuf);
static inline char *cache_info_filename (const char *path, char *buf);
static void remove_from_record_list (blockcache_record *rec, int locked);
static void ref_blockcache (blockcache_record *rec);
static void free_blockcache_record (blockcache_record *rec, int locked);

static int remove_blockcache (blockcache_record *rec, int locked);
static void sort_blockcache_records (cachedfs_info *cachedfs, int locked);
static int cache_block (blockcache_record *rec, char *buf, unsigned int block);

//static int fstatat (int dirfd, const char *path, struct stat *stbuf, int flags);

static int validate_record (cachedfs_info *, blockcache_record *rec);

#define lock_cachedfs(cfs) pthread_mutex_lock(&(cfs->lock))
#define unlock_cachedfs(cfs) pthread_mutex_unlock(&(cfs->lock))
#define initlock_cachedfs(cfs) pthread_mutex_init(&(cfs->lock), NULL)
#define trylock_cachedfs(cfs) pthread_mutex_trylock(&(cfs->lock))

/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/

// marks that a block has been cached
int mark_block_cached (blockcache_record *rec, unsigned int block) {
    unsigned int cn = block>>3;
    if (cn > rec->cached->info.block.count) return 0;
    if (!block_cached(rec, block)) {
        rec->changed = 1;
        rec->cached->blocks[cn] |= 1<<(block & 7);
        rec->cached->info.block.cached++;
        rec->cachedfs->bytes += rec->cached->info.block.size;
    }
    return 1;
}

// marks that a block has not been cached (block purged from cache)
int mark_block_uncached (blockcache_record *rec, unsigned int block) {
    unsigned int cn = block>>3;
    if (cn > rec->cached->info.block.count) return 0;
    if (block_cached(rec, block)) {
        rec->changed = 1;
        rec->cached->blocks[cn] &= ~(1<<block & 7);
        rec->cached->info.block.cached--;
        rec->cachedfs->bytes -= rec->cached->info.block.size;
    }
    return 1;
}

// returns whether a certain block has been cached
int block_cached (blockcache_record *rec, unsigned int block) {
    unsigned int cn = block>>3;
    if (cn > rec->cached->info.block.count) return 0;
    return (rec->cached->blocks[cn] & 1<<(block & 7))?1:0;
}

// returns a number up to 'size' for how many bytes are cached starting at the given offset
int range_cached (blockcache_record *rec, off_t offset, size_t size) {
    unsigned int blk = block_num(rec, offset);
    if (!block_cached(rec, blk)) return 0;
    size_t remain = bytes_in_block_from(rec, offset);
    if (remain < size) size = remain;
    return size;
}

int remove_blockcache_by_path (cachedfs_info *cachedfs, const char *path) {
    fchdir(dirfd(cachedfs->cache_dir));
    if (path[0] == '/') path++;
    blockcache_record *rec = open_blockcache_record(cachedfs, path, 0);
    if (rec == NULL) return -ENOENT;
    return remove_blockcache(rec, 0);
}

static int remove_blockcache (blockcache_record *rec, int locked) {
    char buf[PATH_BUFSIZE];
    cachedfs_info *cachedfs = rec->cachedfs;

    _debug_flow("removing blockcache with the following path: (%s)", rec->path);

    fchdir(dirfd(cachedfs->cache_dir));

    rec->remove = 1;

    unlink(rec->path);
    unlink(cache_info_filename(rec->path, buf));

    if (!locked) lock_cachedfs(cachedfs);

    cachedfs->bytes -= (rec->cached->info.block.size * rec->cached->info.block.cached);
    cachedfs->files--;
    cachedfs->filelist_changed = 1;

    free_blockcache_record(rec, 1);

    if (!locked) unlock_cachedfs(cachedfs);
    write_cached_files_list(cachedfs);

    return 0;
}

/****************************************************/

blockcache_record *open_blockcache_record (cachedfs_info *cachedfs, const char *path, int create) {
    if (path[0] == '/') path++;
    _debug_flow("opening blockcache record for '%s'", path);
    lock_cachedfs(cachedfs);
    _debug_flow("RECORD LIST: LOCKED", 0);

    blockcache_record *rec = cachedfs->records;
    fchdir(dirfd(cachedfs->cache_dir));

    struct stat stbuf;
    if (stat(path, &stbuf)) {
        if (create) { 
            rec = create_cache(cachedfs, path);
            if (rec) {
                cachedfs->files++;
                cachedfs->filelist_changed = 1;
                rec->next = cachedfs->records;
                cachedfs->records = rec;
                write_cached_files_list(cachedfs);
            }
        } else rec = NULL;
        unlock_cachedfs(cachedfs);
        _debug_flow("RECORD LIST: UNLOCKED", 0);
        return rec;
    }

    ino_t inode = stbuf.st_ino;
    _debug_flow("-- searching record list for inode %d", inode);
    while (rec) {
        _debug_flow("-- record list ref (%x)", rec);
        if (rec->cached && (inode == rec->cached->info.inode.local)) {
            _debug_flow("-- found previously open record...", 0);
            unlock_cachedfs(cachedfs);
            _debug_flow("RECORD LIST: UNLOCKED", 0);
            ref_blockcache(rec);
            return rec;
        }
        rec = rec->next;
    }
    unlock_cachedfs(cachedfs);
    _debug_flow("RECORD LIST: UNLOCKED", 0);

    _debug_flow("-- record not found for path '%s'", path);
    return NULL;
}

static inline char *cache_info_filename (const char *path, char *buf) {
    if (path[0] == '/') path++;
    char *ptmp = strdup(path);
    char *bn = basename(ptmp);
    char *dn = dirname(ptmp);
    buf[0] = '\0';
    strcpy(buf, dn);
    strcat(buf, "/.");
    strcat(buf, bn);
    strcat(buf, ".blockcache");
    free(ptmp);
    return buf;
}

static blockcache_record *load_cache_info (cachedfs_info *cachedfs, const char *path, struct stat *stbuf) {
    // chop leading '/' from path
    if (path[0] == '/') path++;

    _debug_flow("loading blockcache info for '%s'", path);

    // change to local cache dir
    fchdir(dirfd(cachedfs->cache_dir));

    // stat local file if stat was not passed (or return NULL on failure)
    if (stbuf == NULL && stat(path, stbuf)) return NULL;

    // open local cache file
    int local_fd = open(path, O_RDWR);

    if (local_fd == -1) {
        _debug_flow("Error: unable to open local file: %s", strerror(errno));
        return NULL;
    }

    // create blockcache link-list entry
    blockcache_record *rec = make_blockcache_record(cachedfs, path);

    rec->cache_stat = *((struct stat*)stbuf);
    rec->local_fd = local_fd;
    rec->atime = rec->cache_stat.st_atime;

    // stat cache info file, read if exists, create if not
    if (read_cache_info(rec) < 0) {
        free_blockcache_record(rec, 0);
        return NULL;
    }

    return rec;
}

static inline int map_blockcache_info (blockcache_record *rec, const char *path, size_t size) {
   
    // open cache info file
    int info_fd = open(path, O_RDWR | O_CREAT, 0644);

    if (info_fd == -1) {
        _debug_flow("Error: unable to open cache info file '%s': %s", path, strerror(errno));
        return -errno;
    }

    struct stat stbuf;
    fstat(info_fd, &stbuf);

    if (!stbuf.st_size) {
        lseek(info_fd, (off_t)(size-1), SEEK_SET);
        write(info_fd, "\0", 1);
    }

    rec->info_size = size;

    // mmap blockcache info file now, instead of reading into ram and recaching it
    rec->cached= (blockcache_t*)mmap(NULL, rec->info_size, PROT_READ | PROT_WRITE, MAP_SHARED, info_fd, 0L);

    if (rec->cached == MAP_FAILED) {
        _show_error("ERROR: Unable to map shared memory segment: %s", strerror(errno));
        rec->cached = NULL;
    } else {
        _debug_flow("-- mapped %d bytes from info file", size);
    }

    // close the fd.
    close(info_fd);

    return 0;
}

static int read_cache_info (blockcache_record *rec) {
    // chdir to cache root
    fchdir(dirfd(rec->cachedfs->cache_dir));

    // get the cache info filename
    char tmp[PATH_BUFSIZE];
    cache_info_filename(rec->path, tmp);

    _debug_flow("reading cache info file (%s)...", tmp);

    // attempt to stat the cache info file, if it does not exist, create it and return
    struct stat infostat;
    if (stat(tmp, &infostat)) {
        _debug_flow("-- failed to stat the cache info file... creating", 0);
        // cache info file does not exist... create it.
        make_blockcache(rec, rec->source_stat.st_ino, rec->cache_stat.st_ino, rec->source_stat.st_size, rec->cachedfs->default_block_size);
        sync_blockcache_info(rec);
        return 0;
    }

    // cache info file exists
    map_blockcache_info(rec, tmp, infostat.st_size);

    /* this section verifies the info file's version */
    int infosize = (!rec->cached->info.block.size)?0:blockcache_info_size(block_count(rec->cache_stat.st_size, rec->cached->info.block.size));
    if (infosize == infostat.st_size) {
        /* the file is a valid v2 blockcache info file */
        return 0;
    }

    _show_error("The info filesize is incorrect (size: %u, expecting: %u)", infostat.st_size, infosize);

    /* the info file is not a valid v2 blockcache info file... */
    blockcache_v1_t *old = (blockcache_v1_t *)rec->cached;

    if (old->info.local_inode != rec->cache_stat.st_ino) {
        /* the info file is not a valid v1 blockcache info file either... corrupt! */
        _show_error("Warning: Cache info file '%s' is corrupted, recreating and assuming no blocks cached", tmp);
        munmap(rec->cached, rec->info_size);
        rec->cached = NULL; rec->info_size = 0;
        unlink(tmp);
        make_blockcache(rec, rec->source_stat.st_ino, rec->cache_stat.st_ino, rec->source_stat.st_size, rec->cachedfs->default_block_size);
        sync_blockcache_info(rec);
        return 0;
    }


    /* the info file is a v1 blockcache info file, convert to v2 */

    _show_error("Warning: Cache info file '%s' is of version 1!, converting...", tmp);

    /* copy the old data */
    old = (blockcache_v1_t*)malloc(rec->info_size);
    int old_info_size = rec->info_size;
    memcpy(old, rec->cached, old_info_size);

    /* remove the old info file */
    munmap(rec->cached, rec->info_size);
    rec->cached = NULL; rec->info_size = 0;
    unlink(tmp);

    /* make a new info file */
    make_blockcache(rec, rec->source_stat.st_ino, rec->cache_stat.st_ino, rec->source_stat.st_size, old->info.block_size);

    /* update the new info file with data from the old info file */
    rec->cached->info.block.cached = old->info.blocks_cached;
    memcpy(&(rec->cached->blocks[0]), &(old->blocks[0]), old_info_size - sizeof(blockcache_info_v1_t));

    /* sync new info to disk */
    sync_blockcache_info(rec);

    return 0;
}

static int make_containing_cache_path (const char *path) {
    if (path[0] == '/') path++;
    char *tmp = strdup(path);
    char *cur;

    for (cur = tmp; *cur != '\0'; ) {
        for (; *cur != '/' && *cur != '\0'; cur++);
        if (*cur == '\0') break;
        *cur = '\0';
        mkdir(tmp, 0755);
        *cur = '/';
        cur++;
    }

    free(tmp);
    return 0;

}

static blockcache_record *create_cache (cachedfs_info *cachedfs, const char *path) {
    // chop initial '/' from path
    if (path[0] == '/') path++;

    _debug_flow("creating local cache for '%s'", path);

    // allocate link list entry
    blockcache_record *rec = make_blockcache_record(cachedfs, path);

    // change to cache dir
    fchdir(dirfd(cachedfs->cache_dir));

    make_containing_cache_path(path);

    // open / create local cache file
    _debug_flow("creating local cache file...", 0);
    rec->local_fd = open(path, O_RDWR | O_CREAT, 0644);
    if (rec->local_fd == -1) {
        _debug_flow("Unable to create local cache file: %s", strerror(errno));
    }

    // stat local file for inode
    fstat(rec->local_fd, &(rec->cache_stat));
    rec->atime = rec->cache_stat.st_atime;

    // allocate blockcache info (source inode, local inode, source file size, cache block size) 
    make_blockcache(rec, rec->source_stat.st_ino, rec->cache_stat.st_ino, rec->source_stat.st_size, rec->cachedfs->default_block_size);

    cache_block(rec, NULL, 0);
    sync_blockcache_info(rec);

    if (rec->cached->info.block.count > 1) {
        // seek to last byte and write a null to create the sparse file
        _debug_flow("Writing null byte at offset %u for a file %u bytes long", rec->source_stat.st_size - 1, rec->source_stat.st_size);
        lseek(rec->local_fd, (off_t)rec->source_stat.st_size-1, SEEK_SET);
        if (write(rec->local_fd, "", 1) == -1) {
            _debug_flow("Error: Unable to write last byte for local sparse file: %s", strerror(errno));
        }
    } else {
        _debug_flow("file is only one block, sparse file not needed", 0);
    }

    // return blockcache list entry
    return rec;
}

static void remove_from_record_list (blockcache_record *rec, int locked) {
    if (!locked) {
        lock_cachedfs(rec->cachedfs);
        _debug_flow("RECORD LIST: LOCKED", 0);
    }
    if (rec->cachedfs->records  == rec) {
        rec->cachedfs->records = rec->next;
    } else {
        _debug_flow("-- searching for record to remove in list", 0);
        _debug_flow("## cachedfs ref is %x", rec->cachedfs);
        blockcache_record *tmp = rec->cachedfs->records;
        while (tmp) {
            if (tmp->next == rec) {
                _debug_flow("-- found it, setting next (%x) to this next (%x)", tmp->next, rec->next);
                tmp->next = rec->next;
                break;
            }
            tmp = tmp->next;
        }
        _debug_flow("-- done searching for record in list!", 0);
    }
    if (!locked) {
        unlock_cachedfs(rec->cachedfs);
        _debug_flow("RECORD LIST: UNLOCKED", 0);
    }
    return;
}

static inline blockcache_record *make_blockcache_record (cachedfs_info *cachedfs, const char *path) {
    blockcache_record *tmp = (blockcache_record*)calloc(1, sizeof(blockcache_record));
    tmp->cachedfs = cachedfs;
    fchdir(dirfd(cachedfs->source_dir));
    if (path[0] == '/') path++;
    tmp->path = strdup(path);
    tmp->source_fd = open(path, O_RDONLY);
    blockcache_stat(tmp, NULL);
    initlock_cachedfs(tmp);
    tmp->refs = 1;
    return tmp;
}

int blockcache_stat (blockcache_record *rec, struct stat *stbuf) {
    struct timeval now;
    gettimeofday(&now, NULL);
    int ret = 0;

    if (now.tv_sec > (rec->last_cached.tv_sec + STAT_CACHE_EXPIRY)) {
        /* recache file stat */
        ret = fstat(rec->source_fd, &(rec->source_stat));
        if (rec->cached) {
            rec->source_stat.st_blksize = rec->cached->info.block.size;
            rec->source_stat.st_blocks = rec->cached->info.block.count;
            rec->last_cached = now;
        }
    }

    if (stbuf != NULL) {
        (*stbuf) = rec->source_stat;
    }
    return ret;
}

static inline int make_blockcache (blockcache_record *rec, ino_t source_inode, ino_t local_inode, size_t filesize, size_t block_size) {
    size_t blocks = block_count(filesize, block_size);
    size_t infosize = blockcache_info_size(blocks); 

    char tmp[PATH_BUFSIZE];
    cache_info_filename(rec->path, tmp);

    _debug_flow("cache info filename: '%s' (info size: %d)", tmp, infosize);
    map_blockcache_info(rec, tmp, infosize);
    
    rec->cached->info.version = 2;
    rec->cached->info.flags = 0;
    rec->cached->info.block.size = block_size;
    rec->cached->info.block.count = blocks;
    rec->cached->info.block.cached = 0;
    rec->cached->info.inode.local = local_inode;
    rec->cached->info.inode.source = source_inode;
    rec->cached->info.filesize = filesize;

    sync_blockcache_info(rec);

    _debug_flow("created blockcache info: filesize(%d), block_size(%d), blocks(%d)", filesize, block_size, blocks); 
    return 0;
}

static void ref_blockcache (blockcache_record *rec) {
    lock_cachedfs(rec);
    rec->refs++;
    unlock_cachedfs(rec);
    return;
}

void close_blockcache_record (blockcache_record *rec) {
    _debug_flow("closing blockcache record", 0);
    lock_cachedfs(rec);
    rec->refs--;
    sync_blockcache_info(rec);
    unlock_cachedfs(rec);
    return;
}

static void free_blockcache_record (blockcache_record *rec, int locked) {
    _debug_flow("freeing blockcache record", 0);
    remove_from_record_list(rec, locked);
    if (rec->source_fd) close(rec->source_fd);
    if (rec->local_fd) close(rec->local_fd);
    if (!rec->remove)
        sync_blockcache_info(rec);

    if (rec->cached) {
        sync_blockcache_info(rec);
        munmap(rec->cached, rec->info_size);
    }

    if (rec->path) free(rec->path);
    free(rec);
    _debug_flow("-- blockcache record freed", 0);
    return;
}

inline void sync_blockcache_info (blockcache_record *rec) {
    // just sync the memory map
    msync(rec->cached, rec->info_size, MS_ASYNC | MS_INVALIDATE);

    return;
}

void write_all_cachedfs_changes (cachedfs_info *cachedfs) {
    _debug_flow("writing cachedfs changes to info files...", 0);
    lock_cachedfs(cachedfs);
    _debug_flow("RECORD LIST: LOCKED", 0);
    write_cached_files_list(cachedfs);

    blockcache_record *rec = cachedfs->records;
    while (rec) {
        // this just does an msync now
        sync_blockcache_info(rec);
        rec = rec->next;
    }

    unlock_cachedfs(cachedfs);
    _debug_flow("RECORD LIST: UNLOCKED", 0);

    return;
}

int read_block (blockcache_record *rec, char *buf, unsigned int block) {
    int ret = 0;
    if (block >= rec->cached->info.block.count) return 0;
    int rem = bytes_in_block(rec, block);
    if (!block_cached(rec, block))
        return cache_block(rec, buf, block);
    
    int r = 0;
    char *tbuf = buf;
    off_t offset = block_addr(rec, block);
    lseek(rec->local_fd, offset, SEEK_SET);

    while (rem) {
        r = read(rec->local_fd, tbuf, rem);
        if (r == -1) return -errno;
        tbuf += r;
        ret += r;
        rem -= r;
    }
    rec->atime = time(NULL);
    return ret;
}

int read_bytes_at (blockcache_record *rec, char *buf, off_t offset, size_t size) {
    int block = block_num(rec, offset);
    int ret = 0;
    if (block >= rec->cached->info.block.count) return 0;
    size_t bytes = bytes_in_block_from(rec, offset);

    _debug_flow("reading %d bytes at %lld (local_fd:%d, source_fd:%d, block %d, bytes in block: %d)", size, offset, rec->local_fd, rec->source_fd, block, bytes);

    if (!block_cached(rec, block) && (ret = cache_block(rec, NULL, block)) < 0)
        return ret;
    
    char *tbuf = buf;
    int rem = bytes, r = 0;
    if (rem > size) rem = size;
    ret = 0;
    lseek(rec->local_fd, offset, SEEK_SET);
    while (rem) {
        r = read(rec->local_fd, tbuf, rem);
        if (r == -1) return -errno;
        rem -= r;
        tbuf += r;
        ret += r;
    }
    
    _debug_flow("read %d bytes from local cache", ret);

    rec->atime = time(NULL);
    return ret;
}

static void sort_blockcache_records (cachedfs_info *cachedfs, int locked) {
    if (!cachedfs->records) return;
    if (!locked) lock_cachedfs(cachedfs);

    blockcache_record *old;

    #ifdef _DEBUG_FLOW_
        _debug_flow("Pre-sort:", 0);
        for (old = cachedfs->records; old; old = old->next) {
            _debug_flow("\t%u: %s", old->atime, old->path);
        }
    #endif

    do {
        for (old = cachedfs->records; old->next && old->next->atime <= old->atime; old = old->next);
        if (old->next) {
            blockcache_record *tmp = old->next;
            old->next = tmp->next;
            tmp->next = cachedfs->records;
            cachedfs->records = tmp;
            continue;
        }
        break;
    } while (1);

    #ifdef _DEBUG_FLOW_
        _debug_flow("Post-sort:", 0);
        for (old = cachedfs->records; old; old = old->next) {
            _debug_flow("\t%u: %s", old->atime, old->path);
        }
    #endif

    if (!locked) unlock_cachedfs(cachedfs);
    return;
}

int write_block (blockcache_record *rec, const char *buf, unsigned int block) {
    int rem = bytes_in_block(rec, block);
    if (block == (rec->cached->info.block.count - 1))
        rem = rec->source_stat.st_size - block_addr(rec, block);
    int wr = 0;
    lseek(rec->local_fd, block_addr(rec, block), SEEK_SET);
    while (rem) {
        wr = write(rec->local_fd, buf, rem);
        if (wr == -1)
            return -errno;
        rem -= wr;
        buf += wr;
    }
    mark_block_cached(rec, block);
    return rec->cached->info.block.size;
}

int write_bytes_at (blockcache_record *rec, const char *buf, off_t offset, size_t size) {
    int block = block_num(rec, offset);
    if (block >= rec->cached->info.block.count) return 0;
    off_t block_offset = block_addr(rec, block);
    int ret = 0;
    const char *buf_ref = buf;
    if (block_offset != offset) {
        size -= (offset - block_offset);
        buf_ref += (offset - block_offset);
        block++;
        block_offset += rec->cached->info.block.size;
    }

    while (size > 0) {
        if (size >= rec->cached->info.block.size || block == (rec->cached->info.block.count - 1)) 
            ret += write_block(rec, buf_ref, block);
        block++;
        if (block == (rec->cached->info.block.count)) return ret;
        block_offset += rec->cached->info.block.size;
        size -= rec->cached->info.block.size;
        buf_ref += rec->cached->info.block.size;
    }

    return ret;
}

static int cache_block (blockcache_record *rec, char *buf, unsigned int block) {
    if (block >= rec->cached->info.block.count) return 0;
    _debug_flow("caching block %d", block);
    off_t offset = block_addr(rec, block);
    size_t bytes = bytes_in_block(rec, block);
    int freeit = 0;
    char *btmp;
    int ret = 0;
    if (buf == NULL) {
        freeit = 1;
        buf = malloc(bytes);
    }
    btmp = buf;

    int rem = bytes, r = 0;

    lseek(rec->source_fd, offset, SEEK_SET);

    while (rem) {
        r = read(rec->source_fd, btmp, rem);
        if (r == -1) {
            ret = -errno;
            break;
        }
        rem -= r;
        btmp += r;
        ret += r;
    }
    if (ret > 0) {
        _debug_flow("read %d bytes from source, writing block to local cache...", ret);
        ret = write_block(rec, buf, block);
    }

    if (freeit) free(buf);
    return ret;
}

/*****************************************************************************************/

cachedfs_info *init_cachedfs (const char *cache_path, const char *source_path) {
    cachedfs_info *cachedfs = calloc(1, sizeof(cachedfs_info));
    initlock_cachedfs(cachedfs);
    cachedfs->source_path = strdup(source_path);
    cachedfs->cache_path = strdup(cache_path);

    _debug_flow("Cachefs init:\n\tsource: '%s'\n\tlocal: '%s'\n", cachedfs->source_path, cachedfs->cache_path);

    cachedfs->source_dir = opendir(cachedfs->source_path);
    if (!cachedfs->source_dir) {
        _show_error("ERROR: Unable to open source dir!: %s", strerror(errno));
        exit(8);
    }

    cachedfs->cache_dir = opendir(cachedfs->cache_path);
    if (!cachedfs->cache_dir) {
        _show_error("ERROR: Unable to open cache dir!: %s", strerror(errno));
        exit(8);
    }

    cachedfs->default_block_size = (unsigned long)read_num_from_file(".cachedfs_blocksize", DEFAULT_BLOCK_SIZE);

    read_watermarks(cachedfs);
//    read_cached_files_list(cachedfs);
    read_cached_files(cachedfs);
    return cachedfs;
}

uint64_t read_num_from_file (const char *filename, uint64_t def) {
    // read a long integer as ascii from filename or return default

    struct stat sttmp;
    if (stat(filename, &sttmp)) return def;

    // if filesize is rediculous, forget it!
    if (sttmp.st_size > 255) return def;

    char buff[256];
    buff[255] = '\0';
    char *tmp = buff;

    int fd = open(filename, O_RDONLY);
    if (fd == -1) return def;

    int r, rm = 255;

    while (rm) {
        r = read(fd, tmp, rm);
        if (r == -1) {
            close(fd);
            return def;
        }
        if (!r) break;
        rm -= r;
        tmp += r;
    }

    close(fd);
    return atoll(buff);
}
    
int read_watermarks (cachedfs_info *cachedfs) {
    fchdir(dirfd(cachedfs->cache_dir));
    cachedfs->high_watermark = read_num_from_file(".cachedfs_high_watermark", DEFAULT_HIGH_WATERMARK);
    cachedfs->low_watermark = read_num_from_file(".cachedfs_low_watermark", DEFAULT_LOW_WATERMARK);
    return 0;
}

int read_cached_files (cachedfs_info *cachedfs) {
    /* must validate cached files against remote server */
    char pathbuf[PATH_MAX];
    pathbuf[0] = '\0';
    if (fchdir(dirfd(cachedfs->cache_dir))) {
        _show_error("ERROR: Could not change to cache dir: %s", strerror(errno));
        exit(10);
    }
    return scan_cache_dir(cachedfs, cachedfs->cache_dir, pathbuf);
}

int validate_records (cachedfs_info *cachedfs) {
    blockcache_record *rec, *last = NULL;
    int inval = 0;
    for (rec = cachedfs->records; rec; rec = rec->next) {
        if (!validate_record(cachedfs, rec)) {
            if (last) last->next = rec->next;
            else cachedfs->records = rec->next;
            remove_blockcache(rec, 0);
            inval++;
        } else
            last = rec;
    }

    return inval;
}


static int validate_record (cachedfs_info *cachedfs, blockcache_record *rec) {
    do {
        /* validate local file */
        if (fstatat(dirfd(cachedfs->cache_dir), rec->path, &(rec->cache_stat), 0)) break;

        /* validate remote file */
        if (fstatat(dirfd(cachedfs->source_dir), rec->path, &(rec->source_stat), 0)) break;

        /* compare sizes */
        if (rec->source_stat.st_size  != rec->cache_stat.st_size) break;

        /* cache is valid */
        rec->last_validated = time(NULL);

        return 1;

    } while(0);

    /* from here, we determine how long its been invalid */
    
    if (!rec->last_validated) {
        /* never validated */
        return 0;
    }

    time_t now = time(NULL);
    if (rec->last_validated < (now - INVAL_EXPIRY)) {
        /* last validated more than INVAL_EXPIRY seconds ago (set in cachedfs.h) */
        return 0;
    }

    /* has not been too long since it had validated, we'll keep it and see it if comes back */
    return 1;
}

//static int fstatat (int dirfd, const char *path, struct stat *stbuf, int flags) {
    /* this is to mimic the fstatat system call which apparently does
     * not exist in the node's version of libc
     */
/*
    char *lwd = get_current_dir_name();
    fchdir(dirfd);
    char *cwd = get_current_dir_name();
    _debug_flow("  fstatat('%s', '%s')", cwd, path);
    free(cwd);
    int ret = stat(path, stbuf);
    chdir(lwd);
    free(lwd);
    return ret;
}
*/
int scan_cache_dir (cachedfs_info *cachedfs, DIR *dir, char *pathbuf) {
    char *lwd = get_current_dir_name();

    _debug_flow("Scanning cache dir '%s'", pathbuf);

    struct dirent *ent;
    struct stat stbuf;

    char fnbuf[245];
    int pathsz = strlen(pathbuf);
    char *tmp = pathbuf + pathsz;
    int rem = PATH_MAX - pathsz;
    int total = 0;

    blockcache_record *last = NULL, *rec = NULL;

    while ((ent = readdir(dir))) {
        _debug_flow("...checking '%s'", ent->d_name);
        if (stat(ent->d_name, &stbuf)) {
            /* stat failed */
            _debug_flow("   NOTE: stat failed", 0);
            continue;
        }

        if (S_ISLNK(stbuf.st_mode)) {
            /* symlink */
            _debug_flow("   NOTE: symlink", 0);
            continue;
        }

        int l = strlen(ent->d_name);
        if (l > rem) {
            /* path buffer overflow */
            _show_error("Warning: Path buffer overflow (%d > %d) at: '%s/%s'", l, rem, pathbuf, ent->d_name);
            continue;
        }

        if (stbuf.st_mode & S_IFDIR) {
            /* a directory */
            if (ent->d_name[0] == '.') {
                /* no directories with leading '.' */
                _debug_flow("    NOTE: directory has leading .", 0);
                continue;
            }
            DIR *subdir = opendir(ent->d_name);
            if (!subdir) {
                /* opendir failed */
                _debug_flow("    NOTE: opendir failed.", 0);
                continue;
            }
            fchdir(dirfd(subdir));

            memcpy(tmp, ent->d_name, l);
            tmp[l] = '/';
            tmp[l+1] = '\0';

            _debug_flow("...found dir in cache at '%s'", pathbuf);

            total += scan_cache_dir(cachedfs, subdir, pathbuf);
            closedir(subdir);
            chdir(lwd);
            tmp[0] = '\0';
            continue;
        }

        /* validate if this is a valid blockcache info file */
        if (!(stbuf.st_mode & S_IFREG)) {
            /* not a regular file */
            _debug_flow("   NOTE: not a regular file", 0);
            continue;
        }
        if (l < 13) {
            /* blockcache info filename length is original filename + 12 chars (.FILENAME.blockcache) */
            _debug_flow("   NOTE: filename not long enough", 0);
            continue;
        }
        if (ent->d_name[0] != '.') {
            /* blockcache info files start with a '.' */
            _debug_flow("   NOTE: not a blockcache file (no leading .)", 0);
            continue;
        }
        if (strcmp(&(ent->d_name[l-11]), ".blockcache")) {
            /* does not end with .blockcache */
            _debug_flow("   NOTE: not a blockcache file (no trailing '.blockcache')", 0);
            continue;
        }

        /* gets the cache filename */
        memcpy(fnbuf, &(ent->d_name[1]), l - 12);
        fnbuf[l-12] = '\0';
        memcpy(tmp, fnbuf, l-11);

        _debug_flow("...found blockcache file for '%s' (%s)", pathbuf, ent->d_name);

        if (stat(fnbuf, &stbuf)) {
            /* local cache file is missing */
            _debug_flow("   NOTE: local cache missing (%s): removing blockcache file.", fnbuf);
            unlink(ent->d_name);
            continue;
        }

        if (fstatat(dirfd(cachedfs->source_dir), pathbuf, &stbuf, 0)) {
            /* remote file is missing */
            _debug_flow("   NOTE: remote file missing (%s/%s): removing cache and blockcache files.", cachedfs->source_path, pathbuf);
            unlink(ent->d_name);
            unlink(fnbuf);
            continue;
        }

        _debug_flow("   Loading blockcache file for '%s'", pathbuf);
        rec = load_cache_info(cachedfs, pathbuf, &stbuf);
        if (!rec) {
            _debug_flow("WARNING: Unable to load cache info for file '%s'", pathbuf);
            continue;
        }

        if (!last) {
            rec->next = cachedfs->records;
            cachedfs->records = rec;
        } else {
            last->next = rec;
            rec->next = NULL;
        }
        last = rec;

        total++;
        cachedfs->files++;
        cachedfs->bytes += (rec->cached->info.block.size * rec->cached->info.block.cached);
    }

    return total;
}


int read_cached_files_list (cachedfs_info *cachedfs) {
    fchdir(dirfd(cachedfs->cache_dir));
    cachedfs->cachelist_fd = open(".cached_files", O_RDWR);
    if (cachedfs->cachelist_fd == -1) {
        cachedfs->cachelist_fd = open(".cached_files", O_CREAT | O_RDWR, 0644);
        if (cachedfs->cachelist_fd == -1) {
            _show_error("ERROR: Unable to open cached files info list '.cached_files': %s", strerror(errno));
            exit(10);
        }
        cachedfs->filelist_changed = 1;
        write_cached_files_list(cachedfs);
        return 0;
    }

    struct stat sttmp;
    fstat(cachedfs->cachelist_fd, &sttmp);
    int r, rm, listsize = sttmp.st_size;

    char *buffer = malloc(listsize + 1);
    buffer[listsize] = '\0';
    char *btmp = buffer;

    blockcache_record *last = NULL;

    rm = listsize;
    while (rm) {
        r = read(cachedfs->cachelist_fd, btmp, rm);
        if (r == -1) {
            _show_error("WARNING: Unable to read cached files info list: %s", strerror(errno));
            exit(11);
        }
        if (!r) {
            btmp[0] = '\0';
            break;
        }
        rm -= r;
        btmp += r;
    }

    blockcache_record *rec = NULL;

    btmp = buffer;
    for (btmp = buffer; btmp < (buffer + listsize); btmp += strlen(btmp) + 1) {
        if (stat(btmp, &sttmp)) {
            _debug_flow("WARNING: Unable to stat cache file '%s': %s", btmp, strerror(errno));
            continue;
        }
        rec = load_cache_info(cachedfs, btmp, &sttmp);
        if (!rec) {
            _debug_flow("WARNING: Unable to load cache info for file '%s'", btmp);
            continue;
        }

        if (!last) {
            rec->next = cachedfs->records;
            cachedfs->records = rec;
        } else {
            last->next = rec;
            rec->next = NULL;
        }
        last = rec;
        /*********************************************************************
         * order records by access time starting with most recently accessed
         */
        /*
        if (!cachedfs->records || cachedfs->records->atime < rec->atime) {
            rec->next = cachedfs->records;
            cachedfs->records = rec;
        } else {
            blockcache_record *tmprec = cachedfs->records;
            for (; tmprec->next && tmprec->next->atime > rec->atime; tmprec = tmprec->next);
            rec->next = tmprec->next;
            tmprec->next = rec;
        }
        */

        cachedfs->files++;
        cachedfs->bytes += (rec->cached->info.block.size * rec->cached->info.block.cached);
    }

    free(buffer);

    sort_blockcache_records(cachedfs, 1);

    cachedfs->filelist_changed = 1;
    write_cached_files_list(cachedfs);
    return cachedfs->files;
}

int write_cached_files_list (cachedfs_info *cachedfs) {
    if (!cachedfs->filelist_changed) return 0;
    fchdir(dirfd(cachedfs->cache_dir));
    lseek(cachedfs->cachelist_fd, 0L, SEEK_SET);

    _debug_flow("Writing the list of cached files into '.cached_files'...", 0);
    int w, wm;
    char *tmp;

    blockcache_record *rec;
    for (rec = cachedfs->records; rec; rec = rec->next) {
        wm = strlen(rec->path) + 1;
        tmp = rec->path;
        while (wm) {
            w = write(cachedfs->cachelist_fd, tmp, wm);
            if (w == -1) {
                _show_error("ERROR: Unable to write cached files info list!: %s", strerror(errno));
                exit(12);
            }
            tmp += w;
            wm -= w;
        }
    }

    ftruncate(cachedfs->cachelist_fd, lseek(cachedfs->cachelist_fd, 0L, SEEK_CUR));
    fdatasync(cachedfs->cachelist_fd);

    cachedfs->filelist_changed = 0;

    _debug_flow("Done writing cached files list", 0);
    return 0;
}

/****************************************************************************/

void cachedfs_check_usage (cachedfs_info *cachedfs) {
    if (cachedfs->bytes < cachedfs->high_watermark) return;
    if (!cachedfs->records) return;

    _debug_flow("NOTE: Bytes used (%llu) is %llu bytes over high watermark (%llu)!", cachedfs->bytes, cachedfs->bytes - cachedfs->high_watermark, cachedfs->high_watermark);
    _debug_flow("--> Purging %llu bytes to low watermark (%llu)", cachedfs->bytes - cachedfs->low_watermark, cachedfs->low_watermark);
        
    lock_cachedfs(cachedfs);

    sort_blockcache_records(cachedfs, 1);

    blockcache_record **rev = malloc(sizeof(void*) * cachedfs->files);
    blockcache_record *tmp = cachedfs->records;

    int i, removed = 0;
    for (i = cachedfs->files - 1; i >= 0; i--) {
        rev[i] = tmp;
        tmp = tmp->next;
    }
    
    #ifdef _DEBUGGING_
        long long start_use = cachedfs->bytes;
    #endif
    i = 0;
    while ((cachedfs->bytes > cachedfs->low_watermark) && i < cachedfs->files) {
        remove_blockcache(rev[i++], 1);
        removed++;
    }

    _debug_flow("--> Removed %u cached files, freeing a total of %llu bytes", removed, start_use - cachedfs->bytes);

    unlock_cachedfs(cachedfs);

    free(rev);

    _debug_flow("--> Bytes used is now %llu", cachedfs->bytes);

    return;
}

/****************************************************************************/

char *cachedfs_status_report (cachedfs_info *cachedfs) {
    char *report = calloc(1, 1000);
    snprintf(report, 1000, 
        "Files: %u\n"
        "block size: %u\n"
        "Watermarks:\n"
        "    High: %llu\n"
        "     Low: %llu\n\n"
        "bytes used: %llu\n"
        "remain before purging: %lld\n\n",
        cachedfs->files,
        cachedfs->default_block_size, 
        cachedfs->high_watermark,
        cachedfs->low_watermark, 
        cachedfs->bytes,
        cachedfs->high_watermark - cachedfs->bytes
    );
    return report;
}

char *cachedfs_info_files (cachedfs_info *cachedfs) {
    struct stat sttmp;
    fstat(cachedfs->cachelist_fd, &sttmp);
    char *tmp = calloc(1, sttmp.st_size + 1);

    int r, rm = sttmp.st_size;
    char *cur = tmp;

    lseek(cachedfs->cachelist_fd, 0L, SEEK_SET);
    while (rm) {
        r = read(cachedfs->cachelist_fd, cur, rm);
        if (r == -1) {
            snprintf(tmp, sttmp.st_size, "An error occurred while listing the cached files: %s\n", strerror(errno));
            return tmp;
        }
        cur += r;
        rm -= r;
    }
    for (r = 0; r < sttmp.st_size; r++)
        if (tmp[r] == '\0') tmp[r] = '\n';
    
    return tmp;
}

char *stringify_number(int64_t bytes) {
    char *tmp = malloc(50);
    snprintf(tmp, 50, "%llu", bytes);
    return tmp;
}

char *stringify_percent(double percent) {
    char *tmp = malloc(10);
    percent *= 100;
    snprintf(tmp, 10, "%.4f%%", percent);
    return tmp;
}


