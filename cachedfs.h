#pragma once
#include <pthread.h>
#include <stdint.h>

#define PATH_BUFSIZE 1025
#define STAT_CACHE_EXPIRY 30
#define INVAL_EXPIRY (/* 24 hours */ 60 * 60 * 24)

typedef struct {
    uint16_t version;
    uint16_t flags;
    struct {
        uint32_t size;
        uint32_t count;
        uint32_t cached;
    } block;
    struct {
        uint64_t local;
        uint64_t source;
    } inode;
    uint64_t filesize;
} blockcache_info_v2_t;

typedef struct {
    blockcache_info_v2_t info;
    uint8_t blocks[];
} blockcache_t;

/* these two old structures are used
 * for backwards compatability.  The
 * blockcache info file's size is 
 * used to determine if it is version 1.
 * All versions > 1 shall use the same 
 * header
 *
 */
typedef struct {
    ino_t local_inode;
    ino_t source_inode;
    uint32_t block_size;
    uint32_t block_count;
    uint32_t blocks_cached;
    uint32_t blockcache_size;
} blockcache_info_v1_t;

typedef struct {
    blockcache_info_v1_t info;
    uint8_t blocks[];
} blockcache_v1_t;


typedef struct _bc_record {
    struct _bc_record *next;
    struct _cfs_index *cachedfs;
    unsigned int refs;
    time_t atime;
    int8_t remove;
    char *path;
    int local_fd;
    int source_fd;
    size_t info_size;
    pthread_mutex_t lock;
    uint8_t changed;
    struct timeval last_cached;
    struct stat source_stat;
    struct stat cache_stat;
    time_t last_validated;
    blockcache_t *cached;
} blockcache_record;

typedef struct _cfs_index {
    uint64_t high_watermark;
    uint64_t low_watermark;
    uint32_t files;
    uint64_t bytes;
    uint32_t default_block_size;
    int8_t filelist_changed;
    char *cache_path;
    char *source_path;
    DIR *cache_dir;
    DIR *source_dir;
    int cachelist_fd;
    pthread_mutex_t lock;
    struct _bc_record *records;
} cachedfs_info;

// block count for filesize and block size
    #define block_count(fsize, bsize) ((fsize / bsize) + ((fsize % bsize)?1:0))

// starting address of block #
    #define block_addr(rec, blk) (rec->cached->info.block.size * blk)

// block # containing offset
    #define block_num(rec, off) (off / rec->cached->info.block.size)

// total bytes in block (mainly for calculating bytes in last block)
    #define bytes_in_block(rec, blk) ((blk < (rec->cached->info.block.count-1))?rec->cached->info.block.size:((blk == (rec->cached->info.block.count-1))?(rec->source_stat.st_size - block_addr(rec, blk)):0))

// bytes remaining in block from offset to end of block
    #define bytes_in_block_from(rec, off) (bytes_in_block(rec, block_num(rec, off)) - (off - block_addr(rec, block_num(rec, off))))

// size a cache info file version >= 2 should be
    #define blockcache_info_size(blocks) (((blocks / sizeof(char)) + ((blocks % sizeof(char))?1:0)) + sizeof(blockcache_info_v2_t))

cachedfs_info *init_cachedfs (const char *source_path, const char *cache_path);

uint64_t read_num_from_file (const char *filename, uint64_t def);

int read_block (blockcache_record *rec, char *buf, unsigned int block);
int read_bytes_at (blockcache_record *rec, char *buf, off_t offset, size_t size);

int write_block (blockcache_record *rec, const char *buf, unsigned int block);
int write_bytes_at (blockcache_record *rec, const char *buf, off_t offset, size_t size);

int mark_block_cached (blockcache_record *rec, unsigned int block);
int mark_block_uncached (blockcache_record *rec, unsigned int block);
int block_cached (blockcache_record *rec, unsigned int block);
inline void sync_blockcache_info (blockcache_record *rec);

blockcache_record *open_blockcache_record (cachedfs_info *cachedfs, const char *path, int create);
void close_blockcache_record (blockcache_record *rec);

int remove_blockcache_by_path (cachedfs_info *cachedfs, const char *path);

void write_all_cachedfs_changes (cachedfs_info *cachedfs);
void cachedfs_check_usage (cachedfs_info *cachedfs);

char *cachedfs_status_report (cachedfs_info *cachedfs);
char *cachedfs_info_files (cachedfs_info *cachedfs);
char *stringify_number(int64_t bytes);
char *stringify_percent(double percent);

int blockcache_stat (blockcache_record *rec, struct stat *stbuf);

int read_watermarks (cachedfs_info *cachedfs);
int read_cached_files_list (cachedfs_info *cachedfs);

int write_cached_files_list (cachedfs_info *cachedfs);

int scan_cache_dir (cachedfs_info *cachedfs, DIR *dir, char *pathbuf);
int read_cached_files (cachedfs_info *cachedfs);

int validate_records (cachedfs_info *cachedfs);

