#pragma once
#include "common.h"
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

int hook_open(const char *path, struct fuse_file_info *fi);
int hook_release (const char *path, struct fuse_file_info *fi);

int hook_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int hook_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);

int hook_truncate(const char *path, off_t offset);
int hook_ftruncate (const char *path, off_t offset, struct fuse_file_info *fi);

int hook_mknod(const char *path, mode_t fmode, dev_t fdev);
int hook_create(const char *path, mode_t mode, struct fuse_file_info *fi);

int hook_unlink(const char *path);
int hook_rename(const char *path, const char *dest);

int hook_lock (const char *path, struct fuse_file_info *fi, int cmd, struct flock *fl);

