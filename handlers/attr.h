#pragma once
#include "common.h"

int hook_statfs (const char *path, struct statvfs *fsstat);
int hook_fgetattr (const char *path, struct stat *buf, struct fuse_file_info *fi);
int hook_getattr(const char *path, struct stat *stbuf);
int hook_chmod(const char *path, mode_t fmode);
int hook_chown(const char *path, uid_t fuid, gid_t fgid);

