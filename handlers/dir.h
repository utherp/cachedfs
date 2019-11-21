#pragma once
#include "common.h"

int hook_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
int hook_mkdir(const char *path, mode_t fmode);
int hook_rmdir(const char *path);

