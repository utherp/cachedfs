#pragma once
#include "common.h"

int hook_readlink(const char *path, char *buf, size_t bufsize);
int hook_symlink(const char *path, const char *dest);
int hook_link(const char *path, const char *dest);

