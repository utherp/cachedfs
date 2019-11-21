#include "link.h"
#include <stdint.h>
#include <unistd.h>

/**********************************************************/

int hook_readlink(const char *path, char *buf, size_t bufsize) {
    _debug_flow("entered readlink on '%s'", path);
    path++;

    int dtype = get_magic_dir_type(&path);

    if (dtype == PEEK_TYPE)
        fchdir(dirfd(cachedfs->cache_dir));
    else if (dtype == INFO_TYPE)
        return -ENOENT;
    else
        fchdir(dirfd(cachedfs->source_dir));

    int bytes = readlink(path, buf, bufsize);
    if (bytes == -1) return -errno;
    return 0;
}

/**********************************************************/

int hook_symlink(const char *path, const char *dest) {
    _debug_flow("entered symlink on '%s'", path);
//  fchdir(dirfd(mount_dir));
//  if (symlink(dest, path+1)) return (errno*-1);
    return -ENOSYS;

}

/**********************************************************/

int hook_link(const char *path, const char *dest) {
    _debug_flow("entered link on '%s'", path);
//  fchdir(dirfd(mount_dir));
//  if (link(path+1, dest)) return (errno*-1);
    return -ENOSYS;
}

/**********************************************************/

