/*
  refer to Joseph J. Pfeiffer, Jr., Ph.D.'s bbfs
  http://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial.tgz
*/

#ifndef _PARAMS_H_
#define _PARAMS_H_

#define FUSE_USE_VERSION 26

#define _XOPEN_SOURCE 500

#include <limits.h>
#include <stdio.h>
struct general_state {
    FILE *logfile;
    char *rootdir;
};
#define GENERAL_DATA ((struct general_state *) fuse_get_context()->private_data)

#endif
