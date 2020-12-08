/*************************************************************************
* Author: Kai Ren
* Created Time: 2012-08-15 12:46:41
* File Name: ./fswrapper_test.cpp
* Description:
 ************************************************************************/

#include <vector>
#include <string>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <utime.h>
#include <string.h>
#include <unistd.h>
#include "port/port.h"
#include "fs/fswrapper.h"
#include "util/monitor.h"
#include "util/allocator.h"
#include "util/traceloader.h"
#include "util/properties.h"
#include "util/command.h"
#include "util/random.h"

#define ASSERT(x) \
  if (!((x))) { fprintf(stderr, "%s %d failed\n", __FILE__, __LINE__); exit(1); }

using namespace tablefs;

void TestFSWrapper(Properties &prop) {
  FileSystemWrapper *fs;
  fs = new TableFSWrapper();
  ASSERT(fs->Setup(prop) == 0);

  char fpath[128] = "/00000000";

  int content_size = 128;
  char *content = new char[content_size];
  for (int i = 0; i < content_size; ++i)
    content[i] = rand() % 26 + 97;
  int fd;
  for (int i = 0; i < 1000000; ++i) {
    sprintf(fpath, "/%08x\0", i);
    fs->Mknod(fpath, S_IRWXU | S_IRWXG | S_IRWXO, 0);
    fd = fs->Open(fpath, O_WRONLY);
    fs->Write(fd, content, content_size);
    fs->Close(fd);
  }

  delete fs;

  fs = new TableFSWrapper();
  ASSERT(fs->Setup(prop) == 0);

  fd = fs->Open(fpath, O_RDONLY);
  fs->Read(fd, content, content_size);
  fs->Close(fd);

  delete fs;
}

int main(int argc, char *argv[]) {
  Properties prop;
  prop.parseOpts(argc, argv);
  std::string config_filename = prop.getProperty("configfile");
  printf("%s\n", config_filename.c_str());
  if (config_filename.size() > 0) {
    prop.load(config_filename);
  }
  TestFSWrapper(prop);
  return 0;
}
