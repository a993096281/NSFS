CC = gcc
CXX = g++
LDFLAGS = -pthread -g -Wformat=0
DEFS = -DNDEBUG
CFLAGS += $(DEFS)
AR = ar
ARFLAGS = rs
LIB_SOURCES =  \
	adaptor.cc  \
	config.cc  \
	inode_format.cc  \
	nsfs.cc  
	


LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(LIB_SOURCES:.cc=)

FUSE_INCLUDE = `pkg-config fuse3 --cflags`
FUSE_LIBRARY = `pkg-config fuse3 --libs`


LIBRARY = libnsfs.a


METADB_LIBRARY = ../metadb/libmetadb.a -lpmem
METADB_INCLUDE = -I../metadb/include

.PHONY: clean
default:  all

all:  clean $(LIBOBJECTS) 

clean: 
	rm -f $(LIBOBJECTS)
	rm -f nsfs_main
	

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CXX) $(CFLAGS) $(LDFLAGS) $(METADB_INCLUDE) $(FUSE_INCLUDE) -c $$sou_file.cc -o $$sou_file.o; \
	done;

$(LIBRARY) : $(LIBOBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $^

nsfs_main: clean $(LIBOBJECTS)
	$(CXX) $(CFLAGS) nsfs_main.cc $(LIBOBJECTS) $(METADB_INCLUDE) $(METADB_LIBRARY) $(FUSE_INCLUDE) $(FUSE_LIBRARY) -o nsfs_main $(LDFLAGS);
