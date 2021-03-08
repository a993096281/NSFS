CC = gcc
CXX = g++
LDFLAGS = -pthread -g -Wformat=0 -lfuse
DEFS = -DNDEBUG
CFLAGS += $(DEFS)
AR = ar
ARFLAGS = rs
LIB_SOURCES =  \
	adaptor.cc  \
	config.cc  \
	inode_format.cc  \
	tablefs.cc  
	


LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(LIB_SOURCES:.cc=)

FUSE_LIBRARY = `pkg-config fuse --cflags --libs`

LIBRARY = libtablefs.a

LEVELDB_LIBRARY = ../lib/leveldb_v1.20/out-static/libleveldb.a
LEVELDB_INCLUDE = -I../lib/leveldb_v1.20/include

.PHONY: clean
default:  all

all:  clean $(LIBOBJECTS) 

clean: 
	rm -f $(LIBOBJECTS)
	rm -f tablefs_main
	

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CXX) $(CFLAGS) $(LDFLAGS) $(LEVELDB_INCLUDE) -c $$sou_file.cc -o $$sou_file.o; \
	done;

$(LIBRARY) : $(LIBOBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $^

tablefs_main: clean $(LIBOBJECTS)
	$(CXX) $(CFLAGS) tablefs_main.cc $(LIBOBJECTS) $(LEVELDB_INCLUDE) $(LEVELDB_LIBRARY) $(FUSE_LIBRARY) -o tablefs_main $(LDFLAGS);
