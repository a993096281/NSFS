CC = gcc
CXX = g++
LDFLAGS = -pthread -g 
DEFS = 
CFLAGS = -I./include
CFLAGS += $(DEFS)
AR = ar
ARFLAGS = rs
LIB_SOURCES =  \
	db/dir_db.cc  \
	db/dir_hashtable.cc  \
	db/dir_nvm_node.cc  \
	db/inode_db.cc  \
	db/inode_hashtable.cc  \
	db/inode_zone.cc  \
	db/metadb.cc  \
	db/nvm_file_allocator.cc  \
	db/nvm_node_allocator.cc  \
	db/thread_pool.cc  
	
TEST_TARGET = tests/db_bench \

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(LIB_SOURCES:.cc=)

LIBRARY = libmetadb.a

.PHONY: clean
default:  all

all:  clean $(LIBRARY) 

clean: 
	rm -f $(LIBRARY)
	rm -f $(LIBOBJECTS)
	rm -f $(TEST_TARGET)
	rm -f test
	rm -f db_bench

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CXX) $(CFLAGS) $(LDFLAGS) -c $$sou_file.cc -o $$sou_file.o; \
	done;

$(LIBRARY) : $(LIBOBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $^

$(TEST_TARGET) :
	for test_file in $(TEST_TARGET) ; do \
	$(CXX) $(CFLAGS) $$test_file.cc -o $$test_file $(LIBRARY) $(LDFLAGS); \
	done;
benchmark: clean $(LIBRARY)
	$(CXX) $(CFLAGS) tests/db_bench.c $(LIBRARY) -o db_bench $(LDFLAGS);


test: clean $(LIBRARY)
	$(CXX) $(CFLAGS) tests/test.c $(LIBRARY) -o test $(LDFLAGS);

ceshi:
	$(CXX) $(CFLAGS) tests/ceshi.c  src/utils/rwlock.c -o test $(LDFLAGS);

INSTALL_PATH := /usr/local
INCLUDE_PATH := metadb

install-headers:
	install -d $(INSTALL_PATH)/lib
	install -d $(INSTALL_PATH)/include/$(INCLUDE_PATH)
	for header_dir in `find "include" -type d | sed '1d' | sed 's/^include\///g' `; do \
		install -d $(INSTALL_PATH)/include/$(INCLUDE_PATH)/$$header_dir; \
	done
	for header in `find "include/" -type f -name "*.h" | sed 's/^include\///g' `; do \
		install -C -m 644 include/$$header $(INSTALL_PATH)/include/$(INCLUDE_PATH)/$$header; \
	done

install-static: $(LIBRARY) install-headers 
	install -C -m 755 $(LIBRARY) $(INSTALL_PATH)/lib

install:install-static

uninstall:
	rm -rf $(INSTALL_PATH)/include/$(INCLUDE_PATH) \
	$(INSTALL_PATH)/lib/$(LIBRARY) \
