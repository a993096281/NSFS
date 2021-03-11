CC = gcc
PROM = generalfs
SOURCE = generalfs.c log.c

$(PROM): $(SOURCE)
	$(CC) -Wall -g -lm -o $(PROM) $(SOURCE) `pkg-config fuse3 --cflags --libs`

default:  $(PROM)

clean: 
	rm -f generalfs
