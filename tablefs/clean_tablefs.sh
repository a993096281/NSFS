#! /bin/sh

mountdir="/home/lzw/fs/mnt"

killall tablefs_main
fusermount -u $mountdir