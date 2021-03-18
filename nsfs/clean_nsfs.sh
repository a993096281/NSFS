#! /bin/sh

mountdir="/pmem0/fs/mnt"

umount $mountdir
killall nsfs_main