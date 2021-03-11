#! /bin/sh

realdir="/pmem0/fs/data"
mountdir="/pmem0/fs/mnt"


if [ -n "$realdir" ];then
    rm -rf $realdir/*
fi


cmd="./generalfs $realdir $mountdir "

echo $cmd
eval $cmd