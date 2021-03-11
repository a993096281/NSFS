#! /bin/sh

realdir="/home/lzw/fs/data"
mountdir="/home/lzw/fs/mnt"


if [ -n "$realdir" ];then
    rm -f $realdir/*
fi


cmd="./generalfs $realdir $mountdir "

echo $cmd
eval $cmd