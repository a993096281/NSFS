#! /bin/sh

mountdir="/home/lzw/fs/mnt"
metadir="/home/lzw/fs/meta"
datadir="/home/lzw/fs/data"

if [ -n "$metadir" ];then
    rm -f $metadir/*
fi

if [ -n "$datadir" ];then
    rm -f $datadir/*
fi

cmd="./nsfs_main -mount_dir $mountdir -meta_dir $metadir -data_dir $datadir"

echo $cmd
eval $cmd