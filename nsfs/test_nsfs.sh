#! /bin/sh

mountdir="/pmem0/fs/mnt"
metadir="/pmem0/fs/meta"
datadir="/pmem0/fs/data"

if [ -n "$metadir" ];then
    rm -f $metadir/*
fi

if [ -n "$datadir" ];then
    rm -f $datadir/*
fi

cmd="./nsfs_main -mount_dir $mountdir -meta_dir $metadir -data_dir $datadir"

echo $cmd
eval $cmd