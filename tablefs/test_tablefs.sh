#! /bin/sh

mountdir="/home/lzw/fs/mnt"
metadir="/home/lzw/fs/meta"
datadir="/home/lzw/fs/data"

cmd="./tablefs_main -mount_dir $mountdir -meta_dir $metadir -data_dir $datadir"

echo $cmd
eval $cmd