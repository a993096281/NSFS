#
# Creates a fileset with a fairly deep directory tree, then does readdir
# operations on them for a specified amount of time
#
#

set $dir=/pmem0/fs/mnt
set $nfiles=81
set $meandirwidth=3
set $nthreads=1

define fileset name=bigfileset,path=$dir,size=0,entries=$nfiles,dirwidth=$meandirwidth,prealloc

define process name=lsdir,instances=1
{
  thread name=dirlister,memsize=1m,instances=$nthreads
  {
    flowop listdir name=open1,filesetname=bigfileset
  }
}

echo  "ListDirs Version 1.0 personality successfully loaded"
run 20