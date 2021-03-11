#
# Creates a fileset with $ndirs empty leaf directories then rmdir's all of them
#

set $dir=/pmem0/fs/mnt
set $ndirs=125
set $meandirwidth=5
set $nthreads=1

set mode quit firstdone

define fileset name=bigfileset,path=$dir,size=0,leafdirs=$ndirs,dirwidth=$meandirwidth,prealloc

define process name=remdir,instances=1
{
  thread name=removedirectory,memsize=1m,instances=$nthreads
  {
    flowop removedir name=dirremover,filesetname=bigfileset
  }
}

echo  "RemoveDir Version 1.0 personality successfully loaded"
run 10