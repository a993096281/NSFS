#
# Creates a directory with $ndirs potential leaf directories, than mkdir's them
# 
# depth = 3, 5, 7, 9,
# meandirwidth=100, 16, 7, 5
#创建目录，创建完目录就停止


set $dir=/pmem0/fs/mnt
set $ndirs=1000000
set $meandirwidth=100
set $nthreads=1

set mode quit firstdone

define fileset name=bigfileset,path=$dir,size=0,leafdirs=$ndirs,dirwidth=$meandirwidth

define process name=dirmake,instances=1
{
  thread name=dirmaker,memsize=1m,instances=$nthreads
  {
    flowop makedir name=mkdir1,filesetname=bigfileset
  }
}

echo  "MakeDirs Version 1.0 personality successfully loaded"

run 20
