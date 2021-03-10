set $dir=/pmem0/fs/mnt
set $nfiles=5
set $meandirwidth=1
set $nthreads=1

define fileset name=bigfileset,path=$dir,size=0,entries=$nfiles,dirwidth=$meandirwidth,prealloc

define process name=fileopen,instances=1
{
  thread name=fileopener,memsize=1m,instances=$nthreads
  {
    flowop openfile name=open1,filesetname=bigfileset,fd=1
    flowop closefile name=close1,fd=1
  }
}

echo  "Openfiles Version 1.0 personality successfully loaded"
run 20