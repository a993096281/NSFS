
# Creates a fileset with 20,000 entries ($nfiles), but only preallocates
# 50% of the files.  Each file's size is set via a gamma distribution with
# a median size of 1KB ($filesize).
#
# The single thread then creates a new file and writes the whole file with
# 1MB I/Os.  The thread stops after 5000 files ($count/num of flowops) have
# been created and written to.

set $dir=/pmem0/fs/mnt
set $count=10
set $filesize=0
set $iosize=1kb
set $meandirwidth=100000
set $nfiles=4
set $nthreads=1

define fileset name=bigfileset,path=$dir,size=$filesize,entries=$nfiles,dirwidth=$meandirwidth,prealloc=50

define process name=filecreate,instances=1
{
  thread name=filecreatethread,memsize=10m,instances=$nthreads
  {
    flowop createfile name=createfile1,filesetname=bigfileset,fd=1
    flowop writewholefile name=writefile1,filesetname=bigfileset,fd=1,iosize=$iosize
    flowop closefile name=closefile1,fd=1
    flowop finishoncount name=finish,value=$count
  }
}

echo  "FileMicro-Createfiles Version 2.2 personality successfully loaded"
