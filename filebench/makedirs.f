#
# Creates a directory with $ndirs potential leaf directories, than mkdir's them
#
set $dir=/tmp
set $ndirs=10
set $meandirwidth=1
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

run 10
