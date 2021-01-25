#! /bin/sh

#db="/home/lzw/ceshi"

value_size="16"
benchmarks="dir_fillrandom" 

nums="1000000"  #
#nums="10000000"  #1千万，0.32G

reads="1000000"  #
deletes="1000000" #
updates="1000000" #
threads="1"

histogram="1"

const_params=""

function FILL_PATAMS() {
    if [ -n "$db" ];then
        const_params=$const_params"--db=$db "
    fi

    if [ -n "$value_size" ];then
        const_params=$const_params"--value_size=$value_size "
    fi

    if [ -n "$benchmarks" ];then
        const_params=$const_params"--benchmarks=$benchmarks "
    fi

    if [ -n "$nums" ];then
        const_params=$const_params"--nums=$nums "
    fi

    if [ -n "$reads" ];then
        const_params=$const_params"--reads=$reads "
    fi

    if [ -n "$deletes" ];then
        const_params=$const_params"--deletes=$deletes "
    fi

    if [ -n "$updates" ];then
        const_params=$const_params"--updates=$updates "
    fi

    if [ -n "$threads" ];then
        const_params=$const_params"--threads=$threads "
    fi

    if [ -n "$histogram" ];then
        const_params=$const_params"--histogram=$histogram "
    fi

    if [ -n "$k_DIR_FIRST_HASH_MAX_CAPACITY" ];then
        const_params=$const_params"--k_DIR_FIRST_HASH_MAX_CAPACITY=$k_DIR_FIRST_HASH_MAX_CAPACITY "
    fi

    if [ -n "$k_DIR_LINKNODE_TRAN_SECOND_HASH_NUM" ];then
        const_params=$const_params"--k_DIR_LINKNODE_TRAN_SECOND_HASH_NUM=$k_DIR_LINKNODE_TRAN_SECOND_HASH_NUM "
    fi

    if [ -n "$k_DIR_SECOND_HASH_INIT_SIZE" ];then
        const_params=$const_params"--k_DIR_SECOND_HASH_INIT_SIZE=$k_DIR_SECOND_HASH_INIT_SIZE "
    fi

    if [ -n "$k_DIR_SECOND_HASH_TRIG_REHASH_TIMES" ];then
        const_params=$const_params"--k_DIR_SECOND_HASH_TRIG_REHASH_TIMES=$k_DIR_SECOND_HASH_TRIG_REHASH_TIMES "
    fi

    if [ -n "$k_INODE_MAX_ZONE_NUM" ];then
        const_params=$const_params"--k_INODE_MAX_ZONE_NUM=$k_INODE_MAX_ZONE_NUM "
    fi

    if [ -n "$k_INODE_HASHTABLE_INIT_SIZE" ];then
        const_params=$const_params"--k_INODE_HASHTABLE_INIT_SIZE=$k_INODE_HASHTABLE_INIT_SIZE "
    fi

    if [ -n "$k_node_allocator_path" ];then
        const_params=$const_params"--k_node_allocator_path=$k_node_allocator_path "
    fi

    if [ -n "$k_node_allocator_size" ];then
        const_params=$const_params"--k_node_allocator_size=$k_node_allocator_size "
    fi

    if [ -n "$k_k_file_allocator_path" ];then
        const_params=$const_params"--k_k_file_allocator_path=$k_k_file_allocator_path "
    fi

    if [ -n "$k_file_allocator_size" ];then
        const_params=$const_params"--k_file_allocator_size=$k_file_allocator_size "
    fi

    if [ -n "$k_thread_pool_count" ];then
        const_params=$const_params"--k_thread_pool_count=$k_thread_pool_count "
    fi

}


bench_file_path="$(dirname $PWD )/db_bench"

if [ ! -f "${bench_file_path}" ];then
bench_file_path="$PWD/db_bench"
fi

if [ ! -f "${bench_file_path}" ];then
echo "Error:${bench_file_path} or $(dirname $PWD )/db_bench not find!"
exit 1
fi

FILL_PATAMS 

cmd="$bench_file_path $const_params "

if [ -n "$1" ];then    #后台运行
cmd="nohup $bench_file_path $const_params >>out.out 2>&1 &"
echo $cmd >out.out
fi

echo $cmd
eval $cmd
