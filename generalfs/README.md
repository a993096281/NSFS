# general_fs
This file system implementation is based on fuse. And it can be mounted on ext4, xfs, btrfs and other filesystems to build ext4-fuse, xfs-fuse, btrfs-fuse and so on!!!
> usage example :

	make
	./generalfs /mnt/ext4 /mnt/myfuse

/mnt/ext4是实际的本地文件系统。

/mnt/myfuse是fuse挂载的文件系统。

本程序只是在原有文件系统上加了一层fuse接口。
