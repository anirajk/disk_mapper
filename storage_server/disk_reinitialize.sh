#!/bin/bash

BAD_DISK_FILE=/var/tmp/disk_mapper/bad_disk
BAD_DISK_LOCK_FILE=/var/tmp/disk_mapper/bad_disk.lock

if [ $UID -ne 0 ];
then
    echo Please run as root
    exit 1
fi

if [ $# -ne 1 ];
then
    echo "Usage: $0 /data_x"
    exit 1
fi

disk=$1

if [ -d $disk ];
then
    grep -q $disk /etc/mtab
    if [ $? -ne 0 ];
    then
        echo Specified disk, $disk is not mounted
        exit 1
    fi

    rm -rf $disk/*
    mkdir $disk/primary $disk/secondary
    ln -s $disk $disk/$disk
    chown storageserver -R $disk

    find -L /var/www/html/ -type l -delete 2> /dev/null
    exec 5>$BAD_DISK_LOCK_FILE
    flock -x 5
    chown storageserver $BAD_DISK_LOCK_FILE

    tmpfile=/tmp/$$.tmpfile
    > $tmpfile
    while read line;
    do
        if [[ ! $disk =~ $line ]];
        then
            echo $line >> $tmpfile
        fi
    done < $BAD_DISK_FILE

    mv $tmpfile $BAD_DISK_FILE
    chown storageserver $BAD_DISK_FILE

    flock -u 5

    echo Successfully re-initialized disk.
else
    echo Disk $disk not found
    exit 1
fi


