#!/bin/bash

python /opt/storage_server/resume_coalescer.py $3
echo $3 >> /var/tmp/disk_mapper/copy_completed
