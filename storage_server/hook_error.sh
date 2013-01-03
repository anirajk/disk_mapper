#!/bin/bash

file_path="$3"
disk=$(echo $file_path | cut -d"/" -f2)

echo $file_path >> /$disk/to_be_deleted
echo $file_path.aria2 >> /$disk/to_be_deleted

