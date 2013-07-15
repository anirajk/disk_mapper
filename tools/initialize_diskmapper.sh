#!/bin/bash

#   Copyright 2013 Zynga Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

function parseArgs() {
  # Setup your arguments here.
  while getopts 'v:t:i:g:h' OPTION; do
    case $OPTION in
      v) VBS_PER_DISK=$OPTARG
         ;;
      t) TOTAL_VBS=$OPTARG
         ;;
      i) DISK_MAPPER_IP=$OPTARG
         ;;
      g) GAME_ID=$OPTARG
         ;;
      h) usage
         exit 0
         ;;
      *) echo 'Invalid option.'
         usage
         exit 3
        ;;
    esac
  done

  if [[ -z $VBS_PER_DISK || -z $TOTAL_VBS || -z $DISK_MAPPER_IP || -z $GAME_ID ]]; then
    usage
    exit 1
  fi
}

function usage() {
  # Output script usage.
  cat << EOF
  Usage: ${0##*/} OPTIONS

  OPTIONS:
    -i  Disk Mapper IP.
    -g  Game ID.
    -v  Number of vbuckets per disk on the storage server.
    -t  Total number of vbuckets in the entire pool.
    -h  Show this message.
EOF
}

function main() {
  parseArgs $@
  vb_id=0
  vb_group_id=0
  vb_group_count=$(($TOTAL_VBS / $VBS_PER_DISK))
  vb_remaining=$(($TOTAL_VBS % $VBS_PER_DISK))

  if [ $vb_remaining -ne 0 ];
  then
      let vb_group_count++
  fi

  echo valid > /tmp/dm_init_emp_file

  while [ $vb_group_id -lt $vb_group_count ] ; do

    actual_url=$(curl -sf --connect-timeout 15 --max-time 120 --request POST http://$DISK_MAPPER_IP/api/$GAME_ID/vb_group_$vb_group_id/)
    i=0
    while [ $i -lt $VBS_PER_DISK -a $(( $i + $vb_group_id*$VBS_PER_DISK )) -lt $TOTAL_VBS ] ; do
      
      curl -sf -L --connect-timeout 15 --max-time 600 --request POST --data-binary @/tmp/dm_init_emp_file $actual_url/vb_$vb_id/valid
      let vb_id=vb_id+1
      let i=i+1
    done
    let vb_group_id=vb_group_id+1
  done

}

# We don't want to call main, if this file is being sourced.
if [[ $BASH_SOURCE == $0 ]]; then
  main $@
fi
