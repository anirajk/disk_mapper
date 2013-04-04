#!/bin/bash

function parseArgs() {
  # Setup your arguments here.
  while getopts 'i:n:h' OPTION; do
    case $OPTION in
      v) VBS_PER_DISK=$OPTARG
         ;;
      t) TOTAL_VBS=$OPTARG
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

  if [[ -z $VBS_PER_DISK || -z $TOTAL_VBS ]]; then
    usage
    exit 1
  fi
}

function usage() {
  # Output script usage.
  cat << EOF
  Usage: ${0##*/} OPTIONS

  OPTIONS:
    -v  Number of vbuckets per disk on the storage server.
    -t  Total number of vbuckets in the entire pool
    -h  Show this message.
EOF
}

function main() {
  parseArgs $@
  disk_count=0
  vb_id=0
  vb_group_count=$(($TOTAL_VBS / $VBS_PER_DISK))
  touch /tmp/dm_init_emp_file

  for i in `seq 0 $TOTAL_VBS` ; do 
    for j in `



  done
  curl -sf --connect-timeout 15 --max-time 120 --request POST http://10.36.193.156/api/test_game/vb_group2/vb_2/.valid
  curl -sf -L --connect-timeout 15 --max-time 600 --request POST --data-binary @/tmp/dm_init_emp_file http://10.36.173.144/api/test_game/vb_group2/vb_2/.valid

}

# We don't want to call main, if this file is being sourced.
if [[ $BASH_SOURCE == $0 ]]; then
  main $@
fi
