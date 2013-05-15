#!/bin/bash
#
# This script deletes old master and daily backups.
#
# This script is to be run on the storage servers.
# Threshold : 
#	-d $NUM_DAILY_BACKUP : Number of daily backups to keep.
#		The latest $NUM_DAILY_BACKUP number of daily backups are kept.
#
#	-m $NUM_MASTER_BACKUP : Number of months of master backups to be kept.
#		Master backup will be kept for $NUM_MASTER_BACKUP number of masters.
#
#	-s $NUM_SELECTIVE_BACKUP : Number of months of selective master backups to be kept.
#		First master backup of the month will be kept for $NUM_SELECTIVE_BACKUP months before "now - $NUM_MASTER_BACKUP" months.
#
#   -t Test run : Will only display files to be deleted won't delete the files.	
#
# Copyright 2010 Zynga, Inc. All rights reserved

AUTHOR='Shabeeb Qadir <sqadir@zynga.com>'

# Defaults
MESSAGE_TAG="MembaseBackup"
CLOUD="$(egrep '^cloud =' /etc/membase-backup/default.ini | awk '{print $NF}')"
GAME_ID="$(egrep '^game_id =' /etc/membase-backup/default.ini | awk '{print $NF}')"
HOST_PATH_PREFIX="/var/www/html/"


# Parse arguments to the script.
function parseArgs() {
    # Setup your arguments here.
    while getopts 'd:m:s:th' OPTION; do
        case $OPTION in
            d) NUM_DAILY_BACKUP=$OPTARG
                 ;;
            m) NUM_MASTER_BACKUP=$OPTARG
                 ;;
            s) NUM_SELECTIVE_BACKUP=$OPTARG
                 ;;
            t) TEST_RUN=true
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

    if [[ -z $NUM_DAILY_BACKUP && -z $NUM_MASTER_BACKUP ]]; then
        usage
        logIt "Invalid arguments passed to ${0##*/}"
        exit 1
    fi

    if [[ -z $NUM_SELECTIVE_BACKUP ]]; then 
        NUM_SELECTIVE_BACKUP=0
    fi
}

# Log function, $1 is message.
function logIt() {
    logger -t $MESSAGE_TAG $1
}

function usage() {
    # Output script usage.
    cat << EOF
    Usage: ${0##*/} OPTIONS

    OPTIONS:
    -d Number of daily backups to keep.
    -m Number of months of master backups to be kept.
    -s Number of months of selective master backups to be kept.
    -h Show this message.
EOF
}

function main() {
    parseArgs $@

    if [[ -z $TEST_RUN ]]; then
        logIt "Started ${0##*/}"
    else 
        logIt "Started ${0##*/} in dry run mode"
    fi

    game_path="$HOST_PATH_PREFIX$GAME_ID"
    hosts=$(ls $game_path)

    # Daily backups.
    if [[ ! -z $NUM_DAILY_BACKUP ]]; then
        [[ $NUM_DAILY_BACKUP -eq 0 ]] && logIt "\-d should be non-zero." && exit 1

        dailys_to_be_del=""
        for host in $hosts ; do dailys_to_be_del="$dailys_to_be_del $(find $game_path/$host/$CLOUD/daily/ | grep -v "mbb\|split\|done\|daily\/$" | sort -n | head -n -$NUM_DAILY_BACKUP)"  ;  done
        
        # Delete daily backups.
        deleteBackups "Daily backups to be deleted are" "$dailys_to_be_del" 
    else 
        logIt '\-d parameter not set, not deleting daily backups.'
    fi


    [[ -z $NUM_MASTER_BACKUP ]] && logIt '\-m parameter not set, not deleting master backups.' && exit 1
    [[ $NUM_MASTER_BACKUP -eq 0 ]] && logIt "\-m should be non-zero." && exit 1

    # Master backups.
    master_backups=$(find /var/www/html/membase_backup/$GAME_ID/*/$CLOUD/master/ | grep -v "mbb\|split\|done\|merged\|master\/$" | sort -n)

    # Generating a list of months for which backups are to be kept.
    
    tmp_date="" 
    now=`date +"%Y-%m-%d" -d "now"` 
    end=`date +"%Y-%m" -d "$( expr $NUM_MASTER_BACKUP + $NUM_SELECTIVE_BACKUP ) months ago"`

    date_range=$(date +"%Y-%m" -d "$now")  

    while [ "$tmp_date" != "$end" ] ; do now=`date +"%Y-%m-%d" --date "$now 1 month ago"`;  tmp_date=`date +"%Y-%m" --date "$now"` ; date_range="$date_range\|$tmp_date" ;done

    # Get list of master backups that are not in the date range. These are the files that need to be deleted.
    masters_to_be_del=$(grep -v "$date_range" <(echo "$master_backups"))

    # Delete master backups.
    deleteBackups "Master backups to be deleted are" "$masters_to_be_del"

    # Keep first backup of the month for $NUM_SELECTIVE_BACKUP months.
    [[ $NUM_SELECTIVE_BACKUP -eq 0 ]] && logIt "\-s is zero, not deleting selective backups." && exit 1

    # Generate months for which selective backup is to be kept.
    tmp_date=""
    now=`date +"%Y-%m-%d" -d "$NUM_MASTER_BACKUP month ago"` 
    end=`date +"%Y-%m" -d "$(expr $NUM_MASTER_BACKUP + $NUM_SELECTIVE_BACKUP ) months ago"`   
    date_range=""
    while [ "$tmp_date" != "$end" ] ; do now=`date +"%Y-%m-%d" --date "$now 1 month ago"`;  tmp_date=`date +"%Y-%m" --date "$now"` ; date_range="$date_range $tmp_date" ;done

    # For each month keep the first backup.
    echo "$hosts" | while read host 
    do

        master_backups=""
        master_backups=$(find /var/www/html/membase_backup/$GAME_ID/$host/$CLOUD/master/ | grep -v "mbb\|split\|done\|merged\|master\/$" | sort -n)
        
        for month in $date_range 
        do
            backups=$(grep "$month" <(echo "$master_backups") | sort -n ) 
            count=$(echo "$backups" | wc -l)
            selective_to_be_del=$(echo "$backups" | tail -n $(expr $count - 1))
            deleteBackups "Selective backups to be deleted are" "$selective_to_be_del"
        done
    done

}

function deleteBackups() {
    message=$1
    files=$2

    if [[ -z $TEST_RUN ]]; then
        logIt "$message : \n [ $files ] \n"
        rm -rf $files
    else
        echo -e "$message (Test mode, won't delete): \n [ $files ] \n"
    fi
}
function exitOnError() {
    echo $2
    exit $1
}

# We don't want to call main, if this file is being sourced.
if [[ $BASH_SOURCE == $0 ]]; then
    main $@
fi
