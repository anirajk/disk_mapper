#!/usr/bin/env python
# Description: Remove a host entry from diskmapper host mapping

import fcntl
import os
import sys
import pickle
from pwd import getpwnam

HOST_MAPPING_PATH = '/var/tmp/disk_mapper/host.mapping'
HOST_MAPPING_LOCK_PATH = '/var/tmp/disk_mapper/host.mapping.lock'
USER = 'apache'

def read_mapping(filename):
    file_content = {}
    if os.path.exists(filename):
        f = open(filename)
        try:
            file_content = pickle.load(f)
        except:
            pass
        f.close()
    return file_content

def write_mapping(filename, data):
    if os.path.exists(filename):
        os.remove(filename)
        f = open(filename, 'w')
        pickle.dump(data, f)
        f.close()

def acquire_lock(lock_file):
    lockfd = open(lock_file, 'w')
    fcntl.flock(lockfd.fileno(), fcntl.LOCK_EX)
    return lockfd

def release_lock(fd):
    fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    fd.close()

def remove_mapping(hostname):
    lockfd = acquire_lock(HOST_MAPPING_LOCK_PATH)
    mapping = read_mapping(HOST_MAPPING_PATH)
    replaced = 0

    for srv, disks in mapping.items():
        for disk, roles in disks.items():
            for role, host in roles.items():
                if host == hostname:
			roles[role] = "%s-(disk-bad)" %(host)
                        replaced += 1
    if replaced == 0:
        print "Unable to find host in the mapping"
    elif replaced == 2:
        write_mapping(HOST_MAPPING_PATH, mapping)
        print "Successfully removed host from the mapping"
    else:
	print "Primary or Secondary of host cannot be found"

    release_lock(lockfd)


if __name__ == '__main__':

    if os.getuid() != 0:
        print "Please run as root"
        sys.exit(1)
    else:
        uid = getpwnam(USER).pw_uid
        os.seteuid(uid)

    if len(sys.argv) == 2:
        host = sys.argv[1]
        remove_mapping(host)
    else:
        print "Invalid args"
        print "Usage: %s hostname" %sys.argv[0]
        sys.exit(1)
