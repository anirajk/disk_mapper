#!/usr/bin/env python
# Description: Cleanup a file from empty spaces in a threadsafe manner
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

import fcntl
import os
import sys

def acquire_lock(lock_file):
    lockfd = open(lock_file, 'w')
    fcntl.flock(lockfd.fileno(), fcntl.LOCK_EX)
    return lockfd

def release_lock(fd):
    fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    fd.close()

def cleanup_file(file):
    lockfd = acquire_lock("%s.lock"%file)
    cleanup_cmd = "sed -i '/^[[:space:]]*$/d' %s"%file
    os.system(cleanup_cmd)
    release_lock(lockfd)


if __name__ == '__main__':

    if os.getuid() != 0:
        print "Please run as root"
        sys.exit(1)
    if len(sys.argv) == 2:
        file_name = sys.argv[1]
        cleanup_file(file_name)
    else:
        print "Invalid args"
        print "Usage: %s filename" %sys.argv[0]
        sys.exit(1)
