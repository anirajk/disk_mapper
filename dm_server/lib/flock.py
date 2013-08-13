#!/bin/env python

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

import os
import socket

class flock(object):
    '''Class to handle creating and removing (pid) lockfiles'''

    # custom exceptions
    class FileLockAcquisitionError(Exception): pass
    class FileLockReleaseError(Exception): pass

    # convenience callables for formatting
    addr = lambda self: '%d@%s' % (self.pid, self.host)
    fddr = lambda self: '<%s %s>' % (self.path, self.addr())
    pddr = lambda self, lock: '<%s %s@%s>' %\
                              (self.path, lock['pid'], lock['host'])

    def __init__(self, path, debug=None):
        self.pid   = os.getpid()
        self.host  = socket.gethostname()
        self.path  = path
        self.debug = debug # set this to get status messages

    def acquire(self):
        '''Acquire a lock, returning self if successful, False otherwise'''
        if self.islocked():
            if self.debug:
                lock = self._readlock()
                print 'Previous lock detected: %s' % self.pddr(lock)
            return False
        try:
            fh = open(self.path, 'w')
            fh.write(self.addr())
            fh.close()
            if self.debug:
                print 'Acquired lock: %s' % self.fddr()
        except:
            if os.path.isfile(self.path):
                try:
                    os.unlink(self.path)
                except:
                    pass
            raise (self.FileLockAcquisitionError,
                   'Error acquiring lock: %s' % self.fddr())
        return self

    def release(self):
        '''Release lock, returning self'''
        if self.ownlock():
            try:
                os.unlink(self.path)
                if self.debug:
                    print 'Released lock: %s' % self.fddr()
            except:
                raise (self.FileLockReleaseError,
                       'Error releasing lock: %s' % self.fddr())
        return self

    def _readlock(self):
        '''Internal method to read lock info'''
        try:
            lock = {}
            fh   = open(self.path)
            data = fh.read().rstrip().split('@')
            fh.close()
            lock['pid'], lock['host'] = data
            return lock
        except:
            return {'pid': 8**10, 'host': ''}

    def islocked(self):
        '''Check if we already have a lock'''
        try:
            lock = self._readlock()
            os.kill(int(lock['pid']), 0)
            return (lock['host'] == self.host)
        except:
            return False

    def ownlock(self):
        '''Check if we own the lock'''
        lock = self._readlock()
        return (self.fddr() == self.pddr(lock))

    def __del__(self):
        '''Magic method to clean up lock when program exits'''
        self.release()
