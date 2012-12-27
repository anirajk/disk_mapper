#!/usr/bin/env python

import sys
from lib.storageserver import StorageServer

ss = StorageServer(None, None)
if len(sys.argv) == 2:
    ss.resume_coalescer(sys.argv[1])
