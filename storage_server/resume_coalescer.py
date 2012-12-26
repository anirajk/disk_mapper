#!/usr/bin/env python

import sys
from lib.storageserver import StorageServer

ss = StorageServer(None, None)
ss.resume_coalescer(sys.argv[1])
