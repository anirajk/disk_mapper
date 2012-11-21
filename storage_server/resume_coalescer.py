#!/usr/bin/env python

import sys
from lib.storageserver import StorageServer

ss = StorageServer(None, None)
if 1 in sys.argv:
	ss.resume_coalescer(sys.argv[1])
