#!/usr/bin/env python

import time
from lib.diskmapper import DiskMapper

dm = DiskMapper(None, None)
print "Initializing"
dm.initialize_diskmapper()
print dm._get_mapping("host")
while True:
    if dm.is_dm_active() == True:
        print "====Active Disk Mapper==="
        print "Enabling replication"
        dm.enable_replication()
        print "Swapping bad disks"
        dm.swap_bad_disk()

    print "Polling storage server for config."
    dm.initialize_diskmapper(True)
    #print dm._get_mapping("storage_server")
    #time.sleep(10)
