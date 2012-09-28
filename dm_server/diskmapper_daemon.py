#!/usr/bin/env python

import sys
import time
import logging
from lib.diskmapper import DiskMapper
from lib.daemon import Daemon

import logging

logger = logging.getLogger('disk_mapper')
hdlr = logging.FileHandler('/var/log/disk_mapper.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

class MyDaemon(Daemon):
    def run(self):

        dm = DiskMapper(None, None)

        logger.setLevel(logging.INFO)
        logger.info("Initializing...")
        dm.initialize_diskmapper()
        while True:
            logger.info("Enabling replication")
            dm.enable_replication()
            logger.info("Swapping bad disks")
            dm.swap_bad_disk()
            logger.info("Polling storage server for config.")
            dm.initialize_diskmapper(True)
            time.sleep(10)

if __name__ == "__main__":
    daemon = MyDaemon('/tmp/daemon-example.pid', stdout="/tmp/jnk.log", stderr="/tmp/jnk.log")
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
