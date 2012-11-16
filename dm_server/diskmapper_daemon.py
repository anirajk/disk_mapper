#!/usr/bin/env python

import time
import os
import logging
from lib.diskmapper import DiskMapper

logger = logging.getLogger('disk_mapper_daemon')
hdlr = logging.FileHandler('/var/log/disk_mapper.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

def is_daemon_stopped():
	if not os.path.exists("/var/run/disk_mapper"):
		logger.info("=== Disk Mapper Stopped ===")
		exit()

try:
	logger.info("=== Disk Mapper Started ===")
	is_daemon_stopped()
	dm = DiskMapper(None, None)
	logger.info("Initializing DiskMapper")
	dm.initialize_diskmapper()
	logger.info(dm._get_mapping("host"))
	while True:
		if dm.is_dm_active() == True:
			logger.debug("====Active Disk Mapper===")
			logger.debug("Enabling replication")
			is_daemon_stopped()
			dm.enable_replication()
			logger.debug("Swapping bad disks")
			is_daemon_stopped()
			dm.swap_bad_disk()

		logger.debug("Polling storage server for config.")
		is_daemon_stopped()
		dm.initialize_diskmapper(True)
		logger.debug(dm._get_mapping("storage_server"))
		logger.debug("===")
		logger.debug(dm._get_mapping("host"))
except:
	os.remove("/var/run/disk_mapper")
