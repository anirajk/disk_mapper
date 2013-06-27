#!/usr/bin/env python

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

import time
import os
import logging
from lib.diskmapper import DiskMapper
from config import config

logger = logging.getLogger('disk_mapper_daemon')
hdlr = logging.FileHandler('/var/log/disk_mapper.log')
formatter = logging.Formatter('%(asctime)s %(process)d %(thread)d %(filename)s %(lineno)d %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)

logger.setLevel(logging.INFO)
poll_interval = 5

if "params" in config.keys():
    if "log_level" in config["params"].keys():
        log_level = config["params"]["log_level"]
        if log_level == "info":
            logger.setLevel(logging.INFO)
        elif log_level == "error":
            logger.setLevel(logging.ERROR)
        elif log_level == "debug":
            logger.setLevel(logging.DEBUG)

    if poll_interval in config["params"].keys():
        poll_interval = config["params"]["poll_interval"]

def is_daemon_stopped():
    if not os.path.exists("/var/run/disk_mapper.lock"):
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
            logger.info("====Active Disk Mapper===")
            logger.info("Polling for copy completed.")
            is_daemon_stopped()
            dm.check_copy_complete()
            logger.info("Polling to delete merged files.")
            is_daemon_stopped()
            dm.delete_merged_files()
            logger.info("Polling for bad disks.")
            is_daemon_stopped()
            dm.swap_bad_disk()
            logger.info("Polling to enable replication.")
            is_daemon_stopped()
            dm.enable_replication()
            
        logger.info("Polling storage server for config.")
        is_daemon_stopped()
        dm.initialize_diskmapper()
        logger.debug(dm._get_mapping("storage_server"))
        logger.debug("===")
        logger.debug(dm._get_mapping("host"))
        time.sleep(poll_interval)
except Exception, e:
    print e
    logger.error(e)
    os.remove("/var/run/disk_mapper.lock")
    raise e
