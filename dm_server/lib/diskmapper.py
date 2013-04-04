#!/bin/env python
"""
This module maps request to function based on the url and method
"""
import os
import glob
import hashlib
import fcntl
import time
import json
import pickle
import threading
import socket
import logging
import httplib
import base64
import subprocess
from signal import SIGSTOP, SIGCONT
from config import config
from cgi import parse_qs

logger = logging.getLogger('disk_mapper')
hdlr = logging.FileHandler('/var/log/disk_mapper.log')
formatter = logging.Formatter('%(asctime)s %(process)d %(thread)d %(filename)s %(lineno)d %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)

logger.setLevel(logging.INFO)
if "params" in config.keys() and "log_level" in config["params"].keys():
	log_level = config["params"]["log_level"]
	if log_level == "info":
		logger.setLevel(logging.INFO)
	elif log_level == "error":
		logger.setLevel(logging.ERROR)
	elif log_level == "debug":
		logger.setLevel(logging.DEBUG)

def acquire_lock(lock_file):
    lockfd = open(lock_file, 'w')
    fcntl.flock(lockfd.fileno(), fcntl.LOCK_EX)
    return lockfd

def release_lock(fd):
    fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    fd.close()


class DiskMapper:

	def __init__(self, environ, start_response):
		self.mapping_file = '/var/tmp/disk_mapper/host.mapping'
		self.host_init_lock = '/var/tmp/disk_mapper/host.init.lock'
		self.mapping_lock = '/var/tmp/disk_mapper/host.mapping.lock'
		self.bad_servers = []
		if environ != None:
			self.environ  = environ
			self.query_string  = environ["QUERY_STRING"]
			self.start_response = start_response
			self.status = '400 Bad Request'
			self.response_headers = [('Content-type', 'text/plain')]
		if not os.path.exists("/var/run/disk_mapper.lock"):
			logger.info("=== Disk Mapper service is Not running ===")
			exit(1)

	def forward_request(self):
		self.status = '202 Accepted'
		path = self.environ["PATH_TRANSLATED"]
		request_uri = self.environ["REQUEST_URI"]

		logger.debug("Redirect request : " + request_uri)
		host_name =  path.split("/")[5]
		mapping = self._get_mapping("host", host_name, False)
			
		if mapping == False:
			logger.error("Failed to get mapping for : " + host_name)
			self.status = '200 OK'
			self._start_response()
			return "Host name " + host_name + " not found in mapping."
			
		#logger.debug("Mapping found for " + host_name + " : " + str(mapping))
		status = None
		if "primary" in mapping.keys():
			logger.info("Found primary for " + host_name)
			storage_server = mapping["primary"]["storage_server"]
			status = mapping["primary"]["status"]
			#logger.debug("Primary mapping : " + str(mapping["primary"]))

		if status == "bad" or status == None or status == "unprocessed_state":
			logger.info("Primary disk is not available or is bad.")
			if "secondary" in mapping.keys():
				logger.info("Found secondary for " + host_name)
				storage_server = mapping["secondary"]["storage_server"]
				status = mapping["secondary"]["status"]
				#logger.debug("Secondary mapping : " + str(mapping["secondary"]))
				if status == "bad" or status == None or status == "unprocessed_state":
					logger.error("Both primary and secondary are bad disks.")
					self.status = '412 Precondition Failed'
					self._start_response()
					return "Both primary and secondary are bad disks."
			
		url = 'http://' + storage_server + request_uri
		logger.info("Request redirected to : " + str(url))
		self.status = '302 FOUND'
		self.response_headers.append(("Location", str(url)))
		self._start_response()
		return str(url)

	def get_vbuckets(self, type=None, key=None):

		mapping = self._get_vbucket_mapping()
			
		if mapping == False:
			logger.error("Failed to get vbucket mapping.)
			self.status = '404 Not Found'
			self._start_response()
			return "Failed to get vbucket mapping."
			
		#mapping[vbucket].update({disk_type : { "disk" : disk, "vb_group" : vb_group, "status" : status, "storage_server" : storage_server}})

		status = None
		for vbucket in mapping:
			if "primary_vbs" in mapping[vbucket]:
				logger.info("Found primary for " + vbucket)
				storage_server = mapping[vbucket]["primary_vbs"]["storage_server"]
				vb_group = mapping[vbucket]["primary_vbs"]["vb_group"]
				disk = mapping[vbucket]["primary_vbs"]["disk"]
				status = mapping[vbucket]["primary_vbs"]["status"]
				type = "primary"
				#logger.debug("Primary mapping : " + str(mapping["primary"]))

			if status == "bad" or status == None or status == "unprocessed_state":
				logger.info("Primary disk is not available or is bad.")
				if "secondary_vbs" in mapping[vbucket]:
					logger.info("Found secondary for " + vbucket)
					storage_server = mapping[vbucket]["primary_vbs"]["storage_server"]
					vb_group = mapping[vbucket]["primary_vbs"]["vb_group"]
					disk = mapping[vbucket]["primary_vbs"]["disk"]
					status = mapping[vbucket]["primary_vbs"]["status"]
					type = "secondary"

					if status == "bad" or status == None or status == "unprocessed_state":
						logger.error("Both primary and secondary are bad disks.")
						continue
			vbucket_mapping = {}
			if type == "vbucket":
				vbucket_mapping[vbucket].update({"storage_server" : storage_server, "disk" : disk, "vb_group" : vb_group, "type" : type})
			elif type == "storage_server":
				vbucket_mapping[storage_server].update({ vbucket : {"path_name" : os.path.join("/",disk,type,vb_group,vbucket) , "disk" : disk, "vb_group" : vb_group, "type" : type}})

			
		self._start_response()
		if key != None:
			return json.dumps(vbucket_mapping[key])
		else:
			return json.dumps(vbucket_mapping)

	def get_all_config(self):
		self.status = '202 Accepted'
		mapping = self._get_mapping ("host")

		self.status = '200 OK'
		self._start_response()
		logger.debug("Mapping : " + str(mapping))
		return json.dumps(mapping)

	def get_host_config(self):
		self.status = '202 Accepted'

		mapping = self._get_mapping ("host")
		host_config = {}

		if mapping == False:
			logger.error("Failed to get host mapping.")
			self.status = '400 Bad Request'
			self._start_response()
			return "No host found"

		for host_name in sorted(mapping):
			status = None
			host_config[host_name] = {}
			if "primary" in mapping[host_name].keys():
				storage_server = mapping[host_name]["primary"]["storage_server"]
				disk = mapping[host_name]["primary"]["disk"]
				status = mapping[host_name]["primary"]["status"]
				

			if status == "bad" or status == None or status == "unprocessed_state":
				if "secondary" in mapping[host_name].keys():
					storage_server = mapping[host_name]["secondary"]["storage_server"]
					disk = mapping[host_name]["secondary"]["disk"]
					status = mapping[host_name]["secondary"]["status"]
					if status == "bad" or status == None or status == "unprocessed_state":
						continue

			host_config[host_name].update({"storage_server" : storage_server, "disk" : disk})

		self.status = '200 OK'
		self._start_response()
		logger.debug("Mapping : " + str(host_config))
		return json.dumps(host_config)

	def upload(self):
		self.status = '202 Accepted'
		path = self.environ["PATH_TRANSLATED"]
		request_uri = self.environ["REQUEST_URI"]
		logger.debug("Upload request : " + path)

		if not self._is_diskmapper_initialized():
			logger.info("Disk Mapper is not initialized.")
			self.initialize_diskmapper()

		host_name =  path.split("/")[5]
		game_id =  path.split("/")[4]

		lockfd = acquire_lock(self.host_init_lock)
		if not self._is_host_initialized(host_name):
			logger.info("Host : " + host_name + " is not initialized.")

			primary_initialized = False

			retries = 5
			while retries > 0:
				retries = retries - 1
				logger.info("Initializing primary for " + host_name)
				primary_initialized = self.initialize_host(host_name, "primary", game_id)
				if primary_initialized == False:
					logger.error("Failed to initialize primary for host : " + host_name)
					time.sleep(5)
				else:
					break
				
			if primary_initialized == True:
				retries = 5
				while retries > 0:
					logger.info("Initializing secondary for " + host_name)
					if self.initialize_host(host_name, "secondary", game_id) == False:
						logger.error("Failed to initialize primary for host : " + host_name)
						time.sleep(5)
					else:
						break

		release_lock(lockfd)
		return self.forward_request()

	def initialize_host(self, host_name, type, game_id, update_mapping=True):
		
		logger.debug("Initialize host : " + host_name + " " + type + " " + game_id + " " + str(update_mapping))
		mapping = self._get_mapping("host", host_name)

		skip_storage_server = None
		if mapping != False:
			if type == "primary" and "secondary" in mapping.keys():
				if mapping["secondary"]["status"] == "good":
					skip_storage_server =  mapping["secondary"]["storage_server"]
					logger.info("Skip server : " + skip_storage_server)

			if type == "secondary" and "primary" in mapping.keys():
				if mapping["primary"]["status"] == "good":
					skip_storage_server =  mapping["primary"]["storage_server"]
					logger.info("Skip server : " + skip_storage_server)
		
		spare = self._get_spare(type, skip_storage_server)
		logger.debug("spare : " + str(spare))
		if spare == False:
			logger.error(type + " spare not found for " + host_name)
			return False

		spare_server  = spare["storage_server"]
		spare_disk  = spare["disk"]
		spare_config = self._get_server_config(spare_server)
		if spare_config == False:
			logger.error("Failed to get server config for " + spare_server)

		if type not in spare_config[spare_disk]:
			logger.debug("Spare disk is no more a spare.")
			self.update_server_config(spare_server)
			logger.info("====" + str(self._get_spare(type, skip_storage_server)))
			return False

		if spare_config[spare_disk][type] != "spare":
			logger.debug("Spare disk is no more a spare.")
			self.update_server_config(spare_server)
			return False

		if self._initialize_host(spare_server, host_name, type, game_id, spare_disk, update_mapping) != False:
			if update_mapping == False:
				return spare
			return True

		return False

	def swap_bad_disk(self, storage_servers=None):
		storage_servers = config['storage_server']
		jobs = []
		for storage_server in storage_servers:
			if storage_server in self.bad_servers:
				continue

			jobs.append(threading.Thread(target=self.poll_bad_file, args=(storage_server,)))

		for j in jobs:
			j.start()

		for j in jobs:
			j.join()

	def poll_bad_file(self, storage_server, swap_all_disk=False):
		lockfd = acquire_lock(self.host_init_lock)
		logger.debug ("Started poll_bad_file for " + storage_server + " with swap_all_disk = " + str(swap_all_disk))
		if swap_all_disk == False:
			bad_disks = self._get_bad_disks(storage_server)

		current_mapping = self._get_mapping("storage_server",storage_server)
		for disk in current_mapping:
			status = "bad"
			if swap_all_disk == False:
				if disk not in bad_disks:
					status = "good"

			if status == "bad":
				if current_mapping[disk]["status"] == "bad":
					continue

				for type in sorted(current_mapping[disk]):

					if type == "status":
						continue

					host_name = current_mapping[disk][type]
					if host_name != "spare":
						if type == "primary":
							cp_from_type = "secondary"
						elif type == "secondary":
							cp_from_type = "primary"

						mapping = self._get_mapping("host", host_name)
						if mapping == False:
							logger.error("Failed to get mapping for " + host_name)
							continue

						logger.info("Found bad disk : " +  storage_server + ":/" + disk + "/" + type + "/" + host_name)
						try:
							cp_from_server = mapping[cp_from_type]["storage_server"]
							cp_from_disk = mapping[cp_from_type]["disk"]
							cp_from_file = os.path.join("/", cp_from_disk, cp_from_type, host_name)
						except KeyError:
							logger.info("Unable to find copy source for promotion of %s from %s:%s" %(host_name, storage_server, disk))
							self._update_mapping(storage_server, disk, type, host_name, status)
							release_lock(lockfd)
							return

						to_be_promoted = self._get_to_be_promoted(cp_from_server)
						if host_name in to_be_promoted:
							continue

						game_id = self._get_game_id(host_name, cp_from_server)
						if game_id == False:
							logger.error("Failed to get mapping for " + host_name)
							continue

						retries = 5
						while retries > 0:
							retries = retries - 1
							logger.info("Getting spare for " + host_name)
							spare = self.initialize_host(host_name, type, game_id, False)
							if spare == False:
								logger.error("Failed to get spare for : " + host_name + " : " + type )
								time.sleep(5)
							else:
								break
						
						if spare == False:
							logger.error("Failed to swap, no spare found for " + storage_server + ":/" + disk + "/" + type)
							continue

						cp_to_server = spare["storage_server"]
						cp_to_disk = spare["disk"]
						cp_to_type = type
						cp_to_file = os.path.join("/", cp_to_disk, cp_to_type, host_name)

						# Copy host
						if  self._rehydrate_replica(cp_from_server, cp_from_file) == False:
							logger.error("Failed to rehydrate replica for " + storage_server + ":" + cp_from_file)

						else:
							# Add to to-be-promoted list
							to_be_promoted = cp_to_server + ":" + cp_to_disk + ":"  + cp_to_type + ":"  + host_name
							if self._add_entry(cp_from_server, to_be_promoted, "to_be_promoted") == False:
								logger.error("Failed to add " + to_be_promoted + " to to_be_promoted list on " + cp_from_server)

						self._update_mapping(storage_server, disk, type, host_name, status)
					elif swap_all_disk != False:
						self._update_mapping(storage_server, disk, type, host_name, status)
		release_lock(lockfd)


	def delete_merged_files(self):
		storage_servers = config['storage_server']

		jobs = []
		for storage_server in storage_servers:
			if storage_server in self.bad_servers:
				continue
			jobs.append(threading.Thread(target=self.update_replica_file, args=(storage_server, "to_be_deleted")))

		for j in jobs:
			j.start()

		for j in jobs:
			j.join()

	def check_copy_complete(self):
		storage_servers = config['storage_server']

		jobs = []
		for storage_server in storage_servers:
			if storage_server in self.bad_servers:
				continue
			jobs.append(threading.Thread(target=self.update_replica_file, args=(storage_server, "copy_complete")))

		for j in jobs:
			j.start()

		for j in jobs:
			j.join()

	def update_replica_file(self, storage_server, type):
		if type == "to_be_deleted":
			replica_files = self._get_to_be_deleted(storage_server)
			dirty_file = self._get_dirty_file(storage_server)
		else:
			replica_files = self._get_copy_completed(storage_server)
		if replica_files == False:
			logger.error("Failed to get " + type + "file from storage server: " + storage_server)
			return False

		files = replica_files.split("\n")
		sorted_files = self._uniq(files)
		for file in sorted_files:
			if file == "":
				continue


			source_detail = file.split("/")
			source_server = storage_server
			try:
				source_disk = source_detail[1]
				source_type = source_detail[2]
				host_name = source_detail[3]
			except IndexError:
				return True

			if type == "to_be_deleted" and source_disk in dirty_file :
				continue

			mapping = self._get_mapping("host", host_name)
			if source_type == "primary":
				dest_type = "secondary"
			elif source_type == "secondary":
				dest_type = "primary"

			try:
				dest_server = mapping[dest_type]["storage_server"]
				dest_disk = mapping[dest_type]["disk"]
				dest_file = file.replace(source_disk,dest_disk).replace(source_type, dest_type)
			except KeyError:
				logger.error("Failed to find corresponding replica for " + file)
				continue

			if type == "copy_complete":
				logger.info("Successfully copied " + dest_server + ":" + dest_file + " to " + source_server + ":" + file)
				self._remove_entry(dest_server, dest_file, "dirty_files")
				self._remove_entry(source_server, file, "copy_completed")
			else:
				if self._delete_file(dest_server, dest_file):
					logger.info ("Successfully deleted " + dest_server + ":" + dest_file)
					self._remove_entry(source_server, file, "to_be_deleted")
				else:
					logger.error ("Failed to deleted " + dest_server + ":" + dest_file)
				
	def enable_replication(self):
		storage_servers = config['storage_server']

		jobs = []
		for storage_server in storage_servers:
			if storage_server in self.bad_servers:
				continue

			dirty_file = self._get_dirty_file(storage_server)
			to_be_promoted = self._get_to_be_promoted(storage_server)

			if dirty_file == False:
				logger.error("Failed to get dirty file from storage server: " + storage_server)
				return False

			if to_be_promoted != False:
				for line in to_be_promoted.split("\n"):
					splits = line.split(":")
					host_name = splits[-1]
					if host_name not in dirty_file:
						replica_server = splits[0]
						replica_disk = splits[1]
						replica_type = splits[2]
						promote_flag = os.path.join("/", replica_disk, replica_type, host_name, ".promoting")
						if not self._delete_file(replica_server, promote_flag):
							logger.error("Failed to remove .promoting for " + line)
						else:
							if not self._update_mapping(replica_server, replica_disk, replica_type, host_name , "good"):
								logger.error("Failed to update mapping for " + line)
							elif not self._remove_entry(storage_server, line, "to_be_promoted"):
								logger.error("Failed to remove " + line + "from to be promoted file")
								
								


			bad_disks = self._get_bad_disks(storage_server)
			if bad_disks == False:
				logger.error("Failed to get dirty file from storage server: " + storage_server)

			files = dirty_file.split("\n")
			sorted_files = self._uniq(files)
			disks = {}
			for file in sorted_files:
				if file == "":
					continue

				disk_name = file.split("/")[1]
				if disk_name in bad_disks:
					continue

				if disk_name not in disks.keys():
					disks[disk_name] = []

				disks[disk_name].append(file)
		

			for disk in disks:
				jobs.append(threading.Thread(target=self.poll_dirty_file, args=(storage_server, disks[disk], to_be_promoted)))

		for j in jobs:
			j.start()

		for j in jobs:
			j.join()

	def poll_dirty_file(self, storage_server,files, to_be_promoted):
		for file in files:
			logger.info("Handling : " + file )
			if file == "":
				return True
			cp_from_detail = file.split("/")
			cp_from_server = storage_server
			try:
				cp_from_disk = cp_from_detail[1]
				cp_from_type = cp_from_detail[2]
				host_name = cp_from_detail[3]
			except IndexError:
				return True

			mapping = self._get_mapping("host", host_name)
			if cp_from_type == "primary":
				cp_to_type = "secondary"
			elif cp_from_type == "secondary":
				cp_to_type = "primary"

			if to_be_promoted != False and host_name in to_be_promoted:
				for line in to_be_promoted.split("\n"):
					if host_name in line:
						splits = line.split(":")
						cp_to_server = splits[0]
						cp_to_disk = splits[1]
						cp_to_file = file.replace(cp_from_disk,cp_to_disk).replace(cp_from_type, cp_to_type)

			else:
				try:
					cp_to_server = mapping[cp_to_type]["storage_server"]
					cp_to_disk = mapping[cp_to_type]["disk"]
					cp_to_file = file.replace(cp_from_disk,cp_to_disk).replace(cp_from_type, cp_to_type)
				except KeyError:
					logger.error("Failed to find corresponding replica for " + file)
					return True

			torrent_url = self._create_torrent(cp_from_server, file)
			if torrent_url == "True":
				logger.info("Torrent is running, for " + storage_server + ":" + file + " Skipping...")
				return True

			if torrent_url == False:
				logger.error("Failed to get torrent url for " + storage_server + ":" + file)
				return False

			if self._start_download(cp_to_server, cp_to_file, torrent_url) == True:
				logger.info("Started replication for : " + storage_server + ":" + file)
			else:
				logger.error("Failed to start download to " + cp_to_server + ":" + cp_to_file)
				return False
			#Process one file in one thread
			return True
		return True


	def initialize_diskmapper(self):
		lockfd = acquire_lock(self.host_init_lock)
		storage_servers = config['storage_server']

		for storage_server in storage_servers:
			if storage_server in self.bad_servers:
				continue
			self.update_server_config(storage_server)

		release_lock(lockfd)

	def update_server_config(self, storage_server):
		server_config = self._get_server_config(storage_server)
		if server_config == False:
			logger.error("Failed to get config from storage server: " + storage_server)
			return False

		bad_disks = self._get_bad_disks(storage_server)
		if bad_disks == False:
			logger.error("Failed to get bad disks form storage server: " + storage_server)
			return False

		for disk in sorted(server_config):
			status = "unprocessed_state"
			if disk in bad_disks or storage_server in self.bad_servers:
				current_mapping = self._get_mapping("storage_server",storage_server)
				if disk in current_mapping.keys():
					if current_mapping[disk]["status"] == "bad":
						status = "bad"
			else:
				status = "good"
			for type in sorted(server_config[disk]):
				if type == "primary" or type == "secondary":
					host_name = server_config[disk][type]
					vbuckets = None
					if type+"_vbs" in server_config[disk].keys():
						vbuckets = server_config[disk][type+"_vbs"]
					host_name = server_config[disk][type]
					self._update_mapping(storage_server, disk, type, host_name, status, vbuckets)
					

	def is_dm_active(self):
		zrt = config["zruntime"]
		url = os.path.join ('https://api.runtime.zynga.com:8994/', zrt["gameid"], zrt["env"], "current")
		retries = int(zrt['retries'])
		while retries > 0:
			retries = retries - 1
			value = self._curl(url, 200, True)
			if value != False:
				break
			logger.error("Retrying zruntime connection...")
			time.sleep(5)

		else:
			if value == False:
				logger.error("Failed to get Zruntime data.\nShutting down Disk Mapper.")
				os.remove("/var/run/disk_mapper.lock")
				exit(1)

		value = json.loads(value)
		active_dm = value["output"][zrt["mcs_key_name"]]
		ip = socket.gethostbyname(socket.gethostname())
		logger.debug("ip : " + str(ip) + " active_dm : " + str(active_dm));
		if active_dm == ip:
			return True
		return False

		
	def make_spare(self, storage_server):
		server_config = self._get_server_config(storage_server)
		if server_config == False:
			logger.error("Failed to get config from storage server: " + storage_server)
			return False

		for disk in server_config:
			for type in server_config[disk]:
				if storage_server not in self.bad_servers:
					return None

				if type == "primary" or type == "secondary":
					host_name = server_config[disk][type]
					if host_name == "spare":
						continue

					host_mapping = self._get_server_config(host_name)
					if host_mapping == False:
						continue

					if storage_server ==  host_mapping["storage_server"]:
						continue

					self._make_spare(storage_server, type, disk)
					
	def _rehydrate_replica(self, storage_server, path):
		# http://http://netops-demo-mb-212.va2/api/?action=copy_host&path=/data_1/primary/game-mb-18/
		url = 'http://' + storage_server + '/api?action=copy_host&path=' + path
		value = self._curl(url, 200)
		if value != False:
			return True
		return False

	def _create_torrent(self, storage_server, file):
		# http://netops-demo-mb-220.va2/api/membase_backup?action=create_torrent&file_path=/data_2/primary/empire-mb-user-b-001/zc1/incremental/test1/
		url = 'http://' + storage_server + '/api?action=create_torrent&file_path=' + file
		value = self._curl(url, 200)
		if value != False:
			return value
		return False

	def _add_entry(self, storage_server, entry, file_type):
		# http://netops-demo-mb-220.va2/api/membase_backup?action=add_entry&type=bad_disk&entry=%22/data_1%22
		url = 'http://' + storage_server + '/api?action=add_entry&entry=' + entry + '&type=' + file_type
		value = self._curl(url, 200)
		if value != False:
			return True
		return False

	def _delete_file(self, storage_server, file_name):
		# http://netops-demo-mb-220.va2/api/membase_backup?action=delete_file&file_name=/data_1/primary/game-mb-6/zc1/daily/small_file:1
		url = 'http://' + storage_server + '/api?action=delete_file&file_name=' + file_name.rstrip() 
		value = self._curl(url, 200)
		if value != False:
			return True
		return False

	def _remove_entry(self, storage_server, entry, file_type):
		# http://netops-demo-mb-220.va2/api/membase_backup?action=remove_entry&type=bad_disk&entry=%22/data_1%22
		url = 'http://' + storage_server + '/api?action=remove_entry&entry=' + entry.rstrip() + '&type=' + file_type
		value = self._curl(url, 200)
		if value != False:
			return True
		return False

	def _make_spare(self, storage_server, type, disk):
		# http://10.36.168.173/api?action=make_spare&type=primary&disk=data_1
		url = 'http://' + storage_server + '/api?action=make_spare&type=' + type + '&disk=' + disk
		value = self._curl(url, 200)
		if value != False:
			return True
		return False

	def _start_download(self, storage_server, file, torrent_url):
		# http://netops-demo-mb-220.va2/api/membase_backup?action=start_download&file_path=/data_3/secondary/empire-mb-user-b-001/zc1/&torrent_url=http://10.36.168.173/torrent/1347783417.torrent
		url = 'http://' + storage_server + '/api?action=start_download&file_path=' + file.rstrip() + '&torrent_url=' + torrent_url
		value = self._curl(url, 200)
		if value != False:
			return True
		return False

	def _get_to_be_promoted(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_file&type=to_be_promoted'
		value = self._curl(url, 200)
		if value != False:
			return json.loads(value)
		return False

	def _get_bad_disks(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_file&type=bad_disk'
		value = self._curl(url, 200)
		if value != False:
			return json.loads(value)
		return False
		
	def _initialize_host(self, storage_server, host_name, type, game_id, disk, update_mapping=True):
		if update_mapping == True:
			url = 'http://' + storage_server + '/api?action=initialize_host&host_name=' + host_name + '&type=' + type + '&game_id=' + game_id + '&disk=' + disk
		else:
			url = 'http://' + storage_server + '/api?action=initialize_host&host_name=' + host_name + '&type=' + type + '&game_id=' + game_id + '&disk=' + disk + '&promote=true'
		logger.debug("Initial request url : " + str(url))
		value = self._curl(url, 201)
		if value != False:
			logger.info("Initialized " + host_name + "at " + storage_server + ":/" + disk + "/" + type)
			if update_mapping == True:
				logger.info("Updating mapping.")
				self._update_mapping(storage_server, disk, type, host_name, "good")
			return True
		logger.error("Failed to initialize " + host_name + " at " + storage_server + ":/" + disk + "/" + type)
		return False

	def _get_copy_completed(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_file&type=copy_completed'
		value = self._curl(url, 200)
		if value != False:
			return json.loads(value)
		return False

	def _get_to_be_deleted(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_file&type=to_be_deleted'
		value = self._curl(url, 200)
		if value != False:
			return json.loads(value)
		return False

	def _get_dirty_file(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_file&type=dirty_files'
		value = self._curl(url, 200)
		if value != False:
			return json.loads(value)
		return False

	def _get_game_id(self, host_name, storage_server):
		url = 'http://' + storage_server + '/api?action=get_game_id&host_name=' + host_name
		value = self._curl(url, 200)
		if value != False:
			return value
		return False

	def _get_server_config(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_config'
		value = self._curl(url, 200)
		if value != False:
			return json.loads(value)
		return False
		
	def _curl_debug (self, debug_type, debug_msg):
		logger.debug("Curl:" + str(debug_type) + " " + str(debug_msg))
		
	def _curl (self, url, exp_return_code=None, insecure=False):
		storage_server = url.split("/")[2]
		try:
			if insecure == True:
				conn = httplib.HTTPSConnection(storage_server)
				zrt = config["zruntime"]
				username = zrt["username"] 
				password = zrt["password"]
				auth = base64.encodestring("%s:%s" % (username, password)) 
				headers = {"Authorization" : "Basic %s" % auth}
				conn.request("GET", url, headers=headers)
			else:
				conn = httplib.HTTPConnection(storage_server)
				conn.request("GET", url)

			response = conn.getresponse()
			conn.close()
			if storage_server in self.bad_servers:
				self.make_spare(storage_server)
				self.bad_servers.remove(storage_server)
		except (httplib.HTTPResponse, socket.error) as error:
			errno, errstr = error
			if errno == 111:
				self._check_server_conn(storage_server)
			return False
		except:
			logger.error("Caught unknown error in _curl")
			return False

		if response.status != exp_return_code and exp_return_code != None:
			return False

		value = response.read()
		return value

	def _check_server_conn(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_config'
		for retry in range(3):
			try:
				time.sleep(5)
				conn = httplib.HTTPConnection(storage_server)
				conn.request("GET", url)
			except (httplib.HTTPResponse, socket.error) as error:
				errno, errstr = error
				if errno == 111:
					logger.error("Failed to connect to " + storage_server)
			else:
			  break
		else:
			if storage_server not in self.bad_servers:
				self.bad_servers.append(storage_server)
			self.poll_bad_file(storage_server, True)

	def _is_diskmapper_initialized(self):
		lockfd = acquire_lock(self.mapping_lock)
		ret = True
		if not os.path.exists(self.mapping_file):
			ret =  False

		release_lock(lockfd)
		return ret

	def _is_host_initialized(self, host_name):
		if self._get_mapping ("host", host_name, False) == False:
			return False
		return True

	def _is_bad_disk(self, type):
		try:
			if type["status"] == "bad":
				return True
		except KeyError:
			return False

	def _get_spare(self, type=None, skip=None):
		mapping = self._get_mapping("storage_server")
		if mapping == False:
			return False

		spare_mapping = {}
		spare_type_mapping = {}
		spare_mapping["primary"] = []
		spare_mapping["secondary"] = []
		for storage_server in sorted(mapping):
			if storage_server == skip or storage_server in self.bad_servers:
				continue
			spare_type_mapping[storage_server] = []
			for disk in sorted(mapping[storage_server]):
				for disk_type in sorted(mapping[storage_server][disk]):
					if disk_type == "primary" or disk_type == "secondary":
						host_name = mapping[storage_server][disk][disk_type]
						if host_name == "spare" and mapping[storage_server][disk]["status"] == "good":
							if type == disk_type:
								spare_type_mapping[storage_server].append(disk)
							spare_mapping[disk_type].append({ "disk" : disk, "storage_server" : storage_server})

		if type != None:
			highest_spare_count = 0
			server_with_most_spare = None
			for storage_server in sorted(spare_type_mapping):
				current_count = len(spare_type_mapping[storage_server])
				if current_count > highest_spare_count:
					highest_spare_count = current_count
					server_with_most_spare = storage_server
				
			if server_with_most_spare == None:
				return False
			spare_disk = spare_type_mapping[server_with_most_spare].pop()
			return {"disk" : spare_disk, "storage_server" : server_with_most_spare }
		return spare_mapping

	def _get_vbucket_mapping(self):


		if not self._is_diskmapper_initialized():
			return False

		lockfd = open(self.mapping_lock, 'w')
		fcntl.flock(lockfd.fileno(), fcntl.LOCK_EX)
		f = open(self.mapping_file, 'r')
		file_content = pickle.load(f)

		#logger.debug("Mapping in file : " + str(file_content))
		mapping = {}
		for storage_server in sorted(file_content):
			for disk in sorted(file_content[storage_server]):
				for disk_type in sorted(file_content[storage_server][disk]):
					if disk_type == "primary_vbs" or disk_type == "secondary_vbs":
						if disk_type == "primary_vbs":
							vb_group_type = "primary"
						else:
							vb_group_type = "primary"
						vbuckets = file_content[storage_server][disk][disk_type]
						vb_group = file_content[storage_server][disk][vb_group_type]
						status = file_content[storage_server][disk]["status"]
						for vbucket in vbuckets.split(","):
							if vbuckets not in mapping.keys():
								mapping[vbucket] = {}
							if status != "good":
								continue
							mapping[vbucket].update({disk_type : { "disk" : disk, "vb_group" : vb_group, "status" : status, "storage_server" : storage_server}})


		f.close()
		fcntl.flock(lockfd.fileno(), fcntl.LOCK_UN)
		lockfd.close()

		return mapping

	def _get_mapping(self, type, key=None, ignore_bad=True):

		logger.debug("Get mapping for, type : " + type + " key : " + str(key) + " ignore_bad : " + str(ignore_bad))

		if not self._is_diskmapper_initialized():
			return False

		lockfd = acquire_lock(self.mapping_lock)
		f = open(self.mapping_file, 'r')
		file_content = pickle.load(f)

		#logger.debug("Mapping in file : " + str(file_content))
		if type == "host":
			mapping = {}
			for storage_server in sorted(file_content):
				for disk in sorted(file_content[storage_server]):
					for disk_type in sorted(file_content[storage_server][disk]):
						if disk_type == "primary" or disk_type == "secondary":
							host_name = file_content[storage_server][disk][disk_type]
							status = file_content[storage_server][disk]["status"]
							if host_name != "spare" :
								if host_name not in mapping.keys():
									mapping[host_name] = {}
									#logger.debug("=========" + disk + disk_type + status + host_name + "==========")
								if status != "good" and ignore_bad:
									continue
								mapping[host_name].update({disk_type : { "disk" : disk, "status" : status, "storage_server" : storage_server}})

		elif type == "storage_server":
			mapping = file_content

		f.close()
		release_lock(lockfd)

		if key == None:
			return mapping

		try:
			return mapping[key]
		except KeyError:
			return False
	
	def _start_response(self):
		self.start_response(self.status, self.response_headers)

	def _update_mapping(self, storage_server, disk, disk_type, host_name, status="good", vbuckets=None):
		lockfd = acquire_lock(self.mapping_lock)

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

		logger.debug("Updating mapping :" + storage_server + " " + disk + " " + disk_type + " " + host_name + " " + str(status))
		file_content = read_mapping(self.mapping_file)

		#logger.debug("here with : " + storage_server + " " + disk + " " + disk_type + " " + host_name + " " + status)
		#logger.debug("Mapping read from file : " + str(file_content))
		if storage_server in file_content.keys():
			if disk in file_content[storage_server].keys():
				#if disk_type in file_content[storage_server][disk].keys()
				file_content[storage_server][disk][disk_type] = host_name
				file_content[storage_server][disk]["status"] = status

			else:
				file_content[storage_server].update({disk : {disk_type : host_name, "status" : status}})
		else:
			file_content.update({storage_server : {disk : {disk_type : host_name, "status" : status}}})
		if vbuckets != None:
			file_content[storage_server][disk][type+"_vbs"] = vbuckets
		#logger.debug("Mapping to be written to file : " + str(file_content))
		write_mapping(self.mapping_file, file_content)
		verify_content = read_mapping(self.mapping_file)
		#logger.debug("Updated content : " + str(verify_content))
		if verify_content != file_content:
			logger.error("Failed to update mapping for " + storage_server + " " + disk + " " + disk_type + " " + host_name + " " + status)

		release_lock(lockfd)
		return True

	def _uniq(self, input):
		output = []
		for x in input:
			if x not in output:
				output.append(x)
		return output

