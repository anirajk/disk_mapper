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
import pycurl
import cStringIO
import threading
import socket
import logging
import subprocess
from signal import SIGSTOP, SIGCONT
from config import config
from cgi import parse_qs

logger = logging.getLogger('disk_mapper')
hdlr = logging.FileHandler('/var/log/disk_mapper.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.ERROR)

class DiskMapper:

	def __init__(self, environ, start_response):
		self.mapping_file = '/var/tmp/disk_mapper/host.mapping'
		self.bad_servers = []
		if environ != None:
			self.environ  = environ
			self.query_string  = environ["QUERY_STRING"]
			self.start_response = start_response
			self.status = '400 Bad Request'
			self.response_headers = [('Content-type', 'text/plain')]
		if not os.path.exists("/var/run/disk_mapper.lock"):
			logger.info("=== Disk Mapper service is Not running ===")
			exit()

	def forward_request(self):
		self.status = '202 Accepted'
		path = self.environ["PATH_TRANSLATED"]
		request_uri = self.environ["REQUEST_URI"]

		logger.debug("Redirect request : " + request_uri)
		host_name =  path.split("/")[5]
		mapping = self._get_mapping ("host", host_name)
			
		if mapping == False:
			logger.error("Failed to get mapping for : " + host_name)
			self.status = '200 OK'
			self._start_response()
			return "Host name " + host_name + " not found in mapping."
			
		status = None
		if "primary" in mapping.keys():
			logger.info("Found primary for " + host_name)
			storage_server = mapping["primary"]["storage_server"]
			status = mapping["primary"]["status"]
			logger.debug("Primary mapping : " + str(mapping["primary"]))

		if status == "bad" or status == None:
			logger.info("Primary disk is not available or is bad.")
			if "secondary" in mapping.keys():
				logger.info("Found secondary for " + host_name)
				storage_server = mapping["secondary"]["storage_server"]
				status = mapping["secondary"]["status"]
				logger.debug("Secondary mapping : " + str(mapping["secondary"]))
				if status == "bad":
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

	def get_host_config(self):
		self.status = '202 Accepted'

		mapping = self._get_mapping ("host")
		host_config = {}

		if mapping == False:
			logger.error("Failed to get host mapping.")
			self.status = '400 Bad Request'
			self._start_response()
			return "No host found"

		for host_name in mapping:
			status = None
			host_config[host_name] = {}
			if "primary" in mapping[host_name].keys():
				storage_server = mapping[host_name]["primary"]["storage_server"]
				disk = mapping[host_name]["primary"]["disk"]
				status = mapping[host_name]["primary"]["status"]
				

			if status == "bad" or status == None:
				if "secondary" in mapping[host_name].keys():
					storage_server = mapping[host_name]["secondary"]["storage_server"]
					disk = mapping[host_name]["secondary"]["disk"]
					status = mapping[host_name]["secondary"]["status"]
					if status == "bad":
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

		if not self._is_host_initialized(host_name):
			logger.info("Host : " + host_name + " is not initialized.")
			logger.info("Initializing primary for " + host_name)
			self.initialize_host(host_name, "primary", game_id)
			logger.info("Initializing secondary for " + host_name)
			self.initialize_host(host_name, "secondary", game_id)

		return self.forward_request()

	def initialize_host(self, host_name, type, game_id, update_mapping=True):
		
		logger.debug("Initialize host : " + host_name + " " + type + " " + game_id + " " + str(update_mapping))
		mapping = self._get_mapping("host", host_name)

		skip_storage_server = None
		if mapping != False:
			if type == "primary" and "secondary" in mapping.keys():
				if mapping["secondary"]["status"] != "bad":
					skip_storage_server =  mapping["secondary"]["storage_server"]
					logger.info("Skip server : " + skip_storage_server)

			if type == "secondary" and "primary" in mapping.keys():
				if mapping["primary"]["status"] != "bad":
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
		if spare_config[spare_disk][type] != "spare":
			logger.debug("Spare disk is no more a spare.")
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
			jobs.append(threading.Thread(target=self.poll_bad_file, args=(storage_server,)))

		for j in jobs:
			j.start()

		while threading.activeCount() > 1:
			pass

	def poll_bad_file(self, storage_server, swap_all_disk=False):
		logger.debug ("Started poll_bad_file for " + storage_server + " with swap_all_disk = " + str(swap_all_disk))
		
		if swap_all_disk == False:
			server_config = self._get_server_config(storage_server)
			if server_config == False:
				logger.error("Failed to get config from storage server: " + storage_server)
				return False

			bad_disks = self._get_bad_disks(storage_server)
			if server_config == False:
				logger.error("Failed to get bad disks form storage server: " + storage_server)
				return False
		else:
			server_config = self._get_mapping("storage_server",storage_server)
			if server_config == False:
				return True

		for disk in server_config:
			status = "bad"
			if swap_all_disk == False:
				if disk not in bad_disks:
					status = "good"

			if status == "bad":
				for type in server_config[disk]:
					if type == "status":
						continue
					host_name = server_config[disk][type]
					self._update_mapping(storage_server, disk, type, host_name, status)
					if host_name != "spare":
						game_id = host_name.split("-")[0]
						if type == "primary":
							cp_from_type = "secondary"
						elif type == "secondary":
							cp_from_type = "primary"

						mapping = self._get_mapping("host", host_name)
						if mapping == False:
							logger.error("Failed to get mapping for " + host_name)
							continue

						if type in mapping.keys():
							continue

						spare = self.initialize_host(host_name, type, game_id, False)
						
						if spare == False:
							logger.error("Failed to swap " + storage_server + ":/" + disk + "/" + type)
							continue

						cp_to_server = spare["storage_server"]
						cp_to_disk = spare["disk"]
						cp_to_type = type
						cp_to_file = os.path.join("/", cp_to_disk, cp_to_type, host_name)

						try:
							cp_from_server = mapping[cp_from_type]["storage_server"]
							cp_from_disk = mapping[cp_from_type]["disk"]
							cp_from_file = os.path.join("/", cp_from_disk, cp_from_type, host_name)
						except KeyError:
							continue

						torrent_url = self._create_torrent(cp_from_server, cp_from_file)
						if torrent_url == False:
							logger.error("Failed to get torrent url for " + storage_server + ":" + file)
							continue

						if self._start_download(cp_to_server, cp_to_file, torrent_url) == True:
							self._update_mapping(cp_to_server, cp_to_disk, cp_to_type, host_name)
						else:
							logger.error("Failed to start download to " + cp_to_server + ":" + cp_to_file)


	def enable_replication(self):
		storage_servers = config['storage_server']

		jobs = []
		for storage_server in storage_servers:
			jobs.append(threading.Thread(target=self.poll_dirty_file, args=(storage_server,)))

		for j in jobs:
			j.start()

		while threading.activeCount() > 1:
			pass

	def poll_dirty_file(self, storage_server):
		dirty_file = self._get_dirty_file(storage_server)
		if dirty_file == False:
			logger.error("Failed to get dirty file from storage server: " + storage_server)
			return False

		files = dirty_file.split("\n")
		sorted_files = self._uniq(files)
		[ x.strip() for x in sorted_files ]
		for file in sorted_files:
			if file == "":
				continue
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

			try:
				cp_to_server = mapping[cp_to_type]["storage_server"]
				cp_to_disk = mapping[cp_to_type]["disk"]
				cp_to_file = file.replace(cp_from_disk,cp_to_disk).replace(cp_from_type, cp_to_type)
			except KeyError:
				self._remove_entry(cp_from_server, file, "dirty_files")
				return True

			torrent_url = self._create_torrent(cp_from_server, file)
			if torrent_url == False:
				logger.error("Failed to get torrent url for " + storage_server + ":" + file)
				return False
				
			if self._start_download(cp_to_server, cp_to_file, torrent_url) == True:
				self._remove_entry(cp_from_server, file, "dirty_files")
			else:
				logger.error("Failed to start download to " + cp_to_server + ":" + cp_to_file)


	def initialize_diskmapper(self, poll=False):
		if os.path.exists(self.mapping_file) and poll == False:
			os.remove(self.mapping_file)
		storage_servers = config['storage_server']
		jobs = []
		for storage_server in storage_servers:
			jobs.append(threading.Thread(target=self.update_server_config, args=(storage_server,)))

		for j in jobs:
			j.start()

		while threading.activeCount() > 1:
			pass

	def update_server_config(self, storage_server):
		server_config = self._get_server_config(storage_server)
		if server_config == False:
			logger.error("Failed to get config from storage server: " + storage_server)
			return False

		bad_disks = self._get_bad_disks(storage_server)
		if bad_disks == False:
			logger.error("Failed to get bad disks form storage server: " + storage_server)
			return False

		for disk in server_config:
			if disk in bad_disks or storage_server in self.bad_servers:
				status = "bad"
			else:
				status = "good"
			for type in server_config[disk]:
				host_name = server_config[disk][type]
				self._update_mapping(storage_server, disk, type, host_name, status)
					

	def is_dm_active(self):
		zrt = config["zruntime"]
		url = os.path.join ('https://api.runtime.zynga.com:8994/', zrt["gameid"], zrt["env"], "current")
		value = self._curl(url, 200, True)
		if value != False:
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

					host_mapping = self._get_server_config("host", host_name)
					if host_mapping == False:
						continue

					if storage_server ==  host_mapping["storage_server"]:
						continue

					self._make_spare(storage_server, type, disk)
					
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

	def _get_bad_disks(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_file&type=bad_disk'
		value = self._curl(url, 200)
		if value != False:
			return json.loads(value)
		return False
		
	def _initialize_host(self, storage_server, host_name, type, game_id, disk, update_mapping=True):
		url = 'http://' + storage_server + '/api?action=initialize_host&host_name=' + host_name + '&type=' + type + '&game_id=' + game_id + '&disk=' + disk
		logger.debug("Initial request url : " + str(url))
		value = self._curl(url, 201)
		if value != False:
			logger.info("Initialized " + host_name + "at " + storage_server + ":/" + disk + "/" + type)
			if update_mapping == True:
				logger.info("Updating mapping.")
				self._update_mapping(storage_server, disk, type, host_name)
			return True
		logger.error("Failed to initialize " + host_name + " at " + storage_server + ":/" + disk + "/" + type)
		return False

	def _get_dirty_file(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_file&type=dirty_files'
		value = self._curl(url, 200)
		if value != False:
			return json.loads(value)
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
		buf = cStringIO.StringIO()
		storage_server = url.split("/")[2]
		c = pycurl.Curl()
		c.setopt(c.URL, str(url))
		if insecure == True:
			c.setopt(pycurl.SSL_VERIFYPEER,0)
			zrt = config["zruntime"]
			c.setopt(pycurl.USERPWD, zrt["username"] + ":" + zrt["password"])

		c.setopt(c.WRITEFUNCTION, buf.write)
		#c.setopt(pycurl.VERBOSE, 1)
		#c.setopt(pycurl.DEBUGFUNCTION, self._curl_debug)
		try:
			c.perform()
			if storage_server in self.bad_servers:
				self.make_spare(storage_server)
				self.bad_servers.remove(storage_server)
		except pycurl.error, error :
			errno, errstr = error
			if errno == 7:
				self._check_server_conn(storage_server)
			return False

		if c.getinfo(pycurl.HTTP_CODE) != exp_return_code and exp_return_code != None:
			c.close()
			return False

		value = buf.getvalue()
		c.close()
		buf.close()
		return value

	def _check_server_conn(self, storage_server):
		url = 'http://' + storage_server + '/api?action=get_config'
		c = pycurl.Curl()
		c.setopt(c.URL, str(url))
		for retry in range(3):
			try:
				time.sleep(5)
				c.perform()
			except pycurl.error, error:
				errno, errstr = error
				if errno == 7:
					logger.error("Failed to connect to " + storage_server)
			else:
			  break
		else:
			if storage_server not in self.bad_servers:
				self.bad_servers.append(storage_server)
			self.poll_bad_file(storage_server, True)

	def _is_diskmapper_initialized(self):
		if not os.path.exists(self.mapping_file):
			return False
		return True

	def _is_host_initialized(self, host_name):
		if not self._get_mapping ("host", host_name):
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
		for storage_server in mapping:
			if storage_server == skip or storage_server in self.bad_servers:
				continue
			spare_type_mapping[storage_server] = []
			for disk in mapping[storage_server]:
				for disk_type in mapping[storage_server][disk]:
					if disk_type == "primary" or disk_type == "secondary":
						host_name = mapping[storage_server][disk][disk_type]
						if host_name == "spare" and mapping[storage_server][disk]["status"] != "bad":
							if type == disk_type:
								spare_type_mapping[storage_server].append(disk)
							spare_mapping[disk_type].append({ "disk" : disk, "storage_server" : storage_server})

		if type != None:
			highest_spare_count = 0
			server_with_most_spare = None
			for storage_server in spare_type_mapping:
				current_count = len(spare_type_mapping[storage_server])
				if current_count > highest_spare_count:
					highest_spare_count = current_count
					server_with_most_spare = storage_server
				
			if server_with_most_spare == None:
				return False
			spare_disk = spare_type_mapping[server_with_most_spare].pop()
			return {"disk" : spare_disk, "storage_server" : server_with_most_spare }
		return spare_mapping

	def _get_mapping(self, type, key = None):

		if not self._is_diskmapper_initialized(): 
			return False

		f = open(self.mapping_file, 'r')
		fcntl.flock(f.fileno(), fcntl.LOCK_EX)
		file_content = pickle.load(f)
		
		if type == "host":
			mapping = {}
			for storage_server in file_content:
				for disk in file_content[storage_server]:
					for disk_type in file_content[storage_server][disk]:
						if disk_type == "primary" or disk_type == "secondary":
							host_name = file_content[storage_server][disk][disk_type]
							status = file_content[storage_server][disk]["status"]
							if host_name != "spare" and status != "bad":
								if host_name not in mapping.keys():
									mapping[host_name] = {}
								mapping[host_name].update({disk_type : { "disk" : disk, "status" : status, "storage_server" : storage_server}})

		elif type == "storage_server":
			mapping = file_content


		fcntl.flock(f.fileno(), fcntl.LOCK_UN)
		f.close()

		if key == None:
			return mapping

		try:
			return mapping[key]
		except KeyError:
			return False
	
	def _start_response(self):
		self.start_response(self.status, self.response_headers)

	def _update_mapping(self, storage_server, disk, disk_type, host_name, status="good"):
		if os.path.exists(self.mapping_file):
			f = open(self.mapping_file, 'r+')
		else:
			f = open(self.mapping_file, 'w+')
			
		#logger.debug("Updating mapping :" + storage_server + " " + disk + " " + disk_type + " " + host_name + " " + status)
		fcntl.flock(f.fileno(), fcntl.LOCK_EX)
		file_content = f.read()
		if file_content != "":
			f.seek(0, 0)
			file_content = pickle.load(f)
			#file_content[storage_server][disk][disk_type] = host_name
		else:
			file_content = {}

		#logger.debug("here with : " + storage_server + " " + disk + " " + disk_type + " " + host_name + " " + status)
		if storage_server in file_content.keys():
			if disk in file_content[storage_server].keys():
				#if disk_type in file_content[storage_server][disk].keys()
				file_content[storage_server][disk][disk_type] = host_name
				file_content[storage_server][disk]["status"] = status

			else:
				file_content[storage_server].update({disk : {disk_type : host_name, "status" : status}})
		else:
			file_content.update({storage_server : {disk : {disk_type : host_name, "status" : status}}})
		f.seek(0, 0)
		f.truncate()
		pickle.dump(file_content,f)
		f.seek(0, 0)
		verify_content = pickle.load(f)
		#logger.debug("Updated content : " + str(verify_content))
		if verify_content != file_content:
			logger.error("Failed to update mapping for " + storage_server + " " + disk + " " + disk_type + " " + host_name + " " + status)
		os.fsync(f)
		fcntl.flock(f.fileno(), fcntl.LOCK_UN)
		f.close()
		return True

	def _uniq(self, input):
		output = []
		for x in input:
			if x not in output:
				output.append(x)
		return output

