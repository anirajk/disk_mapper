#!/usr/bin/env python
import math
import pprint
import getopt
import sys

def main():
		options, remainder = getopt.getopt(sys.argv[1:], 'n:d:s:p')

		server_count = 0
		disk_count = 24
		spare_count = 1
		host_prefix = "game-mb-"

		for opt, arg in options:
		    if opt in ('-n'):
				server_count = arg
		    elif opt in ('-d'):
		        disk_count = int(arg)
		    elif opt == '-s':
		        spare_count = int(arg)
		    elif opt == '-p':
		        host_prefix = int(arg)

		
		if server_count == 0:
			print "Usage: \t-n number of mb nodes, \nOptional args:\n\t-d no. of disks on storage server, default : 24\n\t-s No. of spare storage servers, default : 1\n\t-p Hostname prefix, default : game-mb-"
			exit()
		total_server_count = math.ceil(float(server_count) / disk_count) + spare_count
		if total_server_count < 3:
			total_server_count = 3

		total_disk_count = total_server_count * disk_count
		print total_server_count
		print int(total_disk_count)

		servers = {}

		for i in range (1, int(total_server_count)+1):
			server_name = "storage-server-" + str(i)
			if server_name not in servers:
				servers[server_name] = {}

			for j in range (1, disk_count + 1):
				disk_name = "disk_" + str(j)
				if disk_name not in servers[server_name]:
					servers[server_name][disk_name] = {}

				servers[server_name][disk_name]["primary"] = "spare"
				servers[server_name][disk_name]["secondary"] = "spare"


		for h in range(1, int(server_count)+1):

			host_name = "game-mb-" + str(h)
			primary_spare = get_spare(servers, "primary", None)
			primary_ss = primary_spare['storage_server']
			primary_disk = primary_spare['disk']
			secondary_spare = get_spare(servers, "secondary", primary_ss)
			secondary_ss = secondary_spare['storage_server']
			secondary_disk = secondary_spare['disk']
			servers[primary_ss][primary_disk]["primary"] = host_name
			servers[secondary_ss][secondary_disk]["secondary"] = host_name


		for server in sorted(servers):
			print server + ':\n'
			for disk in sorted(servers[server]):
				print '\t' + disk + ':'
				print '\t\tPrimary : ' + servers[server][disk]["primary"]
				print '\t\tSecondary : ' + servers[server][disk]["secondary"] 
				



def get_spare(mapping, type=None, skip=None):
		if mapping == False:
			return False

		spare_mapping = {}
		spare_type_mapping = {}
		spare_mapping["primary"] = []
		spare_mapping["secondary"] = []
		for storage_server in sorted(mapping):
			if storage_server == skip:
				continue
			spare_type_mapping[storage_server] = []
			for disk in sorted(mapping[storage_server]):
				for disk_type in sorted(mapping[storage_server][disk]):
					if disk_type == "primary" or disk_type == "secondary":
						host_name = mapping[storage_server][disk][disk_type]
						if host_name == "spare":
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



main()
