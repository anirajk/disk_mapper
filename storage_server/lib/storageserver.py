#!/bin/env python
"""
This module maps request to function based on the url and method
"""
import os
import glob
import hashlib
import json
import fcntl
import subprocess
import time
import logging
from cgi import parse_qs
from signal import SIGSTOP, SIGCONT

logger = logging.getLogger('storage_server')
hdlr = logging.FileHandler('/var/log/storage_server.log')
formatter = logging.Formatter('%(asctime)s %(process)d %(thread)d %(filename)s %(lineno)d %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


class StorageServer:

    def __init__(self, environ, start_response):
        if environ != None:
            self.environ  = environ
            self.query_string  = environ["QUERY_STRING"]
            self.start_response = start_response
            self.status = '400 Bad Request'
            self.response_headers = [('Content-type', 'text/plain')]

    def list(self):
        self.status = '202 Accepted'
        path = self.environ["PATH_TRANSLATED"]

        logger.debug("file_path : " + path)
        if not os.path.exists(path):
            logger.debug("File not found : " + path)
            self.status = '400 Bad Request'
            files = "File not found."
        elif not os.access(path, os.R_OK):
            logger.debug("Cannot access file : " + path)
            self.status = '403 Forbidden'
            files = "Cannot access file."
        else:
            file_list = self._file_iterater(path)
            files = self._get_s3_path(file_list)

        self._start_response()
        return [files + "\n"]

    def add_entry(self):
        self.status = '202 Accepted'

        logger.debug("query : " + self.query_string)
        if "type=bad_disk" in self.query_string:
            file = "/var/tmp/disk_mapper/bad_disk"
        elif "type=dirty_files" in self.query_string:
            file_name = "dirty"
        elif "type=copy_completed" in self.query_string:
            file_name = "copy_completed"
        else:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid file type"

        qs = parse_qs(self.query_string)
        try:
            entry =  qs["entry"][0]
        except KeyError:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid arguments."

        if "type=dirty_files"  in self.query_string or "type=copy_completed" in self.query_string:
            for partition_name in sorted(glob.glob('/var/www/html/membase_backup/data_*')):
                file = partition_name + "/" + file_name
                self._append_to_file(file, entry)
        else:
            self._kill_torrent(entry)
            self._append_to_file(file, entry)

        self.status = '200 OK'
        self._start_response()
        return "Successfully add entry to file."

    def remove_entry(self):
        self.status = '202 Accepted'

        logger.debug("query : " + self.query_string)
        if "type=bad_disk" in self.query_string:
            file = "/var/tmp/disk_mapper/bad_disk"
        elif "type=dirty_files" in self.query_string:
            file_name = "dirty"
        elif "type=copy_completed" in self.query_string:
            file_name = "copy_completed"
        else:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid file type"

        qs = parse_qs(self.query_string)
        try:
            entry =  qs["entry"][0]
        except KeyError:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid arguments."

        if "type=dirty_files"  in self.query_string or "type=copy_completed" in self.query_string:
            for partition_name in sorted(glob.glob('/var/www/html/membase_backup/data_*')):
                file = partition_name + "/" + file_name
                self._remove_line_from_file(file, entry)
        else:
            self._remove_line_from_file(file, entry)
        
        self.status = '200 OK'
        self._start_response()
        return "Successfully removed entry from file."

    def _remove_line_from_file(self, file, entry):
        if not os.path.exists(file):
            return True
    
        lock = open("%s.lock" %file, 'w')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)

        f = open(file, 'r+')
        file_content = f.readlines()
        f.seek(0, 0)
        f.truncate()
        for line in file_content:
            if entry not in line:
                f.write(line)
        os.fsync(f)
        f.close()
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


    def get_file(self):
        self.status = '202 Accepted'

        if "type=bad_disk" in self.query_string:
            file = "/var/tmp/disk_mapper/bad_disk"
        elif "type=dirty_files" in self.query_string:
            file_name = "dirty"
        elif "type=copy_completed" in self.query_string:
            file_name = "copy_completed"
        else:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid file type"

        file_content = ""
        if "type=dirty_files"  in self.query_string or "type=copy_completed" in self.query_string:
            for partition_name in sorted(glob.glob('/var/www/html/membase_backup/data_*')):
                file = partition_name + "/" + file_name

                if os.path.exists(file):
                    f = open (file, "r") 
                    file_content = file_content + f.read() + "\n"
                    f.close()
        else:
            if os.path.exists(file):
                f = open (file, "r") 
                file_content = file_content + f.read()
                f.close()


        self.status = '200 OK'
        self._start_response()
        self.response_headers = [('Content-type', 'application/json')]
        return json.dumps(file_content)

    def make_spare(self):
        self.status = '202 Accepted'
        qs = parse_qs(self.query_string)
        try:
            type =  qs["type"][0]
            disk =  qs["disk"][0]
        except KeyError:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid arguments."
            
        path = os.path.join("/var/www/html/membase_backup/", disk, type)

        if not self._delete_file_folder(path):
            self.status = "400 Bad Request"
        
        document_root = self.environ["DOCUMENT_ROOT"]

        for subfolders in os.listdir(document_root):
            full_path = os.path.join(document_root, subfolders)
            if os.path.isdir(full_path):
                for file in os.listdir(full_path):
                    link = os.path.join(full_path, file)
                    if os.path.islink(link):
                        if os.path.join(disk, type) in os.readlink(link):
                            os.remove(link)
                
        self.status = "200 OK"
        self._start_response()
        return disk + "/" + type + " is a spare"

    def get_mtime(self):
        self.status = '202 Accepted'
        qs = parse_qs(self.query_string)
        try:
            host_name =  qs["host_name"][0]
            type =  qs["type"][0]
            disk =  qs["disk"][0]
        except KeyError:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid arguments."
            
        mapping = {}
        path = os.path.join("/var/www/html/membase_backup/", disk, type)
        
        if os.path.isdir(path):
            last_mtime = 0
            for root, dirs, files in os.walk(path, topdown=False, followlinks=True):
                for name in files:
                    mtime = os.path.getmtime(os.path.join(root, name))
                    if mtime > last_mtime: last_mtime = mtime
                for name in dirs:
                    mtime = os.path.getmtime(os.path.join(root, name))
                    if mtime > last_mtime: last_mtime = mtime

        self.status = "200 OK"
        self._start_response()
        return str(last_mtime)

    def get_config(self):
        self.status = '202 Accepted'
        mapping = {}
        path = "/var/www/html/membase_backup/"
        for disk in sorted(os.listdir(path)):
            mapping[disk] = {}
            disk_path = os.path.join(path, disk)
            if os.path.isdir(disk_path):
                for type in  os.listdir(disk_path):
                    if type == "primary" or type == "secondary":
                        type_path = os.path.join(disk_path, type)
                        if os.path.isdir(type_path):

                            if os.listdir(type_path) == []:
                                mapping[disk].update({type : "spare"})
                            else:
                                for host_name in os.listdir(type_path):
                                    mapping[disk].update({type : host_name})

        self.status = '200 OK'
        self._start_response()
        return json.dumps(mapping)

    def start_download(self):

        self.status = '202 Accepted'

        qs = parse_qs(self.query_string)
        try:
            file_path =  qs["file_path"][0]
            torrent_url =  qs["torrent_url"][0]
        except KeyError:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid arguments."
        #ps_cmd = 'ps ax | grep "aria2c -V" | grep "' + os.path.dirname(file_path + "/..") + '" | grep "follow-torrent"'
        #logger.debug("ps cmd : " + ps_cmd)
        #ps_status = subprocess.call(ps_cmd, shell=True)
        
        #if ps_status == 1792:
        #    logger.debug("Torrent already downloaded.")
        #    return True

        #if ps_status != 0:
        #    logger.error(ps_cmd)
        #    self.status = '200 OK'
        #    self._start_response()
        #    return "running"


        # aria2c --dir=/mydownloads --follow-torrent=mem --seed-time=0 --remove-control-file http://10.36.168.173/torrent/1347780080.torrent
        replica_ip = torrent_url.split("/")[2]
        if "primary" in file_path:
            replica_file = file_path.replace("primary", "secondary")
        else:
            replica_file = file_path.replace("secondary", "primary")

        self.pause_coalescer(file_path)
        cmd = 'aria2c -V --dir=' + os.path.dirname(file_path) + ' --out=' + os.path.basename(file_path) + ' --follow-torrent=mem --seed-time=0 --on-download-stop="/opt/storage_server/hook.sh" --allow-overwrite=true --remove-control-file ' + torrent_url + ' --file-allocation=none --bt-stop-timeout=30 ; [[ $? -eq 0 || $? -eq 7 || $? -eq 11 || $? -eq 13 ]] && curl -s \'http://' + replica_ip + '/api?action=remove_entry&type=dirty_files&entry=' + replica_file + '\''
        logger.debug("cmd to start download : " + cmd)
        self.status = '500 Internal Server Error'
        error_code = subprocess.call(cmd, shell=True)
        #if error_code == 768 or error_code == 1792:
        #   return True

        cmd1 = "zstore_cmd del " + torrent_url.replace("http://", "s3://") 
        logger.debug("Return code of seed cmd : " + str(error_code))
        if error_code != 0 and error_code != 7:
            logger.error("Failed to start download : " + cmd + " error code : " + str(error_code))
            self.resume_coalescer(file_path)
            subprocess.call(cmd1, shell=True)
            self._start_response()
            return "Failed to start download."

        logger.debug("cmd to del torrent file : " + cmd1)
        if subprocess.call(cmd1, shell=True):
            logger.error("Failed to delete torrent file : " + cmd1)
            self._start_response()
            return "Failed to remove torrent file."

        self.status = '200 OK'
        self._start_response()
        return "Sucessfully downloaded file."

    def create_torrent(self):
        self.status = '202 Accepted'

        qs = parse_qs(self.query_string)
        try:
            file_path =  qs["file_path"][0]
        except KeyError:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid arguments."
        torrent_folder = "/var/www/html/torrent"

        ps_cmd = 'ps ax | grep "aria2c -V" | grep "' + os.path.dirname(file_path) + '" | grep -v "follow-torrent"'
        logger.debug("ps cmd : " + ps_cmd)
        if subprocess.call(ps_cmd, shell=True) != 1:
            logger.error(ps_cmd)
            self.status = '200 OK'
            self._start_response()
            return "True"

        if not os.path.exists(torrent_folder):
            os.makedirs(torrent_folder)

        torrent_file_name = time.strftime('%s') + ".torrent"
        torrent_path = os.path.join(torrent_folder, torrent_file_name)
        # btmakemetafile.py http://10.34.231.215:6969/announce /home/sqadir/backup/ --target "/tmp/back_up.torrent"
        cmd = 'btmakemetafile.py http://' + self.environ["SERVER_ADDR"] + ':6969/announce ' + file_path + ' --target ' + torrent_path
        logger.debug("Create torrent cmd : " + cmd)
        cmd_status = subprocess.call(cmd, shell=True)
        logger.debug("Create torrent status : " + str(cmd_status))
        if cmd_status:
            logger.error("Failed to create torrent : " + cmd)
            if os.path.exists(torrent_path):
                os.remove(torrent_path)
            self.status = '500 Internal Server Error'
            self._start_response()
            return "Failed to create torrent."

        # aria2c -V --dir=/home/sqadir /var/www/html/torrent/bit.torrent --seed-ratio=1.0
        # aria2c -V --dir=/data_4/primary/game-mb-1/zc1/daily /var/www/html/torrent/1354155230.torrent --seed-ratio=0.0 --remove-control-file  --on-download-stop="/opt/storage_server/hook.sh" --stop=45 -D
        try:
            self.pause_coalescer(file_path)
        except:
            logger.error("Failed to pause coalescer.")
            if os.path.exists(torrent_path):
                os.remove(torrent_path)
            self.status = '500 Internal Server Error'
            self._start_response()
            return "Failed to pause coalescer."

        cmd1 = "aria2c -V --dir=" + os.path.dirname(file_path) + " " + torrent_path + ' --seed-ratio=0 --remove-control-file --stop=90 --on-download-stop="/opt/storage_server/hook.sh" -D &'
        logger.debug("cmd to seed torrent : " + cmd1)
        cmd1_status = subprocess.call(cmd1, shell=True)
        logger.debug("Status of seed cmd : " + str(cmd1_status))
        if cmd1_status:
            logger.error("Failed to seed : " + cmd1 + " return code" + str(cmd1_status))
            self.resume_coalescer(file_path)
            if os.path.exists(torrent_path):
                os.remove(torrent_path)

            self.status = '500 Internal Server Error'
            self._start_response()
            return "Failed to seed file."

        self.status = '200 OK'
        self._start_response()
        torrent_loc = 'http://' + torrent_path.replace(self.environ["DOCUMENT_ROOT"], self.environ["SERVER_ADDR"])
        return torrent_loc

    def initialize_host(self):
        self.status = '202 Accepted'
        qs = parse_qs(self.query_string)
        host_name =  qs["host_name"][0]
        type =  qs["type"][0]
        game_id =  qs["game_id"][0]
        disk =  qs["disk"][0]
        
        type_path = os.path.join("/", disk, type)
        if os.listdir(type_path) != []:
            self.status = '400 Bad Request'
            self._start_response()
            return "Disk is not spare."

        actual_path = os.path.join(type_path, host_name)
        if not os.path.isdir(actual_path):
            os.makedirs(actual_path)
        
        document_root = self.environ["DOCUMENT_ROOT"]
        sym_link_name = os.path.join(document_root, game_id, host_name)
        sym_link_path = os.path.join(document_root, "membase_backup", disk, type, host_name)

        if not os.path.isdir(os.path.dirname(sym_link_name)):
            os.makedirs(os.path.dirname(sym_link_name))

        if os.path.islink(sym_link_name):
            os.remove(sym_link_name)   
        os.symlink(sym_link_path, sym_link_name)

        self.status = '201 Created'
        self._start_response()
        return "Host initialized"

    def save_to_disk(self):
        self.status = '200 OK'
        path = self.environ["PATH_TRANSLATED"]
        path_info = self.environ["PATH_INFO"]

        if not self._is_host_initialized(path_info):
            self.status = '417 Expectation Failed'
            self._start_response()
            return "Host Not initialized for path : " + path_info
            
        block_size = 4096
        file_size = int(self.environ.get('CONTENT_LENGTH','0'))
        chunks = file_size / block_size
        last_chunk_size = file_size % block_size

        dir_name = os.path.dirname(path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        self.pause_coalescer(path)
        f = open(path,'wb')
        while chunks is not 0:
            file_chunk = self.environ['wsgi.input'].read(4096)
            f.write(file_chunk)
            chunks -= 1

        file_chunk = self.environ['wsgi.input'].read(last_chunk_size)
        f.write(file_chunk)
        os.fsync(f)
        f.close()

        document_root = self.environ["DOCUMENT_ROOT"]
        splits =  path_info.split("/")
        host_symlink = os.path.join(document_root, splits[1], splits[2])
        host_path = os.readlink(host_symlink)
        actual_path_prefix = host_path.replace("/var/www/html/membase_backup", "")
        actual_path = path.replace(host_symlink, actual_path_prefix)
        dirty_file = os.path.join(host_symlink, "..", "..", "dirty")
        #self._append_to_file(dirty_file, os.path.dirname(actual_path))
        self._append_to_file(dirty_file, actual_path)
        self.resume_coalescer(path)

        self._start_response()
        return ["Saved file to disk"]

    def _append_to_file(self, file, line):
        f = open(file, 'a+')
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(line + "\n")
        os.fsync(f)
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()    

    def delete(self):
        self.status = '202 Accepted'
        path = self.environ["PATH_TRANSLATED"]

        if not os.path.exists(path):
            self.status = '404 Not Found'
            self._start_response()
            return ["File not found."]
            
        if not os.access(path, os.W_OK):
            self.status = '403 Forbidden'
            self._start_response()
            return ["No permission to delete file."]

        if self._delete_file_folder(path):
            d = os.path.dirname(path)
            f = os.path.basename(path)

            if f.endswith(".torrent") and d.endswith("/torrent"):
                os.system("ps -eo pid,args | grep %s | grep  aria2 | cut -d' ' -f2 | xargs kill -9" %f)

            self.status = '200 OK'
        else:
            self.status = '400 Bad Request'
            
        self._start_response()
        return ["Deleted " + self.environ["SERVER_NAME"] + self.environ["PATH_INFO"]]


    def _delete_file_folder(self, path):
        try:
            if os.path.isdir(path):
                for root, dirs, files in os.walk(path, topdown=False, followlinks=True):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
            else:
                os.remove(path)

            if os.path.exists(path):
                return False
            return True
        except:
            return False

    def _is_host_initialized(self, path, create=False):
        subfolders = path.split('/')
        document_root = self.environ["DOCUMENT_ROOT"]
        host_folder = os.path.join(document_root, subfolders[1], subfolders[2])
        if os.path.isdir(host_folder):
            return True
        return False

    def _file_iterater(self, path, recursive=False):

        files = []

        if not os.path.isdir(path):
            self.response_headers.append(('Etag', self._get_md5sum(open(path, "r"))))
            return path
            
        if "recursive=true" not in self.query_string:
            for item in os.listdir(path):
                full_path = os.path.join(path, item)
                if os.path.isdir(full_path):
                    full_path = full_path + "/"
                files.append(full_path)

        else:
            for root, dirnames, filenames in os.walk(path, followlinks=True):
                for name in filenames:
                    files.append(os.path.join(root, name))
                for name in dirnames:
                    files.append(os.path.join(root, name) + "/")

        return "\n".join(sorted(files))
    
    def _get_md5sum(self, file, block_size=2**20):
        md5 = hashlib.md5()
        while True:
            data = file.read(block_size)
            if not data:
                break
            md5.update(data)
        return md5.hexdigest()

    def _get_s3_path(self, files):

        return files.replace(self.environ["DOCUMENT_ROOT"], "s3://" +
                                         self.environ["SERVER_NAME"] )

    def _start_response(self):
        self.start_response(self.status, self.response_headers)

    def pause_coalescer(self, path):
        disk_id = path.split("/")[1][-1:]
        daily_merge_pfile = "/var/run/daily-merge-disk-" + disk_id + ".pid"
        master_merge_pfile = "/var/run/master-merge-disk-" + disk_id + ".pid"
        daily_pid = self._get_value_pid_file(daily_merge_pfile)
        master_pid = self._get_value_pid_file(master_merge_pfile)
        try:
            if daily_pid is not False:
                #os.kill(int(daily_pid), SIGSTOP)
                subprocess.call("sudo kill -SIGSTOP -" + daily_pid , shell=True)
            if master_pid is not False:
                #os.kill(int(master_pid), SIGSTOP)
                subprocess.call("sudo kill -SIGSTOP -" + master_pid , shell=True)
        except:
            #os.kill(int(daily_pid), SIGCONT)
            subprocess.call("sudo kill -SIGCONT -" + daily_pid , shell=True)
            #os.kill(int(master_pid), SIGCONT)
            subprocess.call("sudo kill -SIGCONT -" + master_pid , shell=True)

    def resume_coalescer(self, path):
        disk_id = path.split("/")[1][-1:]
        daily_merge_pfile = "/var/run/daily-merge-disk-" + disk_id + ".pid"
        master_merge_pfile = "/var/run/master-merge-disk-" + disk_id + ".pid"
        daily_pid = self._get_value_pid_file(daily_merge_pfile)
        master_pid = self._get_value_pid_file(master_merge_pfile)
        if os.path.exists(daily_merge_pfile):
            #os.kill(int(daily_pid), SIGCONT)
            subprocess.call("sudo kill -SIGCONT -" + daily_pid , shell=True)
        if os.path.exists(master_merge_pfile):
            #os.kill(int(master_pid), SIGCONT)
            subprocess.call("sudo kill -SIGCONT -" + master_pid , shell=True)

    def _get_value_pid_file(self, file):
        try:
            if os.path.exists(file):
                f = open(file, "r")
                return f.read().strip()
        except:
            return False
        else:
            return False

    def get_game_id(self):
        self.status = '202 Accepted'
        qs = parse_qs(self.query_string)
        try:
            host_name =  qs["host_name"][0]
        except KeyError:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid arguments."

        document_root = self.environ["DOCUMENT_ROOT"]

        self.status = "200 OK"
        self._start_response()

        for subfolders in os.listdir(document_root):
            full_path = os.path.join(document_root, subfolders)
            if os.path.isdir(full_path):
                for file in os.listdir(full_path):
                    if file == host_name:
                        return subfolders

        self.status = "404 Not Found"
        self._start_response()
        return "False"

    def _kill_torrent(self, disk):
        cmd = "kill -9 $(ps ax | grep aria2c | grep " + disk + " | awk '{print $1}')"
        subprocess.call(cmd, shell=True)
