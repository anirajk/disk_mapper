#!/bin/env python
"""
This module maps request to function based on the url and method
"""
import os
import glob
import hashlib
import json
from cgi import parse_qs


class StorageServer:

    def __init__(self, environ, start_response):
        self.environ  = environ
        self.query_string  = environ["QUERY_STRING"]
        self.start_response = start_response
        self.status = '400 Bad Request'
        self.response_headers = [('Content-type', 'text/plain')]

    def list(self):
        self.status = '202 Accepted'
        path = self.environ["PATH_TRANSLATED"]

        if not os.path.exists(path):
            self.status = '400 Bad Request'
            files = "File not found."
        elif not os.access(path, os.R_OK):
            self.status = '403 Forbidden'
            files = "Cannot access file."
        else:
            file_list = self._file_iterater(path)
            files = self._get_s3_path(file_list)

        self._start_response()
        return [files + "\n"]

    def get_file(self):
        self.status = '202 Accepted'
        files = []

        if "type=bad_disk" in self.query_string:
            files = "/var/tmp/disk_mapper/bad_disk"
        elif "type=dirty_files" in self.query_string:
            file_name = "dirty"
        elif "type=copy_completed" in self.query_string:
            file_name = "copy_completed"
        else:
            self.status = '400 Bad Request'
            self._start_response()
            return "Invalid file type"

        if "type=dirty_files"  in self.query_string or "type=copy_completed" in self.query_string:
            for partition_name in sorted(glob.glob('/var/www/html/membase_backup/partition_*')):
                files.append(partition_name + "/" + file_name)

        file_content = ""
        for file in files:
            if os.path.exists(file):
                f = open (file, "r") 
                file_content = file_content + f.read()
                f.close()

        self._start_response()
        return file_content

    def get_config(self):
        self.status = '202 Accepted'
        mapping = {}
        path = "/var/www/html/membase_backup/"
        for disk in sorted(os.listdir(path)):
            disk_path = os.path.join(path, disk)
            if os.path.isdir(disk_path):
                for type in  os.listdir(disk_path):
                    if type == "primary" or type == "secondary":
                        type_path = os.path.join(disk_path, type)
                        print type_path
                        if os.path.isdir(type_path):
                            for host_name in os.listdir(type_path):
                                mapping[host_name] = {"type" : type, "disk" : disk}

        self._start_response()
        return json.dumps(mapping)

    def initialize_host(self):
        self.status = '202 Accepted'
        qs = parse_qs(self.query_string)
        host_name =  qs["host_name"][0]
        type =  qs["type"][0]
        game_id =  qs["game_id"][0]
        disk =  qs["disk"][0]

        actual_path = os.path.join("/", disk, type, host_name)
        if not os.path.isdir(actual_path):
            os.makedirs(actual_path)
        
        document_root = self.environ["DOCUMENT_ROOT"]
        sym_link_name = os.path.join(document_root, game_id, host_name)
        sym_link_path = os.path.join(document_root, "membase_backup", disk, type, host_name)
        if not os.path.islink(sym_link_name):
            os.symlink(sym_link_path, sym_link_name)

        self.status = '201 Created'
        self._start_response()
        return qs

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
    
        f = open(path,'wb')
        while chunks is not 0:
            file_chunk = self.environ['wsgi.input'].read(4096)
            f.write(file_chunk)
            chunks -= 1

        file_chunk = self.environ['wsgi.input'].read(last_chunk_size)
        f.write(file_chunk)
        f.close()

        self._start_response()
        return ["Saved file to disk"]

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

        if os.path.isdir(path):
            for root, dirs, files in os.walk(path, topdown=False, followlinks=True):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
        else:
            os.remove(path)

        if os.path.exists(path):
            self.status = '400 Bad Request'
        else:
            self.status = '200 OK'
            
        self._start_response()
        return ["Deleted " + self.environ["SERVER_NAME"] + self.environ["PATH_INFO"]]

    def _is_host_initialized(self, path):
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

