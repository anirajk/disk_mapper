#!/bin/env python
"""
This module maps request to function based on the url and method
"""
import os
import glob
import hashlib


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
        return [files]

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

    def _file_iterater(self, path, recursive=False):

        files = []

        if not os.path.isdir(path):
            self.response_headers.append(('Etag', self._get_md5sum(open(path, "r"))))
            return path
            
        if "recursive=true" not in self.query_string:
            for item in os.listdir(path):
                files.append(os.path.join(path, item))

        else:
            for root, dirnames, filenames in os.walk(path, followlinks=True):
                for name in filenames:
                    files.append(os.path.join(root, name))
                for name in dirnames:
                    files.append(os.path.join(root, name))

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

