#!/bin/env python
"""
This module maps request to function based on the url and method
"""

import re
import os
import cgi
import urlrelay
from storageserver import StorageServer
from cgi import parse_qs


@urlrelay.url('^.*$', 'GET')
def index(environ, start_response):
    """Handles GET requests
    """

    query_string = parse_qs(environ.get("QUERY_STRING"))
    status = '202 Accepted'
    response_headers = [('Content-type', 'text/plain')]
    ss = StorageServer(environ, start_response)

    if "action" in query_string:
        action = query_string["action"]
        if "list" in action:
            return ss.list()

        if "copy_host" in action:
            return ss.copy_host()

        if "add_entry" in action:
            return ss.add_entry()

        if "delete_file" in action:
            return ss.delete_merged_file()

        if "remove_entry" in action:
            return ss.remove_entry()

        if "get_file" in action:
            return ss.get_file()

        if "get_mtime" in action:
            return ss.get_mtime()

        if "get_game_id" in action:
            return ss.get_game_id()

        if "make_spare" in action:
            return ss.make_spare()

        if "get_config" in action:
            return ss.get_config()

        if "initialize_host" in action:
            return ss.initialize_host()

        if "create_torrent" in action:
            return ss.create_torrent()

        if "start_download" in action:
            return ss.start_download()

    start_response('200 OK', [('Content-type', 'text/plain')])
    return redirect(environ, start_response)


@urlrelay.url('^.*$', 'DELETE')
def delte(environ, start_response):
    """Handles GET requests
    """

    ss = StorageServer(environ, start_response)
    return ss.delete()

@urlrelay.url('^.*$', 'POST')
def upload(environ, start_response):

    ss = StorageServer(environ, start_response)
    return ss.save_to_disk()
    
# "RESTful" URL to application mapping
def redirect(environ, start_response):
    print "redirect"   
    start_response('302 FOUND', [('Content-type', 'text/plain'), 
                  ("Location", "http://" + environ["SERVER_NAME"] + environ["PATH_INFO"])])
    return ["Redirected"]

