#!/bin/env python
"""
This module maps request to function based on the url and method
"""

import re
import os
import cgi
import urlrelay
from cgi import parse_qs
from diskmapper import DiskMapper


@urlrelay.url('^.*$', 'GET')
def index(environ, start_response):
    """Handles GET requests
    """

    query_string = parse_qs(environ.get("QUERY_STRING"))
    status = '202 Accepted'
    response_headers = [('Content-type', 'text/plain')]
    dm = DiskMapper(environ, start_response)
    if "action" in query_string:
        action = query_string["action"]
        if "get_host_config" in action:
            return dm.get_host_config()
		elif "get_all_config" in action:
            return dm.get_all_config()

    return dm.forward_request()

@urlrelay.url('^.*$', 'DELETE')
def delete(environ, start_response):
    """Handles GET requests
    """

    dm = DiskMapper(environ, start_response)
    return dm.forward_request()

@urlrelay.url('^.*$', 'POST')
def upload(environ, start_response):

    dm = DiskMapper(environ, start_response)
    return dm.upload()
