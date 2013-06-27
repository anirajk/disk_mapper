#!/bin/env python

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
