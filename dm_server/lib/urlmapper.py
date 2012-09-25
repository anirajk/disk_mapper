#!/bin/env python
"""
This module maps request to function based on the url and method
"""

import re
import os
import cgi
import urlrelay
from diskmapper import DiskMapper


@urlrelay.url('^.*$', 'GET')
def index(environ, start_response):
    """Handles GET requests
    """

    status = '202 Accepted'
    response_headers = [('Content-type', 'text/plain')]
    dm = DiskMapper(environ, start_response)

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
