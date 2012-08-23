#!/bin/env python
"""
This module maps request to function based on the url and method
"""

import urlrelay
from storageserver import StorageServer


@urlrelay.url('^.*$', 'GET')
def index(environ, start_response):
    """Handles GET requests
    """

    query_string = environ["QUERY_STRING"]
    status = '202 Accepted'
    response_headers = [('Content-type', 'text/plain')]
    ss = StorageServer(environ, start_response)

    if "list=true" in query_string:
        return ss.list()

    start_response('200 OK', [('Content-type', 'text/plain')])
    return redirect(environ, start_response)


@urlrelay.url('^.*$', 'DELETE')
def delte(environ, start_response):
    """Handles GET requests
    """

    ss = StorageServer(environ, start_response)
    return ss.delete()

@urlrelay.url('^/$', 'POST')
def upload(environ, start_response):
    print "upload"   
    query_string = environ["QUERY_STRING"]
    if "upload=true" in query_string:
        s=environ['wsgi.input'].read(int(environ.get('CONTENT_LENGTH','0')))
        f = open('/tmp/upload.txt','w')
        f.write(s)
        start_response('201 OK', [('Content-type', 'text/plain')])
        return [str(environ)]

    start_response('200 OK', [('Content-type', 'text/plain')])
    return ['I am lost']


# "RESTful" URL to application mapping
def redirect(environ, start_response):
    print "redirect"   
    start_response('302 FOUND', [('Content-type', 'text/plain'), 
                  ("Location", "http://" + environ["SERVER_NAME"] + environ["PATH_INFO"])])
    return ["Redirected"]

