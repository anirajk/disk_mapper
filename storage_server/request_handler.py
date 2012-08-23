#!/bin/env python
""" 
Wsgi app to process apache requests.
"""

import urlrelay
import urlmapper

def application(environ, start_response):
    """Default app
    """

    # Pass request to URLRelay to redirect request based on url
    return urlrelay.URLRelay()(environ, start_response)

