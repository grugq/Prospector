# This file is part of PyBing (http://pybing.googlecode.com).
# 
# Copyright (C) 2009 JJ Geewax http://geewax.org/
# All rights reserved.
# 
# This software is licensed as described in the file COPYING.txt,
# which you should have received as part of this distribution.

"""
This module holds the Bing WebQuery class used to do web searches against Bing.
"""

from pybing import constants
from pybing.query import BingQuery, Pagable

class WebQuery(BingQuery, Pagable):
    SOURCE_TYPE = constants.WEB_SOURCE_TYPE

class FileTypeQuery(WebQuery):
    def __init__(self, app_id, query=None, filetype=None, version=None, *args, **kwargs):
        self.filetype = filetype
        super(FileTypeQuery, self).__init__(app_id, query, version, *args, **kwargs)

    def get_request_parameters(self):
        params = super(FileTypeQuery, self).get_request_parameters()
        params.update({
            'Web.FileType': self.filetype.upper(),
            'Web.Options' : "DisableHostCollapsing"
        })
        return params
