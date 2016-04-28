#!/usr/bin/env python
"""Client for making requests to lightblue platform."""

"""
This file is part of python-lightblueclient.

python-lightblueclient is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

python-lightblueclient is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with python-lightblueclient.  If not, see <http://www.gnu.org/licenses/>.
"""

import json
import httplib
import ssl
from urlparse import urlparse


class DataConnection:
    """A connection to a lightblue data service.

    Can be used with the `with` statement.
    """

    def __init__(self, url, cert_file=None, ssl_context=None):
        """Open a connection to a lightblue data service.

        Parameters
        ----------
        url : url for the data endpoint
        cert_file : file to be used for client cert auth, optional
        ssl_context : SSL context to be used for SSL/TLS, optional

        """
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname
        port = parsed_url.port or 443
        self.path = parsed_url.path
        if self.path.endswith('/'):
            self.path = self.path[:-1]
        if ssl_context:
            self.connection = httplib.HTTPSConnection(hostname, port,
                                                      context=ssl_context)
        elif ssl._create_unverified_context:
            self.connection = httplib.HTTPSConnection(
                hostname,
                port,
                cert_file=cert_file,
                context=ssl._create_unverified_context()
            )
        else:
            self.connection = httplib.HTTPSConnection(hostname, port,
                                                      cert_file=cert_file)

    def close(self):
        """Close the connection."""
        self.connection.close()

    def find(self, entity, version, projection=None, query=None, range=None,
             sort=None, request={}):
        """Do a find request for a particular version of an entity.

        You can either construct the request as a dict or str, or pass parts
        of the request, or no request in order to fetch all instances of the
        entity.

        Parameters
        ----------
        entity : name of the entity
        version : version of the entity
        projection : dict, optional
        query : dict, optional
        range : list, optional
        sort : dict, optional
        request : dict or string, optional
        """
        path = '{0}/find/{1}/{2}'.format(self, path, entity, entity_version)
        if projection:
            request['projection'] = projection
        if query:
            request['query'] = query
        if range:
            request['range'] = range
        if sort:
            request['sort'] = sort
        if request:
            if type(request) is not str:
                request = json.dumps(request)
            headers = {'Content-Type': 'application/json'}
            self.connection.request('POST', path, request, headers)
        else:
            self.connection.request('GET', path)
        data = self.connection.getresponse().read()
        return json.loads(data)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
