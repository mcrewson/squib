# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# Copyright 2012 Mark Crewson <mark@crewson.net>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket, sys, urlparse

from mccorelib.config    import ConfigError
from squib.oxidizers.base import PeriodicOxidizer

##############################################################################

class ApacheOxidizer (PeriodicOxidizer):

    default_address = 'localhost'
    default_port    = 80

    def setup (self):
        super(ApacheOxidizer, self).setup()
        self.setup_status_url()

    def setup_status_url (self):
        status_url = self.config.get('status_url')
        if status_url is None:
            raise ConfigError('%s::status_url must be specified' % self.name)

        # Parse the url and make sure we can use it
        url_parts = urlparse.urlparse(status_url)
        if url_parts[0] and url_parts[0].lower() != 'http':
            raise ConfigError('%s::status_url only supports the HTTP protocol' % self.name)

        address = url_parts[1]
        if not address:
            self.address = self.default_address
            self.port    = self.default_port
        else:
            if '@' in address:
                raise ConfigError('%s::status_url does not support username/passwords' % self.name)
            if ':' in address:
                self.address, self.port = address.split(':', 1)
                try:
                    self.port = convert_to_integer(self.port)
                except ConversionError:
                    raise ConfigError('%s::status_url ports must be specified as an integer number' % self.name)
            else:
                self.address = address
                self.port = self.default_port

        path, params, query, fragments = url_parts[2:6]
        if not query:
            query = 'auto'
        elif 'auto' not in query:
            query += '&auto'

        self.request = path
        if params:
            self.request += ';' + params
        self.request += '?' + query
        if fragments:
            self.request += '#' + fragments

    def run_once (self):
        raw = self.read_raw_status()
        if raw is None: return
        scoreboard = { '_':0, 'S':0, 'R':0, 'W':0, 'K':0, 'D':0, 'C':0, 'L':0, 'G':0, 'I':0, '.':0 }
        for line in raw.split('\n'):
            if not line: continue
            key, value = [ l.strip() for l in line.split(':', 1) ]
            if key == 'Total Accesses':
                print '%s.requests derivmeter %s' % (self.name, value)
            elif key == 'Total kBytes':
                print '%s.kbytes derivmeter %s' % (self.name, value)
            elif key == 'BusyWorkers':
                print '%s.busyworkers gauge %s' % (self.name, value)
            elif key == 'IdleWorkers':
                print '%s.idleworkers gauge %s' % (self.name, value)
            elif key == 'Scoreboard':
                for j in range(len(value)):
                    scoreboard[value[j]] += 1
                print '%s.scoreboard.waiting gauge %s'     % (self.name, scoreboard['_'])
                print '%s.scoreboard.starting gauge %s'    % (self.name, scoreboard['S'])
                print '%s.scoreboard.reading gauge %s'     % (self.name, scoreboard['R'])
                print '%s.scoreboard.writing gauge %s'     % (self.name, scoreboard['W'])
                print '%s.scoreboard.keepalive gauge %s'   % (self.name, scoreboard['K'])
                print '%s.scoreboard.dnslookup gauge %s'   % (self.name, scoreboard['D'])
                print '%s.scoreboard.closing gauge %s'     % (self.name, scoreboard['C'])
                print '%s.scoreboard.logging gauge %s'     % (self.name, scoreboard['L'])
                print '%s.scoreboard.finishing gauge %s'   % (self.name, scoreboard['G'])
                print '%s.scoreboard.idlecleanup gauge %s' % (self.name, scoreboard['I'])
                print '%s.scoreboard.openslot gauge %s'    % (self.name, scoreboard['.'])


    def read_raw_status (self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                sock.connect((self.address, self.port))
                sock.send('GET %s HTTP 1.0\r\nHost: %s\r\nConnection: close\r\n\r\n' % (self.request, self.address))
                results = ''
                data = sock.recv(8192)
                while data:
                    results += data
                    data = sock.recv(2048)
            finally:
                sock.close()
        except socket.error, msg:
            # Failed to retrieve status. Damn
            return None

        headers, body = results.split('\r\n\r\n', 1)
        headers = headers.split('\r\n')
        if not (headers[0].lower().startswith('http/') and headers[0].lower().endswith('200 ok')):
            # Did not return a '200 OK' result. Damn
            return None

        return body

##############################################################################

if __name__ == "__main__":
    ApacheOxidizer('apache', dict(period=5.0, status_url='http://localhost/server-status?auto')).run()

##############################################################################
## THE END
