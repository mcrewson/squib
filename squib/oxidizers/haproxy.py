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

import socket, sys

from squib.core.config    import ConfigError
from squib.oxidizers.base import PeriodicOxidizer

##############################################################################

class HaproxyOxidizer (PeriodicOxidizer):

    stats = ( 'qcur', 'qmax', 'scur', 'smax', 'slim', 'stot', 'bin', 'bout',
              'dreq', 'dresp', 'erep', 'econ', 'eresp', 'wretr', 'wredis', 'status',
              'weight', 'act', 'bck', 'chkfail', 'chkdown', 'lastchg', 'downtime' )

    def setup (self):
        super(HaproxyOxidizer, self).setup()
        self.setup_stats_socket()
        self.num_stats = len(self.stats)

    def setup_stats_socket (self):
        self.stats_socket = self.config.get('stats_socket')
        if self.stats_socket is None:
            raise ConfigError('%s::stats_socket must be specified' % self.name)

    def run_once (self):
        raw = self.read_stats_socket()
        if raw is None: return
        for line in raw.split('\n'):
            if not line or line[0] == '#': continue
            parts = line.split(',', self.num_stats + 2)
            if parts[1] == 'BACKEND': continue
            for idx in range(self.num_stats):
                val = parts[idx+2]
                if val:
                    print '%s.%s.%s.%s %s' % (self.name, parts[0], parts[1], self.stats[idx], val)

    def read_stats_socket (self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            try:
                sock.connect(self.stats_socket)
                sock.sendall('show stat\n')
                results = ''
                data = sock.recv(8192)
                while data:
                    results += data
                    data = sock.recv(2048)
                return results
            finally:
                sock.close()
        except socket.error, msg:
            # Failed to retrieve the stats. Damn
            return None


##############################################################################

if __name__ == "__main__":
    HaproxyOxidizer('haproxy', dict(period=5.0, stats_socket='/var/run/haproxy.stats')).run()

##############################################################################
## THE END
