# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# Copyright 2011 Mark Crewson <mark@crewson.net>
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

import sys
import socket_metrics

from squib.oxidizers.base         import PeriodicOxidizer

##############################################################################

class TcpSocketsOxidizer (PeriodicOxidizer):

    def run_once (self):
        try:
            states = socket_metrics.socket_states_count()
        except Exception:
            # Failed to retrieve the stats. Damn.
            return

        print 'tcpsockets.established %d' % states[0]
        print 'tcpsockets.syn_sent %d'    % states[1]
        print 'tcpsockets.syn_recv %d'    % states[2]
        print 'tcpsockets.fin_wait1 %d'   % states[3]
        print 'tcpsockets.fin_wait2 %d'   % states[4]
        print 'tcpsockets.time_wait %d'   % states[5]
        print 'tcpsockets.close %d'       % states[6]
        print 'tcpsockets.close_wait %d'  % states[7]
        print 'tcpsockets.last_ack %d'    % states[8]
        print 'tcpsockets.listen %d'      % states[9]
        print 'tcpsockets.closed %d'      % states[10]
        sys.stdout.flush()

##############################################################################

if __name__ == "__main__":
    TcpSocketsOxidizer('tcpsockets', dict(period=5.0)).run()

##############################################################################
## THE END
