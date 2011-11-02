#!/usr/bin/python2
# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#

import sys, time

from squib.oxidizers.base import PeriodicOxidizer

##############################################################################

class TrafficOxidizer (PeriodicOxidizer):

    def setup (self):
        super(TrafficOxidizer, self).setup()
        self.prev_traf_stats = {}

    def run_once (self):
        f = open('/proc/net/dev', 'r')
        lines = f.readlines()
        f.close()
        for line in lines[2:]:
            parts = line.strip().split()
            if parts[0][-1] == ':':
                iface = parts[0][:-1]
                rbytes = int(parts[1], 10)
                rpackets = int(parts[2], 10)
                rerrors = int(parts[3], 10)
                rdrops = int(parts[4], 10)
                tbytes = int(parts[9], 10)
                tpackets = int(parts[10], 10)
                terrors = int(parts[11], 10)
                tdrops = int(parts[12], 10)
            else:
                iface, rbytes = parts[0].split(':', 1)
                rbytes = int(rbytes, 10)
                rpackets = int(parts[1], 10)
                rerrors = int(parts[2], 10)
                rdrops = int(parts[3], 10)
                tbytes = int(parts[8], 10)
                tpackets = int(parts[9], 10)
                terrors = int(parts[10], 10)
                tdrops = int(parts[11], 10)

            if iface == 'lo': continue

            prev = self.prev_traf_stats.get(iface)
            if prev is None:
                self.prev_traf_stats[iface] = (rbytes, rpackets, rerrors, rdrops, 
                                               tbytes, tpackets, terrors, tdrops)
                continue
            
            rb_diff = rbytes - prev[0]
            rp_diff = rpackets - prev[1]
            re_diff = rerrors - prev[2]
            rd_diff = rdrops - prev[3]
            tb_diff = tbytes - prev[4]
            tp_diff = tpackets - prev[5]
            te_diff = terrors - prev[6]
            td_diff = tdrops - prev[7]
            self.prev_traf_stats[iface] = (rbytes, rpackets, rerrors, rdrops, 
                                 tbytes, tpackets, terrors, tdrops)

            print 'traffic.%s.rbytes meter +%d'   % (iface, rb_diff)
            print 'traffic.%s.rpackets meter +%d' % (iface, rp_diff)
            print 'traffic.%s.rerrors meter +%d'  % (iface, re_diff)
            print 'traffic.%s.rdrops meter +%d'   % (iface, rd_diff)
            print 'traffic.%s.tbytes meter +%d'   % (iface, tb_diff)
            print 'traffic.%s.tpackets meter +%d' % (iface, tp_diff)
            print 'traffic.%s.terrors meter +%d'  % (iface, te_diff)
            print 'traffic.%s.tdrops meter +%d'   % (iface, td_diff)
            sys.stdout.flush()

if __name__ == '__main__':
    TrafficOxidizer('traffic', dict(period=10.0)).run()

##############################################################################
## THE END
