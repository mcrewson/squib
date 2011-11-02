#!/usr/bin/python2
# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#

import sys, time

from squib.oxidizers.base import PeriodicOxidizer

##############################################################################

class CpuOxidizer (PeriodicOxidizer):

    def setup (self):
        super(CpuOxidizer, self).setup()
        self.prev_cpu_stats = self.raw_cpu()
        time.sleep(0.25)

    def run_once (self):
        raw = self.raw_cpu()
        diff = [ float(raw[x] - self.prev_cpu_stats[x]) for x in range(7) ]
        total = float(reduce(lambda x,y: x+y, diff))
        print 'cpu.user %.2f' % (diff[0] / total * 100)
        print 'cpu.nice %.2f' % (diff[1] / total * 100)
        print 'cpu.system %.2f' % (diff[2] / total * 100)
        print 'cpu.idle %.2f' % (diff[3] / total * 100)
        print 'cpu.iowait %.2f' % (diff[4] / total * 100)
        print 'cpu.irq %.2f' % (diff[5] / total * 100)
        print 'cpu.softirq %.2f' % (diff[6] / total * 100)
        sys.stdout.flush()
        self.prev_cpu_stats = raw

    def raw_cpu (self):
        f = open('/proc/stat', 'r')
        line = f.readline()
        f.close()
        return [ int(el) for el in line.split()[1:] ]


if __name__ == '__main__':
    CpuOxidizer('cpu', dict(period=10.0)).run()

##############################################################################
## THE END
