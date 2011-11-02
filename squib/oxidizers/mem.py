#!/usr/bin/python2
# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:

import sys

from squib.oxidizers.base import PeriodicOxidizer

##############################################################################

class MemOxidizer (PeriodicOxidizer):

    def run_once (self):
        f = open('/proc/meminfo', 'r')
        lines = f.readlines()
        f.close()
        mem = []
        for x in range(4):
            mem.append(int(lines[x].split()[1], 10) * 1024)
        print 'mem.total %d' % (mem[0])
        print 'mem.free %d' % (mem[1])
        print 'mem.buffers %d' % (mem[2])
        print 'mem.cached %d' % (mem[3])
        print 'mem.used %d' %(mem[0] - sum(mem[1:]))
        sys.stdout.flush()

if __name__ == '__main__':
    MemOxidizer('mem', dict(period=10.0)).run()

##############################################################################
## THE END
