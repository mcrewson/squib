#!/usr/bin/python2
# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#

##############################################################################

import sys, time

def raw_cpu ():
    f = open('/proc/stat', 'r')
    line = f.readline()
    f.close()
    return [ int(el) for el in line.split()[1:] ]

_prev_cpu = None

def cpu_usage ():
    global _prev_cpu
    raw = raw_cpu()
    if _prev_cpu is not None:
        diff = [ float(raw[x] - _prev_cpu[x]) for x in range(7) ]
        _prev_cpu = raw
        total = float(reduce(lambda x,y: x+y, diff))
        print 'cpu.user %.2f' % (diff[0] / total * 100)
        print 'cpu.nice %.2f' % (diff[1] / total * 100)
        print 'cpu.system %.2f' % (diff[2] / total * 100)
        print 'cpu.idle %.2f' % (diff[3] / total * 100)
        print 'cpu.iowait %.2f' % (diff[4] / total * 100)
        print 'cpu.irq %.2f' % (diff[5] / total * 100)
        print 'cpu.softirq %.2f' % (diff[6] / total * 100)
        sys.stdout.flush()
    _prev_cpu = raw

def run (conf):
    global _prev_cpu
    period = conf.get('period', 10.0)
    _prev_cpu = raw_cpu()
    time.sleep(0.25)
    while True:
        start = time.time()
        cpu_usage()
        done = time.time()
        delay = period - (done - start)
        if delay > 0.0: time.sleep(delay)

if __name__ == '__main__':
    run(10.0)

##############################################################################
## THE END
