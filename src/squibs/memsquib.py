#!/usr/bin/python2
# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:

##############################################################################

import sys, time

def memory ():
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
    sys.stdout.flush()

def run (poll_interval):
    while True:
        start = time.time()
        memory()
        done = time.time()
        delay = poll_interval - (done - start)
        if delay > 0.0: time.sleep(delay)

if __name__ == '__main__':
    run(10.0)

##############################################################################
## THE END
