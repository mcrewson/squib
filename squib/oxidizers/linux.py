# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#

import sys, time

from squib.core.string_conversion import convert_to_bool, ConversionError
from squib.oxidizers.base         import PeriodicOxidizer

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

##############################################################################

class FileDescriptorOxidizer (PeriodicOxidizer):

    def run_once (self):
        f = open('/proc/sys/fs/file-nr', 'r')
        line = f.readline()
        f.close()
        fd = [ int(el) for el in line.split() ]
        print 'filedescriptors.used %d' % (fd[0])
        print 'filedescriptors.free %d' % (fd[1])
        print 'filedescriptors.max %d' % (fd[2])
        sys.stdout.flush()

##############################################################################

class InodeOxidizer (PeriodicOxidizer):

    def run_once (self):
        f = open('/proc/sys/fs/file-nr', 'r')
        line = f.readline()
        f.close()
        inode = [ int(el) for el in line.split() ]
        print 'inodes.used %d' % (inode[0])
        print 'inodes.free %d' % (inode[1])
        sys.stdout.flush()

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

##############################################################################

class TrafficOxidizer (PeriodicOxidizer):

    default_include_loopback = False

    def setup (self):
        super(TrafficOxidizer, self).setup()
        self.setup_loopback()
        self.prev_traf_stats = {}

    def setup_loopback (self):
        loopback = self.config.get('include_loopback')
        if loopback is None:
            self.include_loopback = self.default_include_loopback
        else:
            try:
                self.include_loopback = convert_to_bool(loopback)
            except ConversionError:
                raise ConfigError('%s::include_loopback must be a boolean' % self.name)

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

            if iface == 'lo' and not self.include_loopback: 
                continue

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

##############################################################################
## THE END
