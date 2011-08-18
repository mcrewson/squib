# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id$
#

import time

from core.log import get_logger

import statistics

##############################################################################

class MetricsRecorder (object):

    def __init__ (self, prefix=""):
        self.log = get_logger()
        self.prefix = prefix
        self.all_metrics = {}

    def record (self, name, value):
        mtype, value = self.parse_value(value)
        self.log.debug("mtype = %s" % mtype)
        self.log.debug("value = %s" % value)
        if mtype is None:
            self.log.warn("Ignored invalid metric: \"%s %s\"" % (name, value))
            return

        m = self.all_metrics.get(name)
        if m is None:
            m = mtype(name)
            self.all_metrics[name] = m
        elif not isinstance(m, mtype):
            self.log.warn("Previous metric with this name was a %s, not a %s" % (mtype.__name__, metric.__class__.__name__))
            return

        m.update(value)

    def parse_value (self, value_string):
        value_parts = value_string.split(' ')
        mtype_string = value_parts[0].lower()
        if mtype_string == 'gauge':
            mtype = GaugeMetric
        elif mtype_string in ('counter', 'cnt'):
            mtype = CounterMetric
        elif mtype_string in ('meter'):
            mtype = MeterMetric
        elif mtype_string in ('histogram', 'hist'):
            mtype = HistogramMetric
        else:
            # with no explicit metric type specified, default
            # to a gauge if the value_string is just a integer
            try:
                float(value_string)
                return GaugeMetric, value_string
            except ValueError:
                return None, value_string

        return mtype, ' '.join(value_parts[1:])

    def publish (self):
        epoch = int(time.time())
        lines = []
        allm = self.all_metrics.values()[:]
        allm.sort()
        for m in allm:
            m.report(lines, self.prefix, epoch)
        return lines

##############################################################################

class BaseMetric (object):

    def __init__ (self, name):
        self.name = name
        self.log = get_logger()

    def __cmp__ (self, other):
        return cmp(self.name, other.name)

    def update (self, value):
        raise NotImplementedError

    def report (self, lines, prefix, epoch):
        raise NotImplementedError

##############################################################################

class GaugeMetric (BaseMetric):

    value = 0
    def __init__ (self, name):
        super(GaugeMetric, self).__init__(name)
        self.log.debug("Created GaugeMetric(%s)" % name)

    def update (self, value):
        self.value = value

    def report (self, lines, prefix, epoch):
        lines.append("%s%s.value %s %d" % (prefix, self.name, self.value, epoch))

##############################################################################

class CounterMetric (BaseMetric):

    count = 0

    def __init__ (self, name):
        super(CounterMetric, self).__init__(name)
        self.log.debug("Created CounterMetric(%s)" % name)

    def update (self, value):
        if value[0] == '+':
            self.count += int(value[1:], 10)
        elif value[0] == '-':
            self.count -= int(value[1:], 10)
        else:
            self.count += int(value, 10)

    def report (self, lines, prefix, epoch):
        lines.append("%s%s.count %d %d" % (prefix, self.name, self.count, epoch))

##############################################################################

class MeterMetric (BaseMetric):

    count = 0

    def __init__ (self, name):
        super(MeterMetric, self).__init__(name)
        self.log.debug("Created CounterMetric(%s)" % name)

        self.startTime = time.time()

        self.m1_rate  = statistics.one_minute_ewma()
        self.m5_rate  = statistics.five_minute_ewma()
        self.m15_rate = statistics.fifteen_minute_ewma()

    def update (self, value):
        if value[0] == '+':
            cnt = int(value[1:], 10)
        else:
            cnt = int(value, 10)

        self.count += cnt
        self.m1_rate.update(cnt)
        self.m5_rate.update(cnt)
        self.m15_rate.update(cnt)

    def report (self, lines, prefix, epoch):
        lines.append("%s%s.count %d %d" % (prefix, self.name, self.count, epoch))
        lines.append("%s%s.meanRate %2.2f %d" % (prefix, self.name, self.meanRate(), epoch))
        lines.append("%s%s.1minuteRate %2.2f %d" % (prefix, self.name, self.m1_rate.averageValue(), epoch))
        lines.append("%s%s.5minuteRate %2.2f %d" % (prefix, self.name, self.m5_rate.averageValue(), epoch))
        lines.append("%s%s.15minuteRate %2.2f %d" % (prefix, self.name, self.m15_rate.averageValue(), epoch))

    def meanRate (self):
        if self.count == 0:
            return 0.0
        else:
            return self.count / (time.time() - self.startTime)

##############################################################################

class HistogramMetric (BaseMetric):
    pass

##############################################################################

class TimerMetric (BaseMetric):
    pass

##############################################################################
## THE END
