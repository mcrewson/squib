# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id$
#

import time

from core.log import get_logger

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
        for m in self.all_metrics.values():
            m.report(lines, self.prefix, epoch)
        return lines

##############################################################################

class BaseMetric (object):

    def __init__ (self, name):
        self.name = name
        self.log = get_logger()

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
    pass

##############################################################################

class MeterMetric (BaseMetric):
    pass

##############################################################################

class HistogramMetric (BaseMetric):
    pass

##############################################################################

class TimerMetric (BaseMetric):
    pass

##############################################################################
## THE END
