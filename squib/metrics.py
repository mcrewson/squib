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

import math, platform, time

from squib                        import statistics
from squib.core.baseobject        import NonStdlibError
from squib.core.log               import getlog
from squib.core.string_conversion import ConversionError, convert_to_integer

##############################################################################

class MetricError (NonStdlibError):
    pass

if platform.architecture()[0] == '64bit':
    MAX_COUNTER = (2 ** 64) - 1
else:
    MAX_COUNTER = (2 ** 32) - 1

##############################################################################

class MetricsRecorder (object):

    def __init__ (self, prefix=""):
        self.log = getlog()
        self.prefix = prefix
        self.all_metrics = {}
        self.selfstats = None

    def set_selfstats (self, selfstats):
        self.selfstats = selfstats

    def record (self, name, value):
        mtype, mtype_args, pvalue = self.parse_value(value)

        full_name = "%s:%s:%s" % (name, mtype, mtype_args)
        m = self.all_metrics.get(full_name)
        if m is None:
            # New metric. Create it
            if mtype is None:
                self.log.warn("Ignored invalid metric: \"%s %s\"" % (name, value))
                self.all_metrics[full_name] = InvalidMetric(name)
                return

            try:
                if mtype_args is None:
                    m = mtype(name)
                else:
                    m = mtype(name, *mtype_args)
            except MetricError:
                self.log.warn("Ignored invalid metric: \"%s %s\"" % (name, value))
                self.all_metrics[full_name] = InvalidMetric(name)
                return

            self.all_metrics[full_name] = m

        elif isinstance(m, InvalidMetric):
            return

        if self.selfstats is not None:
            self.selfstats.mark_metrics_record()
        m.update(pvalue)

    def parse_value (self, value_string):
        value_parts = value_string.split(' ')
        mtype_string = value_parts[0].lower()

        mtype_args = None
        paren_open = mtype_string.find('(')
        paren_clos = mtype_string.find(')')
        if paren_open >= 0 and paren_clos >= 0 and paren_clos > paren_open+1:
            mtype_args = mtype_string[paren_open+1:paren_clos]
            mtype_string = mtype_string[:paren_open]

        if mtype_string == 'gauge':
            mtype = GaugeMetric
        elif mtype_string in ('counter', 'cnt'):
            mtype = CounterMetric
        elif mtype_string == 'derivgauge':
            mtype = DerivativeGaugeMetric
        elif mtype_string == 'meter':
            mtype = MeterMetric
        elif mtype_string == 'derivmeter':
            mtype = DerivativeMeterMetric
        elif mtype_string in ('histogram', 'hist'):
            mtype = HistogramMetric
        else:
            # with no explicit metric type specified, default
            # to a gauge if the value_string is just a integer
            try:
                float(value_string)
                return GaugeMetric, None, value_string
            except ValueError:
                return None, None, value_string

        return mtype, mtype_args, ' '.join(value_parts[1:])

    def publish (self):
        if self.selfstats is not None:
            self.selfstats.mark_metrics_report()
        epoch = int(time.time())
        lines = []
        allm = self.all_metrics.values()[:]
        allm.sort()
        for m in allm:
            m.report(lines, self.prefix, epoch)
        return lines

##############################################################################

class BaseMetric (object):

    def __init__ (self, name, *args):
        self.name = name
        self.parse_args(args)
        self.log = getlog()

    def __cmp__ (self, other):
        return cmp(self.name, other.name)

    def parse_args (self, args):
        pass

    def update (self, value):
        raise NotImplementedError

    def report (self, lines, prefix, epoch):
        raise NotImplementedError

##############################################################################

class InvalidMetric (BaseMetric):
    def update (self, value):
        pass
    def report (self, lines, prefix, epoch):
        pass

##############################################################################

class GaugeMetric (BaseMetric):

    def __init__ (self, name, *args):
        super(GaugeMetric, self).__init__(name, *args)
        self.value = 0

    def update (self, value):
        self.value = value

    def report (self, lines, prefix, epoch):
        lines.append("%s%s.value %s %d" % (prefix, self.name, self.value, epoch))

##############################################################################

class CounterMetric (BaseMetric):

    def __init__ (self, name, *args):
        super(CounterMetric, self).__init__(name, *args)
        self.count = 0

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

class DerivativeMetric (BaseMetric):

    def __init__ (self, name, *args):
        super(DerivativeMetric, self).__init__(name, *args)
        self.last_value = 0

    def parse_args (self, args):
        if not args:
            self.max_value = MAX_COUNTER
        else:
            try:
                self.max_value = convert_to_integer(args[0])
            except ConversionError:
                raise MetricError('max_value must be an integer')

    def derivative (self, value):
        value = int(value, 10)
        if self.last_value == 0:
            result = 0
        else:
            old = self.last_value
            if value < self.last_value:
                old = old - self.max_value
            result = value - old

        self.last_value = value
        return result

##############################################################################

class DerivativeGaugeMetric (GaugeMetric, DerivativeMetric):

    def __init__ (self, name, *args):
        GaugeMetric.__init__(self, name, *args)
        DerivativeMetric.__init__(self, name, *args)
        self.last_time = time.time()

    def derivative (self, value):
        # Derive over a timeperiod as well..
        now = time.time()
        result = super(DerivativeGaugeMetric, self).derivative(value)
        if result != 0:
            result = result / float(now - self.last_time)
        self.last_time = now
        return result
        
    def update (self, value):
        self.value = self.derivative(value)

##############################################################################

class MeterMetric (BaseMetric):

    def __init__ (self, name, *args):
        super(MeterMetric, self).__init__(name, *args)

        self.count = 0
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
        lines.append("%s%s.meanRate %2.2f %d" % (prefix, self.name, self.mean_rate(), epoch))
        lines.append("%s%s.1minuteRate %2.2f %d" % (prefix, self.name, self.m1_rate.averageValue(), epoch))
        lines.append("%s%s.5minuteRate %2.2f %d" % (prefix, self.name, self.m5_rate.averageValue(), epoch))
        lines.append("%s%s.15minuteRate %2.2f %d" % (prefix, self.name, self.m15_rate.averageValue(), epoch))

    def mean_rate (self):
        if self.count == 0:
            return 0.0
        else:
            return self.count / (time.time() - self.startTime)

##############################################################################

class DerivativeMeterMetric (MeterMetric, DerivativeMetric):

    def __init__ (self, name, *args):
        MeterMetric.__init__(self, name, *args)
        DerivativeMetric.__init__(self, name, *args)

    def update (self, value):
        if value[0] == '+':
            value = value[1:]
        cnt = self.derivative(value)
        self.count += cnt
        self.m1_rate.update(cnt)
        self.m5_rate.update(cnt)
        self.m15_rate.update(cnt)

##############################################################################

class HistogramMetric (BaseMetric):

    def __init__ (self, name, *args):
        super(HistogramMetric, self).__init__(name, *args)
        self.count = 0
        self.max_val = None
        self.min_val = None
        self.sum_val = 0
        self.variance = (-1, 0)
        self.sample = statistics.five_minute_eds()

    def update (self, value):
        val = int(value, 10)

        self.count += 1
        self.sample.update(val)
        self.set_max(val)
        self.set_min(val)
        self.sum_val= self.sum_val + val
        self.update_variance(val)

    def report (self, lines, prefix, epoch):
        percentiles = self.sample.percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999)
        lines.append("%s%s.min %2.2f %d"          % (prefix, self.name, self.min_rate(), epoch))
        lines.append("%s%s.max %2.2f %d"          % (prefix, self.name, self.max_rate(), epoch))
        lines.append("%s%s.mean %2.2f %d"         % (prefix, self.name, self.mean_rate(), epoch))
        lines.append("%s%s.stddev %2.2f %d"        % (prefix, self.name, self.std_dev(), epoch))
        lines.append("%s%s.median %2.2f %d"        % (prefix, self.name, percentiles[0], epoch))
        lines.append("%s%s.75percentile %2.2f %d"  % (prefix, self.name, percentiles[1], epoch))
        lines.append("%s%s.95percentile %2.2f %d"  % (prefix, self.name, percentiles[2], epoch))
        lines.append("%s%s.98percentile %2.2f %d"  % (prefix, self.name, percentiles[3], epoch))
        lines.append("%s%s.99percentile %2.2f %d"  % (prefix, self.name, percentiles[4], epoch))
        lines.append("%s%s.999percentile %2.2f %d" % (prefix, self.name, percentiles[5], epoch))

    def set_max (self, value):
        if self.max_val is None:
            self.max_val = value
        else:
            self.max_val = max(self.max_val, value)

    def set_min (self, value):
        if self.min_val is None:
            self.min_val = value
        else:
            self.min_val = min(self.min_val, value)

    def update_variance (self, value):
        old_values = self.variance
        if old_values[0] == -1:
            new_values = (value, 0)
        else:
            oldm, olds = old_values
            newm = oldm + ((value - oldm) / self.count)
            news = olds + ((value - oldm) * (value - newm))
            new_values = (newm, news)
        self.variance = new_values

    def get_variance (self):
        if self.count <= 1:
            return 0.0
        return self.variance[1] / (self.count - 1)

    def max_rate (self):
        return self.max_val or 0.0

    def min_rate (self):
        return self.min_val or 0.0

    def mean_rate (self):
        if self.count > 0:
            return self.sum_val / float(self.count)
        return 0.0

    def std_dev (self):
        if self.count > 0:
            return math.sqrt(self.get_variance())
        return 0.0

##############################################################################
## THE END
