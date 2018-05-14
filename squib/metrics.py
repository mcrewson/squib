# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# Copyright 2011-2018 Mark Crewson <mark@crewson.net>
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

from mccorelib.baseobject        import NonStdlibError
from mccorelib.application       import OperationError
from mccorelib.log               import getlog
from mccorelib.string_conversion import ConversionError, convert_to_integer
from squib                       import statistics

try:
    import json
    json_encode = json.dumps
    json_decode = json.loads
except ImportError:
    import jsonlib
    json_encode = jsonlib.encode
    json_decode = jsonlib.decode

##############################################################################

class MetricError (NonStdlibError):
    pass

if platform.architecture()[0] == '64bit':
    MAX_COUNTER = (2 ** 64) - 1
else:
    MAX_COUNTER = (2 ** 32) - 1

##############################################################################

class MetricsRecorder (object):

    def __init__ (self, prefix="", save_file=None):
        self.log = getlog()
        self.prefix = prefix
        self.save_file = save_file
        self.saved_metrics = None
        self.all_metrics = {}
        self.selfstats = None
        self.load_saved_metrics()

    def set_selfstats (self, selfstats):
        self.selfstats = selfstats

    def record (self, name, value):
        mtype, mtype_args, pvalue = self.parse_value(value)

        full_name = "%s:%s:%s" % (name, mtype.__name__, mtype_args)
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
                self.restore_metric(m, full_name)
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

        if mtype_string == 'string':
            mtype = StringMetric
        elif mtype_string == 'gauge':
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

    def save (self):
        if self.save_file is None: return
        epoch = int(time.time())
        lines = [ '# Squib metrics save file',
                  '# ** DO NOT EDIT **',
                  'timestamp %s' % epoch,
                ]
        for mname, metric in self.all_metrics.items():
            mdata = json_encode(metric.save())
            if mdata is None: continue
            lines.append('%s %s' % (mname, mdata))
        try:
            fp = open(self.save_file, 'w')
            fp.write('\n'.join(lines) + '\n')
            fp.close()
        except (IOError, OSError), why:
            raise OperationError("Failed to save metrics to a file: %s" % str(why))

    def load_saved_metrics (self):
        if self.save_file is None: return
        try:
            fp = open(self.save_file, 'r')
            lines = fp.readlines()
            fp.close()
        except (IOError, OSError), why:
            self.log.debug("Not loading a saved metrics file: %s" % str(why))
            return

        epoch = None
        saved_metrics = {}
        for line in lines:
            line = line.strip()
            if not line or line[0] == '#': continue
            if line.startswith('timestamp '):
                try:
                    epoch = int(line.split(' ', 1)[1])
                except (IndexError, ValueError), why:
                    self.log.debug("Not loading a saved metrics file (%s): invalid timestamp" % self.save_file)
                    return

            else:
                try:
                    mname, mdata = line.split(' ', 1)
                    saved_metrics[mname] = json_decode(mdata)
                except (IndexError, ValueError), why:
                    self.log.debug("Skipping saved metric (%s): invalid format in file (%s)" % (mname, str(why)))
                    continue

        self.saved_epoch = epoch
        self.saved_metrics = saved_metrics
        self.log.debug("Loaded %d saved metrics from file (%s)" % (len(self.saved_metrics), self.save_file))

    def restore_metric (self, metric, mname):
        if self.saved_metrics is None: return
        mdata = self.saved_metrics.get(mname)
        if mdata is not None:
            metric.load(mdata, self.saved_epoch)

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

    def save (self):
        return None

    def load (self, metric_data, timestamp):
        pass

##############################################################################

class InvalidMetric (BaseMetric):
    def update (self, value):
        pass
    def report (self, lines, prefix, epoch):
        pass

##############################################################################

class StringMetric (BaseMetric):

    def __init__ (self, name, *args):
        super(StringMetric, self).__init__(name, *args)
        self.value = 0

    def update (self, value):
        self.value = value

    def report (self, lines, prefix, epoch):
        lines.append("%s%s.string \"%s\" %d" % (prefix, self.name, self.value, epoch))

##############################################################################

class GaugeMetric (BaseMetric):

    def __init__ (self, name, *args):
        super(GaugeMetric, self).__init__(name, *args)
        self.value = 0

    def update (self, value):
        self.value = value

    def report (self, lines, prefix, epoch):
        lines.append("%s%s.value %s %d" % (prefix, self.name, self.value, epoch))

    def save (self):
        return { 'value': self.value }

    def load (self, data, timestamp):
        self.value = data['value']

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

    def save (self):
        return { 'count': self.count }

    def load (self, data, timestamp):
        self.count = data['count']

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
            if value < self.last_value:
                if self.last_value + abs(value) > self.max_value:
                    # overflow! attempt compensate for it.
                    self.log.warn("Derivative overflow for %s ! Attempting to compensate: (value=%d, last_value=%d, max_value=%d)"
                                  % (self.name, value, self.last_value, self.max_value))
                    result = value - (self.last_value - self.max_value)
                else:
                    # reset! same as if last_value == 0
                    self.log.warn("Derivative value for %s reset! Was squib restarted?  (value=%d, last_value=%d, max_value=%d)"
                                  % (self.name, value, self.last_value, self.max_value))
                    result = 0
            else:
                result = value - self.last_value

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
        self.start_time = time.time()

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
        m1_val = self.m1_rate.averageValue()
        if m1_val is not None:
            lines.append("%s%s.1minuteRate %2.2f %d" % (prefix, self.name, m1_val, epoch))
        m5_val = self.m5_rate.averageValue()
        if m5_val is not None:
            lines.append("%s%s.5minuteRate %2.2f %d" % (prefix, self.name, m5_val, epoch))
        m15_val = self.m15_rate.averageValue()
        if m15_val is not None:
            lines.append("%s%s.15minuteRate %2.2f %d" % (prefix, self.name, m15_val, epoch))

    def mean_rate (self):
        if self.count == 0:
            return 0.0
        else:
            return self.count / (time.time() - self.start_time)

    def save (self):
        return { 'count':self.count, 'start_time':self.start_time, 
                 'm1_rate':self.m1_rate.rate, 'm1_uncounted':self.m1_rate.uncounted,
                 'm5_rate':self.m5_rate.rate, 'm5_uncounted':self.m5_rate.uncounted,
                 'm15_rate':self.m15_rate.rate, 'm15_uncounted':self.m15_rate.uncounted, }

    def load (self, data, timestamp):
        self.count = data['count']
        self.start_time = data['start_time']

        # Only load the metrics if they will still have an effect on the current values
        now = int(time.time())
        if now - timestamp < 60:
            self.m1_rate.initialize(data['m1_rate'], data['m1_uncounted'])
        if now - timestamp < 300:
            self.m5_rate.initialize(data['m5_rate'], data['m5_uncounted'])
        if now - timestamp < 900:
            self.m15_rate.initialize(data['m15_rate'], data['m15_uncounted'])

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
