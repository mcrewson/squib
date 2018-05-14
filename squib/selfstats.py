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

import resource, time

from mccorelib.baseobject import BaseObject
from mccorelib.async      import get_reactor

##############################################################################

class SelfStatistics (BaseObject):

    announce_period = 3

    def __init__ (self, config, metrics_recorder):
        super(SelfStatistics, self).__init__()
        self.config = config
        self.metrics_recorder = metrics_recorder
        self.reactor = get_reactor()
        self.setup()

    def setup (self):
        self.metric_record_stat = 0
        self.metric_report_stat = 0
        
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        self.last_cpu_usage = rusage.ru_utime + rusage.ru_stime
        self.last_time  = time.time()

        self.reactor.call_later(self.announce_period, self.announce)

    def mark_metrics_record (self):
        self.metric_record_stat += 1

    def mark_metrics_report (self):
        self.metric_report_stat += 1

    def announce (self):
        try:
            self.metrics_recorder.record('squib.metrics.record', 'derivgauge %d' % self.metric_record_stat)
            self.metrics_recorder.record('squib.metrics.record', 'derivmeter %d' % self.metric_record_stat)
            self.metrics_recorder.record('squib.metrics.report', 'derivgauge %d' % self.metric_report_stat)
            self.metrics_recorder.record('squib.metrics.report', 'derivmeter %d' % self.metric_report_stat)
            self.metrics_recorder.record('squib.cpuUsage', 'gauge %2.2f' % self.get_cpu_usage())
            self.metrics_recorder.record('squib.memUsage', 'gauge %2.2f' % self.get_mem_usage())

        finally:
            self.reactor.call_later(self.announce_period, self.announce)

    def get_cpu_usage (self):
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        current_cpu_usage = rusage.ru_utime + rusage.ru_stime
        current_time   = time.time()

        usage_diff = current_cpu_usage - self.last_cpu_usage
        time_diff  = current_time - self.last_time
        if time_diff == 0: time_diff = 0.000001

        cpu_usage_percent = (usage_diff / time_diff) * 100.0

        self.last_cpu_usage = current_cpu_usage
        self.last_time = current_time

        return cpu_usage_percent

    def get_mem_usage (self):
        return 0.0

##############################################################################
## THE END
