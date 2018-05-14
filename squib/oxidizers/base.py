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

import time

from mccorelib.string_conversion import convert_to_seconds, ConversionError

##############################################################################

class BasePythonOxidizer (object):

    def __init__ (self, name, config):
        super(BasePythonOxidizer, self).__init__()
        self.name = name
        self.config = config
        self.setup()

    def setup (self):
        pass

    def run (self):
        pass

##############################################################################

class PeriodicOxidizer (BasePythonOxidizer):

    default_period = 10.0 # seconds

    def setup (self):
        super(PeriodicOxidizer, self).setup()
        self.setup_period()

    def setup_period (self):
        period = self.config.get('period')
        if period is None:
            self.period = self.default_period
        else:
            try:
                self.period = convert_to_seconds(period)
            except ConversionError:
                raise ConfigError('%s::period must be a time period' % self.name)

    def run (self):
        while True:
            start = time.time()
            self.run_once()
            done = time.time()
            delay = self.period - (done - start)
            if delay > 0.0: 
                try:
                    time.sleep(delay)
                except (KeyboardInterrupt, SystemExit):
                    break

##############################################################################
## THE END
