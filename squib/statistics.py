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

import math

from squib.core.async import get_reactor

##############################################################################

EWMA_DECAY_INTERVAL = 5.0 # seconds

M1_ALPHA  = 1 - math.exp(-5 / 60.0)
M5_ALPHA  = 1 - math.exp(-5 / 60.0 / 5)
M15_ALPHA = 1 - math.exp(-5 / 60.0 / 15)

all_ewmas = []


class ExponentiallyWeightedMovingAverage (object):

    def __init__ (self, alpha, interval):
        self.alpha = alpha
        self.interval = interval
        self.rate = 0.0
        self.initialized = False
        self.uncounted = 0

        global all_ewmas
        all_ewmas.append(self)

    def update (self, n):
        self.uncounted += n

    def decay (self):
        count, self.uncounted = self.uncounted, 0
        instant_rate = count / self.interval
        if self.initialized:
            self.rate += (self.alpha * (instant_rate - self.rate))
        else:
            self.rate = instant_rate
            self.initialized = True

    def averageValue (self):
        return self.rate


def one_minute_ewma ():
    return ExponentiallyWeightedMovingAverage(M1_ALPHA, EWMA_DECAY_INTERVAL)

def five_minute_ewma ():
    return ExponentiallyWeightedMovingAverage(M5_ALPHA, EWMA_DECAY_INTERVAL)

def fifteen_minute_ewma ():
    return ExponentiallyWeightedMovingAverage(M15_ALPHA, EWMA_DECAY_INTERVAL)

##############################################################################

def ewma_decay ():
    global all_ewmas
    for ewma in all_ewmas:
        ewma.decay()
    schedule_ewma_decay()

def schedule_ewma_decay ():
    get_reactor().call_later(EWMA_DECAY_INTERVAL, ewma_decay)

##############################################################################
## THE END
