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

import heapq, math, random, sys, time

from mccorelib.async import get_reactor

##############################################################################

EWMA_DECAY_INTERVAL = 10.0 # seconds

M1_ALPHA  = 1 - math.exp(-EWMA_DECAY_INTERVAL / 60.0)
M5_ALPHA  = 1 - math.exp(-EWMA_DECAY_INTERVAL / 60.0 / 5)
M15_ALPHA = 1 - math.exp(-EWMA_DECAY_INTERVAL / 60.0 / 15)

all_ewmas = []


class ExponentiallyWeightedMovingAverage (object):

    def __init__ (self, alpha, interval):
        self.alpha = alpha
        self.interval = interval
        self.rate = 0.0
        self.uncounted = 0
        self.initialized = False

        global all_ewmas
        all_ewmas.append(self)

    def initialize (self, rate, uncounted):
        self.rate = rate
        self.uncounted = uncounted
        self.initialized = True

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

class Sample (object):

    def __init__ (self, reservoirSize):
        self.reservoirSize = reservoirSize
        self.count = 0

    def update (self, value):
        pass

    def values (self):
        pass

    def percentiles (self, *percentiles):
        scores = [ 0.0 for p in percentiles ]
        if self.count > 0:
            values = self.values()
            values.sort()
            for i in range(len(percentiles)):
                p = percentiles[i]
                pos = p * (len(values) + 1)
                if pos < 1:
                    scores[i] = values[0]
                elif pos >= len(values):
                    scores[i] = values[-1]
                else:
                    lower = values[int(pos) - 1]
                    upper = values[int(pos)]
                    scores[i] = lower + (pos - math.floor(pos)) * (upper - lower)
        return scores

class UniformSample (Sample):

    def __init__ (self, reservoirSize):
        super(UniformSample, self).__init__(reservoirSize)
        self.reservoir = [0] * self.reservoirSize

    def update (self, value):
        self.count += 1
        if self.count <= self.reservoirSize:
            self.reservoir[self.count - 1] = value
        else:
            r = random.randint(0, self.count -1)
            if r < self.reservoirSize:
                self.reservoir[r] = value

    def values (self):
        if self.count > self.reservoirSize:
            return self.reservoir[:]
        else:
            return self.reservoir[:self.count]

class ExponentiallyDecayingSample (Sample):

    rescale_threshold = 60 * 60 # 1 hour

    def __init__ (self, reservoirSize, alpha):
        super(ExponentiallyDecayingSample, self).__init__(reservoirSize)
        self.alpha = alpha
        self.reservoir = []
        self.starttime = int(time.time())
        self.next_rescale_time = self.starttime + ExponentiallyDecayingSample.rescale_threshold

    def update (self, value):
        now = int(time.time())
        priority = math.exp(self.alpha * (now - self.starttime)) / random.random()
        val = (priority, value)
        self.count += 1
        if self.count <= self.reservoirSize:
            self.reservoir.append(val)
        else:
            first = self.reservoir[0][0]
            if first < priority:
                self.reservoir.append(val)
                del self.reservoir[0]
        self.reservoir.sort()

        if now > self.next_rescale_time:
            self.rescale(now)

    def rescale (self, now):
        self.next_rescale_time = now + ExponentiallyDecayingSample.rescale_threshold
        new_reservoir = []
        old_starttime = self.starttime
        self.starttime = now
        for priority,value in self.reservoir:
            new_reservoir.append((priority * math.exp(-self.alpha * (self.starttime - old_starttime)), value))
        self.reservoir = new_reservoir

    def values (self):
        if self.count > self.reservoirSize:
            return [ v for p,v in self.reservoir ]
        else:
            return [ v for p,v in self.reservoir[:self.count] ]

    def dump (self):
        print "count = %d, reservoirSize = %d" % (self.count, self.reservoirSize)
        if self.count > self.reservoirSize:
            for p,v in self.reservoir:
                print "%2.2f ... %s" % (p, v)
        else:
            for p,v in self.reservoir[:self.count]:
                print "%2.2f ... %s" % (p, v)


def one_minute_eds ():
    return ExponentiallyDecayingSample(1028, M1_ALPHA)

def five_minute_eds ():
    return ExponentiallyDecayingSample(1028, M5_ALPHA)

def fifteen_minute_eds ():
    return ExponentiallyDecayingSample(1028, M15_ALPHA)

##############################################################################
## THE END
