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

from squib.core.async     import ReadOnlyFileDescriptorReactable
from squib.core.config    import ConfigError
from squib.core.log       import get_logger
from squib.core.multiproc import ChildController
from squib.oxidizers.base import BasePythonOxidizer

from squib import utility

##############################################################################

def create_oxidizer (name, config, metrics_recorder):
    if config.has_key("class"):
        return PythonOxidizer(name, config, metrics_recorder)
    else:
        raise ConfigError("Unknown type of oxidizer: %s" % name)

##############################################################################

class BaseOxidizer (ChildController):

    def __init__ (self, name, config, metrics_recorder):
        super(BaseOxidizer, self).__init__(name)
        self.config = config
        self.metrics_recorder = metrics_recorder
        self.setup()

    def setup (self):
        pass

    def get_stdout_reactable (self, stdout_fd):
        return MetricsReader(self.metrics_recorder, stdout_fd)

    def get_stderr_reactable (self, stderr_fd):
        return ErrorReporter(stderr_fd)

##############################################################################

class PythonOxidizer (BaseOxidizer):

    def setup (self):

        class OxidizerCallableWrapper (object):
            def __init__ (inself, call, conf):
                inself.call = call
                inself.conf = conf
            def run (inself):
                inself.call(inself.conf)

        klass = self.config.get("class")
        if klass is None:
            raise ConfigError("No class defined for this oxidizer")

        try:
            obj = utility.find_python_object(klass)
        except ImportError, err:
            raise ConfigError("Cannot find the oxidizer object: %s" % klass)

        # How do we invoke this object?
        if type(obj) is type and issubclass(obj, BasePythonOxidizer):
            # A BasePythonOxidizer class!
            # Instantiate it and invoke
            self.oxidizer = obj(self.name, self.config)

        elif type(obj) is type(utility):
            # Python module. If it has a run() function, we'll invoke that
            try:
                run_function = getattr(obj, 'run')
            except AttributeError:
                raise ConfigError("Cannot invoke an oxidizer module without a 'run' function: %s" % klass)
            self.oxidizer = OxidizerCallableWrapper(run_function, self.config)

        elif callable(obj):
            # Some other callable object. Try to invoke it directly
            self.oxidizer = OxidizerCallableWrapper(obj, self.config)

        else:
            # Give up
            raise ConfigError("Cannot determine how to invoke this oxidizer: %s" % klass)

    def run (self):
        self.oxidizer.run()
            
##############################################################################

class MetricsReader (ReadOnlyFileDescriptorReactable):

    def __init__ (self, metrics_recorder, fd=None, reactor=None):
        ReadOnlyFileDescriptorReactable.__init__(self, fd, reactor)
        self.metrics_recorder = metrics_recorder

    def on_data_read (self, data):
        for line in data.split('\n'):
            if not line: continue
            mname, mvalue = line.split(' ', 1)
            self.metrics_recorder.record(mname, mvalue)

class ErrorReporter (ReadOnlyFileDescriptorReactable):

    def __init__ (self, fd=None, reactor=None):
        ReadOnlyFileDescriptorReactable.__init__(self, fd, reactor)
        self.log = get_logger()

    def on_data_read (self, data):
        self.log.error(data)

##############################################################################
## THE END
