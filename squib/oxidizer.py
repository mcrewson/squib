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

import os, traceback

from mccorelib.async     import ReadOnlyFileDescriptorReactable
from mccorelib.config    import ConfigError
from mccorelib.log       import getlog
from mccorelib.multiproc import ChildController
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
        return MetricsReader(self.metrics_recorder, fd=stdout_fd)

    def get_stderr_reactable (self, stderr_fd):
        return ErrorReporter(fd=stderr_fd)

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

    def rename_oxidizer_process (self):
        pname = 'ox:%s' % self.name
        utility.set_process_name(pname)

    def run (self):
        self.rename_oxidizer_process()
        try:
            self.oxidizer.run()
        except:
            tb = traceback.format_exc()
            os.write(2, '\'%s\' oxidizer threw an unexpected exception:\n%s' % (self.name, tb))
            
##############################################################################

class MetricsReader (ReadOnlyFileDescriptorReactable):

    def __init__ (self, metrics_recorder, **kw):
        super(MetricsReader, self).__init__(**kw)
        self.metrics_recorder = metrics_recorder
        self.buff = ''
        self.log = getlog()

    def on_data_read (self, data):
        self.buff += data
        lines, _unused, self.buff = self.buff.rpartition('\n')
        if lines:
            for line in lines.split('\n'):
                if not line: continue
                try:
                    mname, mvalue = line.split(' ', 1)
                except ValueError:
                    self.log.warning('Invalid metric: %s' % line)
                    continue
                self.metrics_recorder.record(mname, mvalue)

class ErrorReporter (ReadOnlyFileDescriptorReactable):

    def __init__ (self, **kw):
        ReadOnlyFileDescriptorReactable.__init__(self, **kw)
        self.log = getlog()

    def on_data_read (self, data):
        self.log.error(data)

##############################################################################
## THE END
