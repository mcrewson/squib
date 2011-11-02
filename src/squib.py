# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#

import sys

from core.async     import ReadOnlyFileDescriptorReactable
from core.config    import ConfigError
from core.log       import get_logger
from core.multiproc import ChildController

##############################################################################

def create_squib (name, config, metrics_recorder):
    if config.has_key("class"):
        return PythonSquib(name, config, metrics_recorder)
    else:
        raise ConfigError("Unknown type of squib: %s" % name)

##############################################################################

class BaseSquib (ChildController):

    def __init__ (self, name, config, metrics_recorder):
        super(BaseSquib, self).__init__(name)
        self.config = config
        self.metrics_recorder = metrics_recorder

    def get_stdout_reactable (self, stdout_fd):
        return MetricsReader(self.metrics_recorder, stdout_fd)

    def get_stderr_reactable (self, stderr_fd):
        return ErrorReporter(stderr_fd)

##############################################################################

class PythonSquib (BaseSquib):

    def get_squib_module (self, fqclass):
        __import__(fqclass)
        return sys.modules[fqclass]

    def run (self):
        klass = self.config.get("class")
        if klass is None:
            raise ConfigError("No class defined for this squib")

        module = self.get_squib_module(klass)
        run_method = getattr(module, 'run')
        run_method(10.0)


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
