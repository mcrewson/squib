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

"""
Standard python logging extensions and configuration classes.
"""

__all__ = [ 'configure', 'getlog' ]

import errno, logging, new, os, time, types, traceback

from squib.core.baseobject        import BaseObject
from squib.core.config            import Config
from squib.core.string_conversion import convert_to_bool

try:
    import codecs
except ImportError:
    codecs = None

##############################################################################

## Logging extensions

TRACE    = (logging.NOTSET + logging.DEBUG) / 2
MOREINFO = (logging.DEBUG  + logging.INFO ) / 2

# Logger class methods to be injected

def trace_method (self, msg='trace', *args, **kwargs):
    if self.manager.disable >= TRACE:
        return
    if TRACE >= self.getEffectiveLevel():
        stack = traceback.extract_stack()[-2]
        msg = '%s [%s:%i] %s' % (stack[2] + '()',            # FUNCNAME
                                 os.path.basename(stack[0]), # FILENAME
                                 stack[1],                   # LINENO
                                 msg)
        apply(self._log, (TRACE, msg, args), kwargs)
        self.log(Logger.TRACE, msg)

def moreinfo_method (self, msg, *args, **kwargs):
    if self.manager.disable >= MOREINFO:
        return
    if MOREINFO >= self.getEffectiveLevel():
        apply(self._log, (MOREINFO, msg, args), kwargs)

def repr_method (self):
    return '<logging.Logger instance at 0x%x (name = "%s")>' % (id(self), self.name)

##############################################################################

## Logging configuration class

class LoggingFactory (BaseObject):

    DEFAULT_LEVEL   = 'INFO'
    DEFAULT_FORMAT  = '%(asctime)s %(levelname)-8s %(message)s'
    DEFAULT_DATEFMT = '%Y-%m-%d %H:%M:%S'

    DEFAULT_CONSOLE_LEVEL   = 'WARNING'
    DEFAULT_CONSOLE_FORMAT  = '%(levelname)s: %(message)s'
    DEFAULT_CONSOLE_DATEFMT = ''

    options = { 'file'           : None,
                'level'          : DEFAULT_LEVEL,
                'format'         : DEFAULT_FORMAT,
                'datefmt'        : DEFAULT_DATEFMT,
                'console'        : (False, convert_to_bool),
                'console_level'  : DEFAULT_CONSOLE_LEVEL,
                'console_format' : DEFAULT_CONSOLE_FORMAT,
                'console_datefmt' : DEFAULT_CONSOLE_DATEFMT,
                'enabled'        : (True, convert_to_bool),
              }

    def __init__ (self, **kw):
        super(LoggingFactory, self).__init__()
        self._parse_options(LoggingFactory.options, kw)
        self.is_setup = False
        self.setup()
        self.loggers = dict()

    def setup (self):
        logging.getLogger('').handlers = []

        logging.addLevelName(TRACE, 'TRACE')
        logging.addLevelName(MOREINFO, 'MOREINFO')

        if self.enabled == True:

            # setup root logger
            self.root_logger = logging.getLogger('')
            if self.file is not None:
                formatter = logging.Formatter(self.format, self.datefmt)
                handler = MyFileHandler(self.file, 'a')
                handler.setFormatter(formatter)
                self.root_logger.addHandler(handler)
            else:
                self.root_logger.addHandler(DevnullHandler())
            self.root_logger.setLevel(logging._levelNames[self.level])

            self.root_logger.trace = new.instancemethod(trace_method, self.root_logger, self.root_logger.__class__)
            self.root_logger.moreinfo = new.instancemethod(moreinfo_method, self.root_logger, self.root_logger.__class__)
            self.root_logger.__repr__ = new.instancemethod(repr_method, self.root_logger, self.root_logger.__class__)

            # setup a console logger, if enabled
            if self.console == True:
                console_fmtr = logging.Formatter(self.console_format, self.console_datefmt)
                console_hdlr = logging.StreamHandler()
                console_hdlr.setFormatter(console_fmtr)
                console_hdlr.setLevel(logging._levelNames[self.console_level])
                self.root_logger.addHandler(console_hdlr)

        self.is_setup = True

    def get_logger (self, name=None, config=None):
        if self.enabled == False:
            logger = self.devnull_logger()
        elif name is None:
            logger = self.root_logger
        elif self.loggers.has_key(name):
            logger = self.loggers[name]
        else:
            logger = self.make_logger(name, config)
            self.loggers[name] = logger

        return logger
        
    def make_logger (self, name, config):
        logger = logging.getLogger(name)
        if name is not None:
            logfile = config.get('file', None)
            if logfile is not None:
                handler = MyFileHandler(logfile, 'a')
                format = config.get('format', self.root_format)
                datefmt = config.get('datefmt', self.root_datefmt)
                formatter = logging.Formatter(format, datefmt)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            level = config.get('level', None)
            if level is not None:
                logger.setLevel(logging._levelNames[level])

        if not hasattr(logger, 'trace'):
            logger.trace = new.instancemethod(__trace_method, logger, logger.__class__)
        if not hasattr(logger, 'moreinfo'):
            logger.moreinfo = new.instancemethod(__moreinfo_method, logger, logger.__class__)
        if not hasattr(logger, '__repr__'):
            logger.__repr__ = new.instancemethod(__repr_method, logger, logger.__class__)

        return logger

    def devnull_logger (self):
        try:
            return self._devnull_logger
        except AttributeError:
            self._devnull_logger = logging.getLogger("devnull")
            self._devnull_logger.addHandler(DevnullHandler())
            self._devnull_logger.propagate = False
            return self._devnull_logger

##############################################################################

## Logging write handlers

class DevnullHandler (logging.Handler):
    
    def __init__ (self):
        logging.Handler.__init__(self)

    def handle (self, record):
        pass

class MyFileHandler (logging.StreamHandler):

    checkperiod = 10  # seconds

    def __init__ (self, filename, mode='a', encoding=None):
        if codecs is None:
            encoding = None
        self.baseFilename = os.path.abspath(filename)
        self.mode = mode
        self.encoding = encoding
        logging.StreamHandler.__init__(self, self._open())

    def close (self):
        if self.stream:
            try:
                self.flush()
            except ValueError:
                pass
            if hasattr(self.stream, 'close'):
                self.stream.close()
            logging.StreamHandler.close(self)
            self.stream = None

    def _open (self):
        if self.encoding is None:
            stream = open(self.baseFilename, self.mode)
        else:
            stream = codecs.open(self.baseFilename, self.mode, self.encoding)
        self.inode = os.stat(self.baseFilename).st_ino
        self.lastcheck = time.time()
        return stream

    def _check (self):
        now = time.time()
        if self.lastcheck + self.checkperiod >= now:
            return

        try:
            inode = os.stat(self.baseFilename).st_ino
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise
            # File was probably deleted.
            inode = None

        if inode != self.inode:
            self.close()
            self.stream = self._open()
        self.lastcheck = now

    def emit (self, record):
        self._check()
        try:
            msg = self.format(record)
            if msg.endswith('\n'):
                fs = '%s'
            else:
                fs = '%s\n'
            if not hasattr(types, "UnicodeType"):
                self.stream.write(fs % msg)
            else:
                try:
                    self.stream.write(fs % msg)
                except UnicodeError:
                    self.stream.write(fs % msg.encode("UTF-8"))
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

##############################################################################

__logging_factory = None

def configure (config):
    global __logging_factory
    if isinstance(config, Config):
        config = config.section('logging')
    __logging_factory = LoggingFactory(**config)

def getlog (name=None, config=None):
    global __logging_factory
    if __logging_factory is None:
        return logging
    return __logging_factory.get_logger(name, config)

##############################################################################
## THE END
