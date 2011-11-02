# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id: log.py 6694 2010-04-15 16:29:49Z markc $
#

"""
Standard python logging extensions and configuration classes.
"""

__all__ = [ 'LoggingFactory', 'get_logger', ]

import logging, new, types, traceback

from squib.core.baseobject        import BaseObject
from squib.core.string_conversion import convert_to_bool

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

    def __init__ (self, config=None, **kw):
        super(LoggingFactory, self).__init__()
        self._parse_options(LoggingFactory.options, kw)
        if config is not None:
            self.file           = config.get('file', None)
            self.level          = config.get('level', self.DEFAULT_LEVEL)
            self.format         = config.get('format', self.DEFAULT_FORMAT)
            self.datefmt        = config.get('datefmt', self.DEFAULT_DATEFMT)
            self.console        = convert_to_bool(config.get('console', False))
            self.console_level  = config.get('console_level', self.DEFAULT_CONSOLE_LEVEL)
            self.console_format = config.get('console_format', self.DEFAULT_CONSOLE_FORMAT)
            self.console_datefmt = config.get('console_datefmt', self.DEFAULT_CONSOLE_DATEFMT)
            self.enabled        = config.get('enabled', True)

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
                handler = MultilineFileHandler(self.file, 'a')
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
                handler = MultilineFileHandler(logfile, 'a')
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

class MultilineFileHandler (logging.FileHandler):

    def __init__ (self, filename, mode='a', encoding=None):
        try:
            logging.FileHandler.__init__(self, filename, mode, encoding)
        except TypeError:
            # support python2.3
            logging.FileHandler.__init__(self, filename, mode)

    def emit (self, record):
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

def get_logger (name=None, config=None, **kw):
    global __logging_factory
    if config is not None:
        __logging_factory = LoggingFactory(config, **kw)
    elif __logging_factory is None:
        __logging_factory = LoggingFactory(**kw)
    return __logging_factory.get_logger(name)

##############################################################################
## THE END
