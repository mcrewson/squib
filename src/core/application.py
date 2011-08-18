# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id: application.py 6724 2010-04-15 21:01:19Z markc $
#

__all__ = [ 'Application', 'get_app', ]

import logging, sys, traceback

from core.baseobject import BaseObject, NonStdlibError
from core.config     import Config, ConfigError

#############################################################################

class ApplicationError (NonStdlibError):
    """Base exception for application errors"""
    pass

class OperationError (ApplicationError):
    """Raised when a runtime application operation fails"""
    pass

#############################################################################

_app_inst = None
_app_name = "UnamedApp"
_app_version = "0"

def get_app ():
    global _app_inst
    return _app_inst

def _except_hook (exc_type, value, tb):
    global _app_name, _app_version

    log = logging.getLogger()
    if log is not None and log.handlers:
        log.critical('Exception: please include the following information '
                     'in any bug report:')
        log.critical('  %s version %s' % (_app_name, _app_version))
        log.critical('  Python version %s' % sys.version.split('\n',1)[0])
        log.critical('')
        log.critical('Unhandled exception follows:')
        tblist = (traceback.format_tb(tb, None) +
                  traceback.format_exception_only(exc_type, value))
        if type(tblist) != list: tblist = [tblist, ]
        for line in tblist:
            for l in line.split('\n'):
                if not l: continue
                log.critical('%s' % l.rstrip())
        log.critical('')
        log.critical('Please also include configuration information from '
                     'running %s' % _app_name)
        log.critical('with your normal options plus \'--dump\'')
    else:
        traceback.print_exception(exc_type, value, tb)

class Application (BaseObject):

    app_name = "UnnamedApp"
    app_version = "0"

    short_cmdline_args = None
    long_cmdline_args = None

    options = { 'exception_hook' : _except_hook,
              }

    def __init__ (self, **kw):
        """constructor"""
        super(Application, self).__init__()
        if not kw.has_key('logger'):
            kw['logger'] = logging.getLogger()
        self._parse_options(Application.options, kw)

        if hasattr(self, 'config_files'):
            self.app_config = self.config_files
        elif hasattr(self, 'config_file'):
            self.app_config = str(self.config_file)
        else:
            self.app_config = "%s.conf" % self.app_name

        global _app_inst, _app_name, _app_version
        _app_inst = self
        _app_name = self.app_name
        _app_version = self.app_version

        if self.exception_hook is not None:
            sys.excepthook = self.exception_hook

    def start (self):
        try:
            self.config = Config(self.app_config, sys.argv,
                                 short_arguments=self.short_cmdline_args,
                                 long_arguments=self.long_cmdline_args,
                                 command_line_handler=self.cmdline_handler)

            self.setup()
            self.run()
            return

        except KeyboardInterrupt:
            self.abort('Aborted by user (keyboard interrupt)', errorcode=0)

        except ConfigError, o:
            self.abort('Configuration error: %s' % (o), errorcode=2)

        except OperationError, o:
            self.abort('Error: %s' % (o), errorcode=3)

        # otherwise, let the exception_hook handle the error (if defined)

    def cmdline_handler (self, argument, value):
        pass

    def setup (self):
        raise NotImplementedError("Override this function to do setup work")

    def run (self):
        raise NotImplementedError("Override this function to do something")

    def abort (self, message, errorcode=1):
        sys.stderr.write('ERROR: %s\n' % message)
        sys.stderr.flush()
        sys.exit(errorcode)

#############################################################################
## THE END