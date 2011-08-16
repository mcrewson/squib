# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id: baseobject.py 6074 2009-10-21 19:01:48Z markc $
#

"""
Base object class for many classes. Provides standard class configuration
and logging utility methods often needed.
"""

__all__ = [ "BaseObject", ]

##############################################################################

class NonStdlibError (Exception):
    """Base for Exceptions NOT from the python stdlib"""
    pass

class BaseObjectError (NonStdlibError):
    """Top level exception for errors to do with the BaseObject"""
    pass

class InvalidOptions (BaseObjectError):
    """Invalid option definitions in the class"""
    pass

class InvalidParameter (BaseObjectError):
    """Invalid option passed to the constructor of the class"""
    pass

##############################################################################

class BaseObject (object):

    options = { 'logger'       : None,
                'logger_level' : 'debug',
              }

    def __init__ (self, **kw):
        super(BaseObject, self).__init__()
        self._parse_options(BaseObject.options, kw)

    def _parse_options (self, options, kw):
        for key,v in options.items():
            conversion = None
            if type(v) in (list, tuple):
                try:
                    default_value = v[0]
                    conversion    = v[1]
                except LookupError:
                    raise InvalidOptions("Invalid option: %s.%s" % (self.__class__, key))
            else:
                default_value = v

            value = kw.get(key, default_value)
            if conversion is not None:
                try:
                    value = conversion(value)
                except Exception, e:
                    raise InvalidParameter("The parameter '%s.%s=%s' cannot be type converted: %s" % (self.__class__, key, value, e))

            self.__dict__[key] = value

    def _log (self, message):
        if self.logger is None: return

        if hasattr(self, '__logger_log'):
            self.__logger_log(message)
        elif hasattr(self, '__logger_write'):
            self.__logger_write(message + '\n')
        else:
            if hasattr(self.logger, self.logger_level):
                self.__logger_log = eval('self.logger.%s' % self.logger_level)
                self.__logger_log(message)
            elif hasattr(self.logger, 'write'):
                self.__logger_write = self.logger.write
                self.__loger_write(message + '\n')
            else:
                # Logger is unrecognizable. Ignore it
                self.logger = None

##############################################################################
## THE END
