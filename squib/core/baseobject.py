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
Base object class for many classes. Provides standard class configuration
utility methods often needed.
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

    options = {}

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

##############################################################################
## THE END
