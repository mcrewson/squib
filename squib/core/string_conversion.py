# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id: string_conversion.py 6694 2010-04-15 16:29:49Z markc $
#

"""
Simple string conversion utilities.
"""

__all__ = [ 'ConversionError', 'convert_to_bool', 'convert_to_seconds',
            'convert_to_milliseconds', 'convert_to_minutes', 'convert_to_hours',
            'convert_to_days', 'convert_to_bytes', 'convert_to_kilobytes',
            'convert_to_megabytes', 'convert_to_gigabytes', 'convert_to_terabytes',
            'convert_to_integer', 'convert_to_octal', 'convert_to_floating',
            'convert_to_whitespace_list', 'convert_to_comma_list',
            'convert_to_list', 'convert_to_uid', 'convert_to_gid', ]

import re

from squib.core.baseobject import NonStdlibError

##############################################################################

class ConversionError(NonStdlibError):
    """Raised when one of these function fails to convert a string"""
    pass

##############################################################################

def convert_to_bool (string, allow_invalid_strings=True, invalid_string_value=False):
    if string is None or type(string) is bool: return string
    string = string.lower()
    if string in ('1', 'true', 'yes', 'on'):
        return True
    elif string in ('0', 'false', 'no', 'off'):
        return False
    if allow_invalid_strings:
        return invalid_string_value
    else:
        raise ConversionError

time_string_regex = re.compile(r'([\d.]+)\s*(ms|s|m|h|d)?')

def convert_to_milliseconds (string, default_unit='ms'):
    if string is None or type(string) in (float, int, long): return string
    string = string.lower()
    match = time_string_regex.match(string)
    if match is None:
        raise ConversionError
    tvalue = float(match.group(1))
    tunit  = match.group(2)
    if tunit is None:
        tunit = default_unit
    if tunit == 'ms':
        return tvalue
    elif tunit == 's':
        return tvalue * 1000
    elif tunit == 'm':
        return tvalue * 60000
    elif tunit == 'h':
        return tvalue * 3600000
    elif tunit == 'd':
        return tvalue * 86400000
    raise ConversionError

def convert_to_seconds (string):
    if string is None or type(string) in (float, int, long): return string
    milliseconds = convert_to_milliseconds(string, default_unit='s')
    return milliseconds / 1000

def convert_to_minutes (string):
    if string is None or type(string) in (float, int, long): return string
    milliseconds = convert_to_milliseconds(string, default_unit='m')
    return milliseconds / 60000

def convert_to_hours (string):
    if string is None or type(string) in (float, int, long): return string
    milliseconds = convert_to_milliseconds(string, default_unit='h')
    return milliseconds / 3600000

def convert_to_days (string):
    if string is None or type(string) in (float, int, long): return string
    milliseconds = convert_to_milliseconds(string, default_unit='d')
    return milliseconds / 86400000

size_string_regex = re.compile(r'([\d.]+)\s*(b|k|m|g|t)?')

def convert_to_bytes (string, default_unit='b'):
    if string is None or type(string) in (int, long): return string
    string = string.lower()
    match = size_string_regex.match(string)
    if match is None:
        raise ConversionError
    svalue = float(match.group(1))
    sunit  = match.group(2)
    if sunit is None:
        sunit = default_unit
    if sunit == 'b':
        return svalue
    elif sunit == 'k':
        return svalue * 1024
    elif sunit == 'm':
        return svalue * 1048576
    elif sunit == 'g':
        return svalue * 1073741824
    elif sunit == 't':
        return svalue * 1099511627776
    raise ConversionError

def convert_to_kilobytes (string):
    if string is None or type(string) in (int, long): return string
    bytes = convert_to_bytes(string, default_unit='k')
    return bytes / 1024

def convert_to_megabytes (string):
    if string is None or type(string) in (int, long): return string
    bytes = convert_to_bytes(string, default_unit='m')
    return bytes / 1048576

def convert_to_gigabytes (string):
    if string is None or type(string) in (int, long): return string
    bytes = convert_to_bytes(string, default_unit='g')
    return bytes / 1073741824

def convert_to_terabytes (string):
    if string is None or type(string) in (int, long): return string
    bytes = convert_to_bytes(string, default_unit='t')
    return bytes / 1099511627776

def convert_to_integer (string):
    if string is None or type(string) in (int, long): return string
    try:
        return int(string, 10)
    except ValueError:
        raise ConversionError

def convert_to_octal (string):
    if string is None or type(string) in (int, long): return string
    try:
        return int(string, 8)
    except ValueError:
        raise ConversionError

def convert_to_floating (string):
    if string is None or type(string) in (float, ): return string
    try:
        return float(string)
    except ValueError:
        raise ConversionError

def convert_to_whitespace_list (string, element_conversion=None):
    if string is None or type(string) in (list, tuple): return string
    result = [ s.strip() for s in string.split() ]
    if element_conversion is not None:
        result = [ element_conversion(e) for e in result ]
    return result

def convert_to_comma_list (string, element_conversion=None):
    if string is None or type(string) in (list, tuple): return string
    result = [ s.strip() for s in string.split(',') ]
    if element_conversion is not None:
        result = [ element_conversion(e) for e in result ]
    return result

convert_to_list = convert_to_comma_list

def convert_to_uid (string):
    if string is None or type(string) in (int, long): return string
    try:
        return int(string, 10)
    except ValueError:
        from pwd import getpwnam
        try:
            return getpwnam(string).pw_uid
        except KeyError:
            raise ConversionError('%s is not a valid user name' % string)

def convert_to_gid (string):
    if string is None or type(string) in (int, long): return string
    try:
        return int(string, 10)
    except ValueError:
        from grp import getgrname
        try:
            return grp.getgrnam(string).gr_gid
        except KeyError:
            raise ConversionError('%s is not a valid group name' % string)

##############################################################################
## THE END
