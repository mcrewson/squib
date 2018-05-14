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

import os, socket, sys

##############################################################################

def daemonize ():
    pid = os.fork()
    if pid != 0:
        # Parent
        os._exit(0)

    # Child
    os.setsid()

##############################################################################

def calculate_hostname ():
    name = socket.gethostname()
    try:
        hostname, aliases, ipaddrs = socket.gethostbyaddr(name)
    except socket.error:
        pass
    else:
        aliases.insert(0, hostname)
        for name in aliases:
            if '.' and name and not name.startswith('localhost'):
                break
        else:
            name = hostname
    return name

##############################################################################

def find_python_object (name, module=None):
    if '.' not in name:
        return getattr(module, name)
    else:
        try:
            exec 'import ' + name
        except ImportError:
            try:
                module = '.'.join(name.split('.')[:-1])
                exec 'import ' + module
            except (ImportError, AttributeError), err:
                raise ImportError(err)
        return eval(name)

##############################################################################

_ctypes, _ctypes_util = None, None
try:
    import ctypes
    _ctypes = ctypes
    import ctypes.util
    _ctypes_util = ctypes.util
except ImportError:
    pass

def set_process_name (name, argv=None):
    if _ctypes is None or _ctypes_util is None: return
    try:
        if argv is None:
            argv = name
        _set_process_argv(argv)
        _set_process_prctl(name)
    except:
        # Silently ignore any errors
        pass

_current_process_name, _max_process_name_length, _libc, _argc_t, _py_getargcargv = None, None, None, None, None

def _set_process_argv (name):
    global _argc_t, _py_getargcargv
    global _current_process_name, _max_process_name_length
    if _argc_t is None:
        _argc_t = _ctypes.POINTER(_ctypes.c_char_p)
    if _py_getargcargv is None:
        _py_getargcargv = _ctypes.pythonapi.Py_GetArgcArgv
        _py_getargcargv.restype = None
        _py_getargcargv.argtypes = [ _ctypes.POINTER(ctypes.c_int),
                                     _ctypes.POINTER(_argc_t) ]

    argv = _ctypes.c_int(0)
    argc = _argc_t()
    _py_getargcargv(argv, _ctypes.pointer(argc))

    if _current_process_name is None:
        args = []
        for i in range(100):
            if argc[i] == None: break
            args.append(str(argc[i]))
        _current_process_name = " ".join(args)
        _max_process_name_length = len(_current_process_name)

    if len(name) > _max_process_name_length:
        name = name[:_max_process_name_length]

    zerosz = max(len(_current_process_name), len(name))
    _ctypes.memset(argc.contents, 0, zerosz + 1)
    _ctypes.memmove(argc.contents, name, len(name))
    _current_process_name = name

def _set_process_prctl (name):
    global _libc
    if _libc is None:
        _libc = _ctypes.CDLL(_ctypes_util.find_library('c'))

    if len(name) > 15:
        name = name[:15]
    buff = _ctypes.create_string_buffer(len(name) + 1)
    buff.value = name
    _libc.prctl(15, _ctypes.byref(buff), 0, 0, 0)

##############################################################################
## THE END
