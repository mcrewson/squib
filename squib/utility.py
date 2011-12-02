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
    os.close(0)
    sys.stdin = sys.__stdin__ = open("/dev/null")
    os.close(1)
    sys.stdout = sys.__stdout__ = open("/dev/null", "w")
    os.close(2)
    sys.stderr = sys.__stderr__ = open("/dev/null", "w")
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
## THE END
