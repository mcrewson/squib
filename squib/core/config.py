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
A configuration file parser.
"""

import getopt, os, os.path, re, sys

from squib.core.baseobject        import BaseObject, NonStdlibError
from squib.core.string_conversion import convert_to_bool, convert_to_list

##############################################################################

class ConfigError (NonStdlibError):
    """Base exception class for Config exceptions"""
    pass

class InvalidCommandlineArgument (ConfigError):
    pass

class InvalidConfigParameter (ConfigError):
    pass

##############################################################################

class Config (BaseObject):

    DEFAULT_SHORT_ARGUMENTS = 'f:hqv'

    DEFAULT_LONG_ARGUMENTS  = [ 'config=', 'option=', 'dump', 'quiet', 
                                'trace', 'verbose', 'help' ]

    DEFAULT_CONFIG_PATHS  = [ '/etc', '/usr/local/etc', '.' ]

    SECTION_REX  = re.compile(r'^\[(.*?)\]')
    KEYVALUE_REX = re.compile(r'^([^=]+)=(.*)$')
    SUBST_REX    = re.compile(r'\${([\w:]+)}')
    VARSEC_REX   = re.compile('^([a-z]+)::(.*)')


    options = { 'base_config_dict'              : None,
                'short_arguments'               : None,
                'long_arguments'                : None,
                'ignore_default_arguments'      : (False, convert_to_bool),
                'command_line_handler'          : None,
                'error_on_invalid_config_param' : (False, convert_to_bool),
                'skipped_sections'              : ([], convert_to_list),
              }

    def __init__ (self, filenames=[], commandline_args=None, **kw):
        """constructor"""
        super(Config, self).__init__(**kw)
        self._parse_options(Config.options, kw)

        if type(filenames) != list:
            filenames = [ filenames, ]
        self.filenames = filenames
        self.conf = dict()
        self.cmd_conf = dict()

        if commandline_args is not None:
            self._parse_commandline(commandline_args)

        if self.filenames:
            self._load_config_files()
        else:
            if self.base_config_dict is None:
                self.conf = dict(DEFAULT=dict())
            else:
                self.conf = dict(DEFAULT=dict(self.base_config_dict))

    def get (self, key, default=None):
        try:
            return self.__getitem__(key)
        except LookupError:
            return default

    def get_sections (self):
        sections = self.conf.keys()
        sections.sort()
        return sections

    def section (self, section):
        return self.conf[section]

    def dump_config (self, file):
        file.write('#configuration dump:\n')
        sections = self.conf.keys()
        sections.sort()
        for sec in sections:
            if type(self.conf[sec]) != dict:  continue
            file.write('[%s]\n' % sec)
            params = self.conf[sec].keys()
            params.sort()
            for param in params:
                file.write('%s = %s\n' % (param, self.conf[sec][param]))
            file.write('\n')
        file.write('#the end\n')

    def read_nonconfig_section (self, section_name, concat_all_config_files=False):
        rev_filenames = self.filenames
        rev_filenames.reverse()
        found_section = False
        section_lines = []
        for filename in rev_filenames:
            config_filename = self._find_config_file(filename)
            if config_filename is None: 
                continue
            
            fp = open(config_filename, 'r')
            lines = fp.readlines()
            fp.close()

            linecount = 0
            num_lines = len(lines)

            # find the section_name
            while linecount < num_lines:
                line = lines[linecount].strip()
                linecount += 1
                if line == '[%s]' % section_name:
                    found_section = True
                    break
            if not found_section:
                continue

            # read the lines until the next section or eof
            while linecount < num_lines:
                line = lines[linecount].strip()
                linecount += 1
                if line and line[0] == '[' and line[-1] == ']':
                    break
                section_lines.append(line)
            linecount += 1
        
            if found_section and concat_all_config_files == False:
                return section_lines

        return section_lines

    def _parse_commandline (self, args):
        sopts = ""
        lopts = list()
        if self.short_arguments:
            sopt = sopt + self.short_arguments
        if self.long_arguments:
            lopts = lopts = self.long_arguments
        if not self.ignore_default_arguments:
            sopts = sopts + Config.DEFAULT_SHORT_ARGUMENTS
            lopts.extend(Config.DEFAULT_LONG_ARGUMENTS)

        try:
            opts, args = getopt.getopt(sys.argv[1:], sopts, lopts)
        except getopt.GetoptError, o:
            raise InvalidCommandlineArgument(o)

        for o,v in opts:
            if not self.ignore_default_arguments:
                self._handle_default_commandline_argument(o, v)
            else:
                self._handle_commandline_argument(o, v)

        self.cmd_arguments = args

    def _handle_default_commandline_argument (self, option, value):
        if option in ('-h','--help'):
            usage()
        elif option in ('-f', '--config'):
            self.filenames = [ value, ]
        elif option in ('-q', '--quiet'):
            pass
        elif option in ('--trace',):
            pass
        elif option in ('-v', '--verbose'):
            pass
        elif option in ('--dump',):
            pass
        elif option in ('--option',):
            try:
                key,val = [ x.strip() for x in value.split('=', 1) ]
                if not key or not val: raise ValueError
                if key.find('::') == -1:
                    key = '::%s' % key
                self.cmd_conf[key] = val
            except ValueError:
                raise InvalidCommandlineArgument("Invalid config option on command line: %s" % value)

        else:
            self._handle_commandline_argument(option, value)

    def _handle_commandline_argument (self, option, value):
        if self.command_line_handler is None:
            return
        self.command_line_handler(option, value)

    def _load_config_files (self):
        if self.base_config_dict is None:
            self.conf = dict(DEFAULT=dict())
        else:
            self.conf = dict(DEFAULT=dict(self.base_config_dict))
        for filename in self.filenames:
            configFilename = self._find_config_file(filename)
            if configFilename is None: 
                continue

            fp = open(configFilename, 'r')
            lines = fp.readlines()
            fp.close()

            config_lines = []
            linecount = 0
            while linecount < len(lines):
                line = re.sub('#.*$', '', lines[linecount]).strip()
                if not line:
                    linecount += 1
                    continue
                while line[-1] == '\\':
                    line = line[:-1] + lines[linecount+1].strip()
                    linecount += 1
                config_lines.append(line)
                linecount += 1

            section = 'DEFAULT'
            in_skipped_section = False
            for line in config_lines:
                section_mo= Config.SECTION_REX.match(line)
                if section_mo:
                    section = section_mo.group(1)
                    if section in self.skipped_sections:
                        in_skipped_section = True
                    else:
                        in_skipped_section = False
                        self.conf[section] = dict()
                    continue

                if in_skipped_section == True:
                    continue

                if Config.KEYVALUE_REX.match(line):
                    key, value = line.split('=', 1)
                    key = key.rstrip()
                    value = value.lstrip()

                    value = os.path.expanduser(os.path.expandvars(value))

                    # Check for variable substitution
                    # This allows you to stick things like "${key}" or "${section::key}"
                    # into the config file.
                    subst = Config.SUBST_REX.search(value)
                    while subst:
                        var = subst.group(1)
                        # Work around the use of ${} for other purposes (for now)
                        varsec = Config.VARSEC_REX.match(var)
                        if varsec:
                            prefix = self.conf[varsec.group(1)][varsec.group(2)] or ""
                        else:
                            prefix = self.conf[section][var] or ""
                        # If not subst was found, check for an environment variable
                        if not prefix:
                            try:
                                prefix = os.environ[var]
                            except KeyError:
                                pass
                        value = re.sub('\${%s}' % var, prefix, value)
                        subst = Config.SUBST_REX.search(value)
                        
                    self.conf[section][key] = value

                elif self.error_on_invalid_config_param == True:
                    raise InvalidConfigParameter("Invalid config parameter: %s" % line)

                # else ignore any invalid lines

    def _find_config_file (self, filename):
        if filename.startswith("/"):
            if os.path.exists(filename):
                return filename
        else:
            for path in Config.DEFAULT_CONFIG_PATHS:
                fullname = os.path.join(path, filename)
                if os.path.exists(fullname):
                    return fullname
        return None
            

    def __getitem__  (self, key):
        if self.cmd_conf.has_key(key):
            return self.cmd_conf[key]
        else:
            if key.find('::') >= 0:
                section, key = key.split('::', 1)
                if not section:
                    section = 'DEFAULT'
            else:
                section = 'DEFAULT'
            return self.conf[section][key]

##############################################################################

if __name__ == "__main__":

    pass

##############################################################################
## THE END
