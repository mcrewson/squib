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

import socket

from squib.core.async             import SocketReactable
from squib.core.baseobject        import BaseObject
from squib.core.config            import ConfigError
from squib.core.log               import get_logger
from squib.core.string_conversion import convert_to_integer, convert_to_seconds, ConversionError

##############################################################################

class BaseReporter (BaseObject):

    default_report_period = 10.0 # seconds

    def __init__ (self, reporter_config, metrics_recorder, **kw):
        super(BaseReporter, self).__init__(**kw)
        #self._parse_options(BaseReporter.options, kw)
        self.reporter_config = reporter_config
        self.metrics_recorder = metrics_recorder
        self.log = get_logger()
        self.setup()

    def setup (self):
        self.setup_report_period()

    def setup_report_period (self):
        if self.reporter_config is None:
            self.report_period = BaseReporter.default_report_period
            return

        report_period = self.reporter_config.get('period')
        if report_period is None:
            self.report_period = BaseReporter.default_report_period
            return

        try:
            self.report_period = convert_to_seconds(report_period)
        except ConversionError:
            raise ConfigError('reporter::period must be a floating point number')

    def get_report_period (self):
        return self.report_period

    def send_report (self):
        pass

##############################################################################

class SimpleLogReporter (BaseReporter):

    def send_report (self):
        lines = self.metrics_recorder.publish()
        for line in lines:
            self.log.info("REPORT: %s" % line)

##############################################################################

class GraphiteReporter (BaseReporter):

    default_graphite_server = 'localhost'
    default_graphite_port   = 2003

    def setup (self):
        super(GraphiteReporter, self).setup()
        self.setup_graphite()

    def setup_graphite (self):
        graphite_server = self.reporter_config.get('graphite_server')
        if graphite_server is None:
            self.log.warning('no graphite_server specified for GraphiteReporter. Using default: %s' 
                             % GraphiteReporter.default_graphite_server)
            self.graphite_server = GraphiteReporter.default_graphite_server
        else:
            self.graphite_server = graphite_server

        graphite_port = self.reporter_config.get('graphite_port')
        if graphite_port is None:
            self.log.debug('no graphite_port specified for GraphiteReporter. Using default: %s'
                           % GraphiteReporter.default_graphite_port)
            self.graphite_port = GraphiteReporter.default_graphite_port
        else:
            try:
                self.graphite_port = convert_to_integer(graphite_port)
            except ConversionError:
                raise ConfigError('reporter::graphite_port mus be an integer number')

        self.log.info('Reporting to graphite server: %s:%s' % (self.graphite_server,
                                                               self.graphite_port))

    def send_report (self):
        lines = self.metrics_recorder.publish()
        message = '\n'.join(lines) + '\n'

        try:
            sock = SocketReactable(addr=(self.graphite_server, self.graphite_port))
            sock.create_socket()
            sock.write_data(message)
            sock.close_when_done()
        except socket.error, why:
            self.log.warn('Failed to report to graphite: %s' % str(why))

##############################################################################
## THE END
