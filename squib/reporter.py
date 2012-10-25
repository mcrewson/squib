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

from squib.core.asyncnet          import TCPReactable, MulticastReactable
from squib.core.baseobject        import BaseObject
from squib.core.config            import ConfigError
from squib.core.log               import getlog
from squib.core.string_conversion import convert_to_bool, convert_to_integer, convert_to_seconds, ConversionError

##############################################################################

class BaseReporter (BaseObject):

    default_report_period = 10.0 # seconds

    def __init__ (self, reporter_config, metrics_recorder, **kw):
        super(BaseReporter, self).__init__(**kw)
        #self._parse_options(BaseReporter.options, kw)
        self.reporter_config = reporter_config
        self.metrics_recorder = metrics_recorder
        self.log = getlog()
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

class TcpReporter (BaseReporter):

    default_destination_addr = None
    default_destination_port = None

    def setup (self):
        super(TcpReporter, self).setup()
        self.setup_address()

    def setup_address (self):
        destination_addr = self.reporter_config.get('destination_addr')
        if destination_addr is None:
            if self.default_destination_addr is not None:
                self.log.warning('no destination_addr specified for TcpReporter. Using default: %s'
                                 % self.default_destination_addr)
                self.destination_addr = self.default_destination_addr
            else:
                raise ConfigError('reporter::destination_addr must be specified')
        else:
            self.destination_addr = destination_addr

        destination_port = self.reporter_config.get('destination_port')
        if destination_port is None:
            if self.default_destination_port is not None:
                self.log.warning('no destination_port specified for TcpReporter. Using default: %s'
                                 % self.default_destination_port)
                self.destination_port = self.default_destination_port
            else:
                raise ConfigError('reporter::destination_port must be specified')
        else:
            try:
                self.destination_port = convert_to_integer(destination_port)
            except ConversionError:
                raise ConfigError('reporter::destination_port must be an integer number')

        self.log.info('Reporting to tcp address: %s:%s' % (self.destination_addr,
                                                           self.destination_port))

    def send_report (self):
        lines = self.metrics_recorder.publish()
        message = '\n'.join(lines) + '\n'
        try:
            sock = TCPReactable(address=(self.destination_addr, self.destination_port))
            sock.create_socket()
            sock.write_data(message)
            sock.close_when_done()
        except socket.error, why:
            self.log.warn('Failed to send report: %s' % str(why))

class GraphiteReporter (TcpReporter):
    """Do not break old configurations..."""

    default_graphite_server = 'localhost'
    default_graphite_port   = 2003

##############################################################################

class MulticastReporter (BaseReporter):

    def setup (self):
        super(MulticastReporter, self).setup()
        self.setup_address()

    def setup_address (self):
        self.multicast_addr = self.reporter_config.get('multicast_addr')
        if not self.multicast_addr:
            raise ConfigError('reporter::multicast_addr must be specified')

        self.multicast_port = self.reporter_config.get('multicast_port')
        if not self.multicast_port:
            raise ConfigError('reporter::multicast_port must be specified')
        else:
            try:
                self.multicast_port = convert_to_integer(self.multicast_port)
            except ConversionError:
                raise ConfigError('reporter::multicast_port must be an integer number')

        self.multicast_ttl = self.reporter_config.get('multicast_ttl')
        if self.multicast_ttl:
            try:
                self.multicast_ttl = convert_to_integer(self.multicast_ttl)
            except ConversionError:
                raise ConfigError('reporter::multicast_ttl must be an integer number')

        self.multicast_loopback = self.reporter_config.get('multicast_loopback', True)
        try:
            self.multicast_loopback = convert_to_bool(self.multicast_loopback)
        except ConversionError:
            raise ConfigError('reporter::multicast_loopback must be a boolean')


        self.log.info('Reporting to multicast address:  %s:%s' % (self.multicast_addr,
                                                                  self.multicast_port))
        if self.multicast_ttl is not None:
            self.log.info('MulticastReporter will send reports beyond this network (multicast_ttl = %d)' % self.multicast_ttl)
        if self.multicast_loopback == False:
            self.log.info('MulticastReporter will NOT send reports to this machine (multicast_loopback = False)')

    def send_report (self):
        lines = self.metrics_recorder.publish()
        message = '\n'.join(lines) + '\n'
        try:
            address=(self.multicast_addr, self.multicast_port)
            sock = MulticastReactable(address)
            sock.create_socket()
            if self.multicast_ttl:
                sock.set_ttl(self.multicast_ttl)
            sock.set_loopback_mode(self.multicast_loopback)
            sock.write_data(message)
            sock.close_when_done()
        except socket.error, why:
            self.log.warn('Failed to send report: %s' % str(why))

##############################################################################
## THE END
