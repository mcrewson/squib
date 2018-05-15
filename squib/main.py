#!/usr/bin/python2
# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# Copyright 2011-2018 Mark Crewson <mark@crewson.net>
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

import os, re, signal, sys, tempfile, time

from mccorelib.application       import Application
from mccorelib.async             import free_reactor
from mccorelib.config            import Config, ConfigError
from mccorelib.log               import getlog
from mccorelib.multiproc         import ParentController, ParentStates
from mccorelib.string_conversion import convert_to_bool, ConversionError

from squib import metrics, oxidizer, reporter, selfstats, statistics, utility

##############################################################################

class SquibMain (Application):

    app_name = 'squib'
    app_version = '0.1.0'

    long_cmdline_args = [ 'nodaemon', ]

    def __init__ (self, **kw):
        super(SquibMain, self).__init__(**kw)
        self.nodaemon = False
        self.daemonized = False
        self.pid_file = None

    def cmdline_handler (self, argument, value):
        if argument in ('--nodaemon'):
            self.nodaemon = True

    #### SETUP AND CLEANUP ################################################

    def setup (self):
        super(SquibMain, self).setup()
        self.log = getlog()
        self.rename_process()
        self.configure_metrics_recorder()
        self.configure_reporter()
        self.configure_oxidizers()
        self.configure_extra_oxidizers()
        self.configure_selfstats()
        self.daemonize()
        self.write_pid()

    def cleanup (self):
        self.remove_pid()
        super(SquibMain, self).cleanup()

    def rename_process (self):
        utility.set_process_name(self.app_name)

    def configure_metrics_recorder (self):
        hostname = utility.calculate_hostname()
        if '.' in hostname:
            hostname = hostname.split('.', 1)[0]
        save_file = self.config.get('common::metrics_save_file', None)
        self.metrics_recorder = metrics.MetricsRecorder(prefix='%s.' % hostname,
                                                        save_file=save_file)

    def configure_reporter (self):
        try:
            reporter_config = self.config.section('reporter')
        except KeyError:
            self.log.warning('No reporter defined. Falling back to SimpleLogReporter.')
            self.reporter = reporter.SimpleLogReporter(None, self.metrics_recorder)
        else:
            reporter_klass = reporter_config.get('class')
            if reporter_klass is None:
                self.log.warning('No report class defined. Falling back to SimpleLogReporter.')
                self.reporter = reporter.SimpleLogReporter(None, self.metrics_recorder)
            else:
                klass = utility.find_python_object(reporter_klass)
                self.reporter = klass(reporter_config, self.metrics_recorder)

    def configure_oxidizers (self):
        self.controller = SquibController(self.reporter)
        for ox in self.config.read_nonconfig_section('oxidizers'):
            ox = ox.strip()
            if not ox or ox.startswith('#'): continue
            try:
                oxconfig = self.config.section(ox)
            except KeyError:
                self.log.warn("No configuration for an oxidizer named \"%s\". Ignored" % (ox))
                continue

            try:
                self.controller.add_child(oxidizer.create_oxidizer(ox, oxconfig, self.metrics_recorder))
            except ConfigError, err:
                self.log.warn(str(err))
                self.log.warn("Invalid oxidizer named \"%s\". Ignored" % (ox))

    def configure_extra_oxidizers (self):
        oxconfig_dir = self.config.get('common::oxidizers_config_directory', None)
        if oxconfig_dir is None:
            self.log.debug('No common::oxidizers_config_directory defined. Skipping extra oxidizers.')
            return
        if not os.path.isdir(oxconfig_dir):
            self.log.warn('No directory, skipping extra oxidizers: %s' % oxconfig_dir)
            return

        for oxfile in os.listdir(oxconfig_dir):
            oxname = os.path.splitext(oxfile)[0]
            oxfile = os.path.join(oxconfig_dir, oxfile)
            if not os.path.isfile(oxfile):
                self.log.warn('Not an oxidizer config file, skipping: %s' % oxfile)
                continue
            self.log.debug('Reading extra oxidizer config file: %s' % oxfile)

            try:
                try:
                    oxconfig = Config(oxfile).section('oxidizer')
                except KeyError:
                    self.log.warn("Invalid configuration file %s: no [oxidizer] section" % oxfile)
                    continue

                self.controller.add_child(oxidizer.create_oxidizer(oxname, oxconfig, self.metrics_recorder))
            except ConfigError, err:
                self.log.warn(str(err))
                self.log.warn("Invalid oxidizer named \"%s\" (from file: %s). Ingored" % (oxname, oxfile))


    def configure_selfstats (self):
        try:
            if convert_to_bool(self.config.get('common::selfstats', True)) == True:
                self.selfstats = selfstats.SelfStatistics(self.config, self.metrics_recorder)
                self.metrics_recorder.set_selfstats(self.selfstats)
        except ConversionError:
            raise ConfigError("noselfstats must be a boolean")

    def daemonize (self):
        nodaemon = self.nodaemon
        if nodaemon == False:
            try:
                if convert_to_bool(self.config.get('common::nodaemon', False)) == False:
                    if self.daemonized == False:
                        utility.daemonize()
                        self.daemonized = True
            except ConversionError:
                raise ConfigError("nodaemon must be a boolean")

    def write_pid (self):
        pid_file = self.config.get('common::pid_file')
        if pid_file is None:
            return

        self.pid_file = os.path.abspath(pid_file)
        try:
            self.log.debug("Writing pid file: %s" % self.pid_file)
            f = open(self.pid_file, 'w')
            f.write('%d\n' % os.getpid())
            f.close()
        except (IOError, OSError), why:
            self.log.error("Cannot write pid file: %s" % str(why))
            raise ConfigError("Cannot write pid file: %s" % self.pid_file)

    def remove_pid (self):
        if self.pid_file is not None:
            try:
                self.log.debug("Removing pid file: %s" % self.pid_file)
                os.unlink(self.pid_file)
            except OSError, why:
                self.log.debug("Cannot remove pid file: %s" % str(why))

    #### PROCESS CONTROL #####################################################

    def start (self):
        while 1:
            super(SquibMain, self).start()
            if self.controller.should_shutdown():
                break
            free_reactor()

    def run (self):
        self.log.info("%s %s STARTED" % (self.app_name, self.app_version))
        self.controller.start()
        self.metrics_recorder.save()
        self.log.info("%s %s STOPPED" % (self.app_name, self.app_version))

##############################################################################

import socket

class SquibController (ParentController):
    """
    The squib main loop controller. This object manages the oxidizer children and
    triggers the reporter.
    """

    def __init__ (self, reporter, **kw):
        super(SquibController, self).__init__(**kw)
        self.reporter = reporter
        self.report_period = self.reporter.get_report_period()

    def setup (self):
        self.reactor.call_later(self.report_period, self.report)
        statistics.schedule_ewma_decay()

    def report (self):
        try:
            self.reporter.send_report()
        finally:
            self.reactor.call_later(self.report_period, self.report)


##############################################################################

if __name__ == "__main__":
    g = SquibMain()
    g.start()

##############################################################################
## THE END
