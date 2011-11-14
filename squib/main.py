#!/usr/bin/python2
# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id: main.py 8613 2011-07-15 20:33:36Z markc $
#

import os, re, signal, sys, tempfile, time

from squib.core.application       import Application
from squib.core.async             import free_reactor
from squib.core.config            import ConfigError
from squib.core.log               import get_logger
from squib.core.multiproc         import ParentController, ParentStates
from squib.core.string_conversion import convert_to_bool, ConversionError

from squib import metrics, oxidizer, reporter, statistics, utility

##############################################################################

class SquibMain (Application):

    app_name = 'squib'
    app_version = '0.0.1'

    long_cmdline_args = [ 'nodaemon', ]

    def __init__ (self, **kw):
        #self.config_files = self.centralconf_update()
        super(SquibMain, self).__init__(**kw)
        self.prelog_error_messages = []
        self.prelog_info_messages = []
        self.nodaemon = False

    def cmdline_handler (self, argument, value):
        if argument in ('--nodaemon'):
            self.nodaemon = True

    #### SETUP AND CLEANUP ################################################

    def setup (self):
        super(SquibMain, self).setup()
        self.configure_logging()
        self.configure_metrics_recorder()
        self.configure_reporter()
        self.configure_oxidizers()
        self.daemonize()
        self.write_pid()

    def cleanup (self):
        self.remove_pid()
        super(SquibMain, self).cleanup()

    def configure_logging (self):
        logfile = self.config.get('common::logfile', '%s.log' % self.app_name)
        if logfile.lower() == '__none__': 
            self.log = get_logger()
            return
        logcfg = dict(file=logfile)
        logcfg['level'] = self.config.get('common::loglevel', 'INFO')
        logcfg['console'] = self.config.get('common::console', 'True')
        logcfg['console_level'] = self.config.get('common::console_loglevel', 'WARNING')
        try:
            self.log = get_logger(config=logcfg)
        except IOError, e:
            self.abort("Cannot open log file: %s" % e)

        self.log.info("%s %s INITIALIZING" % (self.app_name, self.app_version))
        for msg in self.prelog_error_messages:
            self.log.error(msg)
        for msg in self.prelog_info_messages:
            self.log.info(msg)

    def configure_metrics_recorder (self):
        hostname = utility.calculate_hostname()
        if '.' in hostname:
            hostname = hostname.split('.', 1)[0]
        self.metrics_recorder = metrics.MetricsRecorder(prefix='%s.' % hostname)

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

    def daemonize (self):
        nodaemon = self.nodaemon
        if nodaemon == False:
            try:
                if convert_to_bool(self.config.get('common::nodaemon', False)) == False:
                    utility.daemonize()
            except ConversionError:
                raise ConfigError("nodaemon must be a boolean")

    def write_pid (self):
        pid_file = self.config.get('common::pid_file')
        if pid_file is None:
            self.pid_file = None
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
            if self.controller.state < ParentStates.RESTARTING:
                break
            free_reactor()

    def run (self):
        self.log.info("%s %s STARTED" % (self.app_name, self.app_version))
        self.controller.start()
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
