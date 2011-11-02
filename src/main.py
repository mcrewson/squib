#!/usr/bin/python2
# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id: main.py 8613 2011-07-15 20:33:36Z markc $
#

import os, re, signal, sys, tempfile, time

from core.application       import Application
from core.config            import ConfigError
from core.log               import get_logger
from core.multiproc         import ParentController
from core.string_conversion import convert_to_bool, convert_to_comma_list, \
                                   convert_to_integer, convert_to_octal, \
                                   ConversionError

import metrics
import squib
import statistics
import utility

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
        self.configure_squibs()
        self.daemonize()

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

        self.log.info("%s %s START" % (self.app_name, self.app_version))
        for msg in self.prelog_error_messages:
            self.log.error(msg)
        for msg in self.prelog_info_messages:
            self.log.info(msg)

    def configure_metrics_recorder (self):
        hostname = utility.calculate_hostname()
        if '.' in hostname:
            hostname = hostname.split('.', 1)[0]
        self.metrics_recorder = metrics.MetricsRecorder(prefix='%s.' % hostname)

    def configure_squibs (self):
        self.controller = SquibController(self.metrics_recorder)
        for sqname in self.config.read_nonconfig_section('squibs'):
            sqname = sqname.strip()
            if not sqname or sqname.startswith('#'): continue
            try:
                sqconfig = self.config.section(sqname)
            except KeyError:
                self.log.warn("No configuration for a squib named \"%s\". Ignored" % (sqname))
                continue

            try:
                self.controller.add_child(squib.create_squib(sqname, sqconfig, self.metrics_recorder))
            except ConfigError:
                self.log.warn("Invalid squib named \"%s\". Ignored" % (sqname))

    def daemonize (self):
        nodaemon = self.nodaemon
        if nodaemon == False:
            try:
                if convert_to_bool(self.config.get('common::nodaemon', False)) == False:
                    utility.daemonize()
            except ConversionError:
                raise ConfigError("nodaemon must be a boolean")

    def run (self):
        self.controller.start()
        self.log.info("%s %s STOP" % (self.app_name, self.app_version))

##############################################################################

import socket

class SquibController (ParentController):

    report_period       = 10.0 # seconds

    def __init__ (self, metrics_recorder, **kw):
        super(SquibController, self).__init__(**kw)
        self.metrics_recorder = metrics_recorder

    def setup (self):
        self.reactor.call_later(self.report_period, self.report)
        statistics.schedule_ewma_decay()

    def report (self):
        lines = self.metrics_recorder.publish()

        sock = socket.socket()
        try:
            sock.connect(('127.0.0.1', 2003))
        except:
            self.log.error("Failed to connect to graphite backend.")
            return

        message = '\n'.join(lines) + '\n'
        sock.sendall(message)
        sock.close()

        self.reactor.call_later(self.report_period, self.report)


##############################################################################

if __name__ == "__main__":
    g = SquibMain()
    g.start()

##############################################################################
## THE END
