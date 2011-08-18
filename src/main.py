#!/usr/bin/python2
# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id: main.py 8613 2011-07-15 20:33:36Z markc $
#

import os, re, signal, sys, tempfile, time

from core.application       import Application
from core.async             import get_reactor
from core.config            import ConfigError
from core.log               import get_logger
from core.string_conversion import convert_to_bool, convert_to_comma_list, \
                                   convert_to_integer, convert_to_octal, \
                                   ConversionError

import metrics
import squib
import statistics
import utility

##############################################################################

class AppStates:
    FATAL      = 2
    RUNNING    = 1
    RESTARTING = 0
    SHUTDOWN   = -1

def app_state_description (code):
    for stname in AppStates.__dict__:
        if getattr(AppStates, stname) == code:
            return stname

##############################################################################

class SquibMain (Application):

    app_name = 'squib'
    app_version = '0.0.1'

    long_cmdline_args = [ 'nodaemon', ]

    housekeeping_period =  0.5 # seconds
    report_period       = 10.0 # seconds

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
        self.configure_logging()
        self.configure_metrics_recorder()
        self.configure_squibs()
        self.configure_signals()
        self.daemonize()

        self.reactor = get_reactor()

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
        self.squibs = []
        for sqname in self.config.read_nonconfig_section('squibs'):
            sqname = sqname.strip()
            if not sqname or sqname.startswith('#'): continue
            try:
                sqconfig = self.config.section(sqname)
            except KeyError:
                self.log.warn("No configuration for a squib named \"%s\". Ignored" % (sqname))
                continue

            try:
                self.squibs.append(squib.create_squib(sqname, sqconfig, self.metrics_recorder))
            except ConfigError:
                self.log.warn("Invalid squib named \"%s\". Ignored" % (sqname))

        self.squibs.sort()
        self.log.debug("squibs = %s" % self.squibs)

    def configure_signals (self):
        self.signal = None
        signal.signal(signal.SIGTERM, self.sig_receiver)
        signal.signal(signal.SIGINT,  self.sig_receiver)
        signal.signal(signal.SIGQUIT, self.sig_receiver)
        signal.signal(signal.SIGHUP,  self.sig_receiver)
        signal.signal(signal.SIGCHLD, self.sig_receiver)
        signal.signal(signal.SIGUSR2, self.sig_receiver)

    def sig_receiver (self, sig, frame):
        self.signal = sig

    def daemonize (self):
        nodaemon = self.nodaemon
        if nodaemon == False:
            try:
                if convert_to_bool(self.config.get('common::nodaemon', False)) == False:
                    utility.daemonize()
            except ConversionError:
                raise ConfigError("nodaemon must be a boolean")

    #### MAIN LOOP ####

    def run (self):
        try:
            self.lastshutdownreport = 0
            self.state = AppStates.RUNNING
            self.stopping_squibs = None
            self.stopping = False

            self.reactor.call_later(self.housekeeping_period, self.housekeeping)
            self.reactor.call_later(self.report_period, self.report)
            statistics.schedule_ewma_decay()
            self.reactor.start()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.reactor.stop()
            exc_type, value, tb = sys.exc_info()
            if self.exception_hook is not None:
                self.exception_hook(exc_type, value, tb)
            self.abort('Unhandled exception in async loop: %s' % value)

    def housekeeping (self):
        if self.state < AppStates.RUNNING:
            self.handle_shutdown_1()

        self.handle_reap()
        self.handle_signal()
        self.handle_squib_states()

        if self.state < AppStates.RUNNING:
            self.handle_shutdown_2()

        self.reactor.call_later(self.housekeeping_period, self.housekeeping)

    def handle_reap (self, once=False):
        pid, sts = utility.waitpid()
        while pid:

            sqobj = None
            for sq in self.squibs:
                if sq.pid == pid:
                    sqobj = sq
                    break

            if sqobj is None:
                self.log.critical("reaped unknown pid %s" % pid)
            else:
                sqobj.finish(pid, sts)

            if once: break
            pid, sts = utility.waitpid()

    def handle_signal (self):
        if self.signal is not None:
            sig, self.signal = self.signal, None
            if sig in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT):
                self.log.warn("Received %s indicating exit request" % utility.signame(sig))
                self.state = AppStates.SHUTDOWN

            elif sig == signal.SIGHUP:
                self.log.warn("Received %s indicating restart request" % utility.signame(sig))
                self.state = AppStates.RESTARTING

            elif sig == signal.SIGCHLD:
                self.log.debug("Received %s indicating a child quit" % utility.signame(sig))

            elif sig == signal.SIGUSR2:
                self.log.info("Received %s indicating log reopen request" % utility.signame(sig))
                self.log.warn("TODO: implement log reopen")

            else:
                self.log.moreinfo("Received %s signal. Ignored." % utility.signame(sig))

    def handle_squib_states (self):
        [ sqobj.do_state_transition() for sqobj in self.squibs ]

    def handle_shutdown_1 (self):
        if not self.stopping:
            # first time, set the stopping flag, and and tell all squibs to stop
            self.stopping = True
            self.stopping_squibs = self.squibs[:]

        # stop the last squib (the one with the "highest" priority)
        if self.stopping_squibs:
            self.stopping_squibs[-1].stop()

        for sqobj in self.squibs:
            if not sqobj.is_stopped():
                break
        else:
            self.log.info("%s %s STOP" % (self.app_name, self.app_version))
            self.reactor.stop()

    def handle_shutdown_2 (self):
        # after part1, we've transitioned and reaped, so lets see if we can
        # remove the squib stopped from the stopping_squibs queue
        if self.stopping_squibs:
            sqobj = self.stopping_squibs.pop()
            if not sqobj.is_stopped():
                # if the squib is not yet in a stopped state, we're not yet done
                # shutting this squib done, so push it back on to the end of the
                # stopping_squibs queue
                self.stopping_squibs.append(sqobj)

    def report (self):
        lines = self.metrics_recorder.publish()
        for line in lines:
            self.log.info("REPORT: %s" % line)
        self.reactor.call_later(self.report_period, self.report)

##############################################################################

if __name__ == "__main__":
    g = SquibMain()
    g.start()

##############################################################################
## THE END
