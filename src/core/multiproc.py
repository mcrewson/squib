# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id: application.py 6724 2010-04-15 21:01:19Z markc $
#

__all__ = [ 'ParentController', 'ChildController' ]

import errno, fcntl, logging, os, signal, sys, time, traceback

from core.async             import get_reactor
from core.baseobject        import BaseObject, NonStdlibError
from core.log               import get_logger
from core.string_conversion import convert_to_floating

##############################################################################

class ParentStates:
    FATAL      = 2
    RUNNING    = 1
    RESTARTING = 0
    SHUTDOWN   = -1

class ParentController (BaseObject):

    options = { 'housekeeping_period' : (0.5, convert_to_floating) }

    def __init__ (self, **kw):
        super(ParentController, self).__init__(**kw)
        self._parse_options(ParentController.options, kw)
        self.children = []
        self.stopping = False
        self.stopping_children = None
        self.log = get_logger()

    #### SETUP AND CLEANUP ################################################

    def _internal_setup (self):
        self.signal = None
        signal.signal(signal.SIGTERM, self.sig_receiver)
        signal.signal(signal.SIGINT,  self.sig_receiver)
        signal.signal(signal.SIGQUIT, self.sig_receiver)
        signal.signal(signal.SIGHUP,  self.sig_receiver)
        signal.signal(signal.SIGCHLD, self.sig_receiver)
        signal.signal(signal.SIGUSR2, self.sig_receiver)

        self.reactor = get_reactor()
        self.reactor.call_later(self.housekeeping_period, self.housekeeping)

    def sig_receiver (self, sig, frame):
        self.signal = sig

    def setup (self):
        pass

    #### MAIN LOOP ####

    def start (self):
        self._internal_setup()
        self.setup()

        try:
            self.state = ParentStates.RUNNING
            self.reactor.start()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.reactor.stop()
            exc_type, value, tb = sys.exc_info()
            if self.exception_hook is not None:
                self.exception_hook(exc_type, value, tb)
            self.abort('Unhandled exception in async loop: %s' % value)

    def add_child (self, child):
        self.children.append(child)

    def housekeeping (self):
        if self.state < ParentStates.RUNNING:
            self.handle_shutdown_1()

        self.handle_reap()
        self.handle_signal()
        self.handle_child_states()

        if self.state < ParentStates.RUNNING:
            self.handle_shutdown_2()

        self.reactor.call_later(self.housekeeping_period, self.housekeeping)

    def handle_reap (self, once=False):
        pid, sts = _waitpid()
        while pid:
            for child in self.children:
                if child.pid == pid:
                    child.finish(pid, sts)
            if once: break
            pid, sts = _waitpid()

    def handle_signal (self):
        if self.signal is not None:
            sig, self.signal = self.signal, None
            if sig in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT):
                self.log.warn("Received %s indicating exit request" % _signame(sig))
                self.state = ParentStates.SHUTDOWN

            elif sig == signal.SIGHUP:
                self.log.warn("Received %s indicating restart request" % _signame(sig))
                self.state = ParentStates.RESTARTING

            elif sig == signal.SIGCHLD:
                self.log.debug("Received %s indicating a child quit" % _signame(sig))

            elif sig == signal.SIGUSR2:
                self.log.info("Received %s indicating log reopen request" % _signame(sig))
                self.log.warn("TODO: implement log reopen")

            else:
                self.log.moreinfo("Received %s signal. Ignored." % _signame(sig))

    def handle_child_states (self):
        [ child.do_state_transition() for child in self.children ]

    def handle_shutdown_1 (self):
        if not self.stopping:
            # first time, set the stopping flag, and and tell all children to stop
            self.stopping = True
            self.stopping_children = self.children[:]

        # stop the last child (the one with the "highest" priority)
        if self.stopping_children:
            self.stopping_children[-1].stop()

        for child in self.children:
            if not child.is_stopped():
                break
        else:
            self.reactor.stop()

    def handle_shutdown_2 (self):
        # after part1, we've transitioned and reaped, so lets see if we can
        # remove the child stopped from the stopping_children queue
        if self.stopping_children:
            child = self.stopping_children.pop()
            if not child.is_stopped():
                # if the child is not yet in a stopped state, we're not yet done
                # shutting this child done, so push it back on to the end of the
                # stopping_children queue
                self.stopping_children.append(child)

#############################################################################

class ChildStates:
    STOPPED  = 0
    STARTING = 10
    RUNNING  = 20
    BACKOFF  = 30
    STOPPING = 40
    EXITED   = 100
    FATAL    = 200
    UNKNOWN  = 1000

def get_childstate_description (code):
    for statename in ChildStates.__dict__:
        if getattr(ChildStates, statename) == code:
            return statename

##############################################################################

class ChildController (object):

    pid       = 0       # process id; 0 when not running
    laststart = 0       # last time the process was started; 0 if never
    laststop  = 0       # last time the process was stopped; 0 if never
    delay     = 0       # if nonzero, delay starting or killing until this time
    killing   = 0       # flag determines whether we are trying to kill this process
    backoff   = 0       # backoff counter (to startretries)

    def __init__ (self, name):
        self.name = name
        self.state = ChildStates.STOPPED
        self.pipes = {}
        self.reactables = []
        self.log = get_logger()

        self.priority = 999
        self.startsecs = 1
        self.startretries = 3
        self.stopsignal = _signum("TERM")
        self.stopwaitsecs = 10

    def __cmp__ (self, other):
        return cmp(self.priority, other.priority)

    def __repr__ (self):
        return "<ChildController: %s, priority=%d>" % (self.name, self.priority)

    def assert_state (self, *states):
        if self.state not in states:
            current_state = get_childstate_description(self.state)
            allowable_states = ' '.join(map(get_childstate_description, states))
            raise AssertionError("Assertion failed for %s: %s not in %s" % (self.name, current_state, allowable_states))

    def change_state (self, new_state, expected=True):
        old_state = self.state
        if new_state is old_state:
            return False # for unit testing

        if new_state == ChildStates.BACKOFF:
            now = time.time()
            self.backoff = self.backoff + 1
            self.delay = now + self.backoff

        self.state = new_state

    def do_state_transition (self):
        now = time.time()
        state = self.state

        if state == ChildStates.EXITED:
            self.launch()
            return

        elif state == ChildStates.STOPPED and not self.laststart:
            self.launch()
            return

        elif state == ChildStates.BACKOFF:
            if self.backoff <= self.startretries:
                if now > self.delay:
                    self.launch()
                    return
            else:
                self.give_up()

        elif state == ChildStates.STARTING:
            if now - self.laststart > self.startsecs:
                # STARTING -> RUNNING if the process has started successfully
                # and it has stayed up for at least startsecs
                self.delay = 0
                self.backoff = 0
                self.change_state(ChildStates.RUNNING)

        elif state == ChildStates.STOPPING:
            time_left = self.delay - now
            if time_left <= 0:
                self.kill("KILL")

    def launch (self):
        if self.pid:
            self.log.warn("Process %r already running" % self.name)
            return

        self.laststart = time.time()

        self.assert_state(ChildStates.EXITED, ChildStates.FATAL,
                          ChildStates.BACKOFF, ChildStates.STOPPED)

        self.change_state(ChildStates.STARTING)

        try:
            self.pipes = _make_pipes()
        except OSError, why:
            code = why[0]
            if code == errno.EMFILE:
                # too many open file descriptors
                msg = 'too many open files to launch %r' % self.name
            else:
                msg = 'unknown error: %s' % errno.errorcode.get(code, code)
            self.log.warn("launch error: %s" % msg)
            self.assert_state(ChildStates.STARTING)
            self.change_state(ChildStates.BACKOFF)
            return

        _close_fd(self.pipes['p_stdin'])
        stdout_reactable = self.get_stdout_reactable(self.pipes['p_stdout'])
        if stdout_reactable is not None:
            self.reactables.append(stdout_reactable)
        stderr_reactable = self.get_stderr_reactable(self.pipes['p_stderr'])
        if stderr_reactable is not None:
            self.reactables.append(stderr_reactable)

        try:
            pid = os.fork()
        except OSError, why:
            code = why[0]
            if code == errno.EAGAIN:
                errmsg = 'Too many processes in process table to sawn %r' % (self.name)
            else:
                errmsg = 'unknown error: %s' % errno.errorcode.get(code, code)
            self.log.warn("launch error: %s" % errmsg)
            self.assert_state(ChildStates.STARTING)
            self.change_state(ChildStates.BACKOFF)
            _close_parent_pipes(self.pipes)
            _close_child_pipes(self.pipes)
            return

        if pid != 0:
            # Parent process
            self.pid = pid
            _close_child_pipes(self.pipes)
            self.log.info("launched: %r with pid %s" % (self.name, pid))
            self.delay = time.time() + self.startsecs
            return pid

        else:
            # Child process
            try:
                try:
                    os.setpgrp()

                    os.dup2(self.pipes['c_stdin'], 0)
                    os.dup2(self.pipes['c_stdout'], 1)
                    os.dup2(self.pipes['c_stderr'], 2)
                    _close_parent_pipes(self.pipes)

                    for i in range(3, 1024):
                        _close_fd(i)

                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    signal.signal(signal.SIGINT,  signal.SIG_DFL)
                    signal.signal(signal.SIGQUIT, signal.SIG_DFL)
                    signal.signal(signal.SIGHUP,  signal.SIG_DFL)
                    
                    self.run()

                except Exception:
                    tb = _get_exception_string()
                    os.write(2, 'could not launch child %s: unknown exception: %s\n' % (self.name, tb))
            finally:
                os._exit(127)

    def get_stdout_reactable (self, stdout_fd):
        return None

    def get_stderr_reactable (self, stderr_fd):
        return None

    def run (self):
        pass

    def stop (self):
        if self.state in (ChildStates.RUNNING, ChildStates.STARTING):
            self.kill(self.stopsignal)
        elif self.state == ChildStates.BACKOFF:
            self.give_up()

    def kill (self, sig):
        now = time.time()
        if not self.pid:
            msg = "attempting to kill %s with sig %s but is wasn't running" % (self.name, _signame(sig))
            self.log.debug(msg)
            return msg

        self.log.debug("killing %s (pid %s) with signal %s" % (self.name, self.pid, _signame(sig)))

        self.killing = 1
        self.delay = now + self.stopwaitsecs

        self.assert_state(ChildStates.RUNNING, ChildStates.STARTING, ChildStates.STOPPING)
        self.change_state(ChildStates.STOPPING)

        try:
            os.kill(self.pid, sig)
        except:
            tb = _get_exception_string()
            msg = "unknown problem killing %s (%s): %s" % (self.name, self.pid, tb)
            self.log.critical(msg)
            self.change_state(ChildStates.UNKNOWN)
            self.pid = 0
            self.killing = 0
            self.delay = 0
            return msg
        
        return None

    def is_stopped (self):
        return self.state in (ChildStates.STOPPED, ChildStates.EXITED, ChildStates.FATAL, ChildStates.UNKNOWN)

    def give_up (self):
        self.delay = 0
        self.backoff = 0
        self.assert_state(ChildStates.BACKOFF)
        self.change_state(ChildStates.FATAL)

    def finish (self, pid, sts):
        """
        This process was reaped and we need to report and manage it's state
        """

        # drain the reactables
        for reactable in self.reactables:
            if reactable.readable():
                reactable.handle_read_event()
            if reactable.writable():
                reactable.handle_write_event()

        # Decode the wait status
        if os.WIFEXITED(sts):
            es = os.WEXITSTATUS(sts) & 0xffff
            msg = "exit status %s" % es
            return es, msg
        elif os.WIFSIGNALED(sts):
            es = -1
            sig = os.WTERMSIG(sts)
            msg = "terminated by %s" % signame(sig)
            if hasattr(os, "WCOREDUMP"):
                iscore = os.WCOREDUMP(sts)
            else:
                iscore = sts & 0x80
            if iscore:
                msg += " (core dumped)"
        else:
            es  = -1
            msg = "unknown termination cause 0x%04x" % sts

        now = time.time()
        self.laststop = now
        processname = self.name

        tooquickly = now - self.laststart < self.startsecs
        exit_expected = False

        if self.killing:
            # likely the request of a stop request
            self.killing = 0
            self.delay = 0
            self.exitstatus = es

            msg = "stopped: %s (%s)" % (processname, msg)
            self.assert_state(ChildStates.STOPPING)
            self.change_state(ChildStates.STOPPED)

        elif tooquickly:
            # the program did not stay up long enough to make it to RUNNING
            self.exitstatus = None
            msg = "exited: %s (%s)" % (processname, msg + "; not expected")
            self.assert_state(ChildStates.STARTING)
            self.change_state(ChildStates.BACKOFF)

        else:
            # this finish was not the result of a stop request, the program
            # was in the RUNNING state but exited
            self.delay = 0
            self.backoff = 0
            self.exitstatus = es

            # hack
            if self.state == ChildStates.STARTING:
                self.change_state(ChildStates.RUNNING)

            self.assert_state(ChildStates.RUNNING)

            if exit_expected:
                # expected exit code
                msg = "exited: %s (%s)" % (processname, msg + "; expected")
                self.change_state(ChildStates.EXITED, expected=True)
            else:
                # unexpected exit code
                msg = "exited: %s (%s)" % (processname, msg + "; not expected")
                self.change_state(ChildStates.EXITED, expected=False)

        self.log.info(msg)

        self.pid = 0
        _close_parent_pipes(self.pipes)
        self.pipes = {}
        self.reactables = []


##############################################################################

def _close_fd (fd):
    try:
        os.close(fd)
    except OSError:
        pass

def _make_pipes (stderr=True):
    pipes = { 'p_stdin'  : None, 'c_stdin'  : None,
              'p_stdout' : None, 'c_stdout' : None,
              'p_stderr' : None, 'c_stderr' : None }

    try:
        pipes['p_stdin'], pipes['c_stdin'] = os.pipe()
        pipes['p_stdout'], pipes['c_stdout'] = os.pipe()
        if stderr == True:
            pipes['p_stderr'], pipes['c_stderr'] = os.pipe()
        for fd in (pipes['p_stdout'], pipes['p_stderr'], pipes['p_stdin']):
            if fd is not None:
                fcntl.fcntl(fd, fcntl.F_SETFL, fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NDELAY)
        return pipes
    except OSError, e:
        for fd in pipes.values():
            if fd is not None:
                _close_fd(fd)
        raise e

def _close_parent_pipes (pipes):
    for fdname in ('p_stdin', 'p_stdout', 'p_stderr'):
        fd = pipes[fdname]
        if fd is not None:
            _close_fd(fd)

def _close_child_pipes (pipes):
    for fdname in ('c_stdin', 'c_stdout', 'c_stderr'):
        fd = pipes[fdname]
        if fd is not None:
            _close_fd(fd)

def _waitpid ()
    # need pthread_sigmask here to avoid concurrent sigchld, but Python
    # does not offer it as its not standard across UNIX versions. There is
    # still a raise condition here; we can get a sigchld while we're
    # sitting in the waitpid call
    try:
        pid, sts = os.waitpid(-1, os.WNOHANG)
    except OSError, why:
        err = why[0]
        if err no in (errno.ECHILD, errno.EINTR):
            logging.critical("waitpid error; a process may not be cleaned up properly")
        if err == errno.EINTR:
            logging.debug("EINTR during reap")
        pid, sts = None, None
    return pid, sts

_signames = None

def _signame (sig):
    global _signames
    if _signames is None:
        _init_signames()
    return _signames.get(sig) or "signal %d" % sig

def _init_signames ():
    global _signames
    d = {}
    for k, v in signal.__dict__.items():
        k_startswith = getattr(k, "startswith", None)
        if k_startswith is None: continue
        if k_startswith("SIG") and not k_startswith("SIG_"):
            d[v] = k
    _signames = d

def _signum (sig):
    result = None
    try:
        result = int(sig)
    except (ValueError, TypeError):
        result = getattr(signal, 'SIG'+sig, None)
    try:
        result = int(result)
        return result
    except (ValueError, TypeError):
        raise ValueError('value %s is not a signal name/number' % sig)

def _get_exception_string ():
    if hasattr(traceback, 'format_exc'):
        return traceback.format_exc()
    else:
        import StringIO
        io = StringIO.StringIO()
        traceback.print_exc(file=io)
        return io.getvalue()


#############################################################################
## THE END
