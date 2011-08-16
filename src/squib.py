# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#

import errno, fcntl, logging, os, signal, sys, time, traceback

from core.async  import Reactable
from core.config import ConfigError
from core.log    import get_logger
import utility

##############################################################################
##############################################################################

class SquibStates:
    STOPPED  = 0
    STARTING = 10
    RUNNING  = 20
    BACKOFF  = 30
    STOPPING = 40
    EXITED   = 100
    FATAL    = 200
    UNKNOWN  = 1000

def get_state_description (code):
    for statename in SquibStates.__dict__:
        if getattr(SquibStates, statename) == code:
            return statename

##############################################################################

def create_squib (name, config, metrics_recorder):
    if config.has_key("class"):
        return PythonSquib(name, config, metrics_recorder)
    else:
        raise ConfigError("Unknown type of squib: %s" % name)

##############################################################################

class Squib (object):

    pid       = 0       # process id; 0 when not running
    laststart = 0       # last time the process was started; 0 if never
    laststop  = 0       # last time the process was stopped; 0 if never
    delay     = 0       # if nonzero, delay starting or killing until this time
    killing   = 0       # flag determines whether we are trying to kill this process
    backoff   = 0       # backoff counter (to startretries)

    def __init__ (self, name, config, metrics_recorder):
        self.name = name
        self.config = config
        self.metrics_recorder = metrics_recorder
        self.state = SquibStates.STOPPED
        self.pipes = {}
        self.reactables = []
        self.log = get_logger()

        self.priority = 999
        self.startsecs = 1
        self.startretries = 3
        self.stopsignal = utility.signum("TERM")
        self.stopwaitsecs = 10

    def __cmp__ (self, other):
        return cmp(self.priority, other.priority)

    def __repr__ (self):
        return "<Squib: %s, priority=%d>" % (self.name, self.priority)

    def assert_state (self, *states):
        if self.state not in states:
            current_state = get_state_description(self.state)
            allowable_states = ' '.join(map(get_state_description, states))
            raise AssertionError("Assertion failed for %s: %s not in %s" % (self.name, current_state, allowable_states))

    def change_state (self, new_state, expected=True):
        old_state = self.state
        if new_state is old_state:
            return False # for unit testing

        if new_state == SquibStates.BACKOFF:
            now = time.time()
            self.backoff = self.backoff + 1
            self.delay = now + self.backoff

        self.state = new_state

    def do_state_transition (self):
        now = time.time()
        state = self.state

        if state == SquibStates.EXITED:
            self.launch()
            return

        elif state == SquibStates.STOPPED and not self.laststart:
            self.launch()
            return

        elif state == SquibStates.BACKOFF:
            if self.backoff <= self.startretries:
                if now > self.delay:
                    self.launch()
                    return
            else:
                self.give_up()

        elif state == SquibStates.STARTING:
            if now - self.laststart > self.startsecs:
                # STARTING -> RUNNING if the process has started successfully
                # and it has stayed up for at least startsecs
                self.delay = 0
                self.backoff = 0
                self.change_state(SquibStates.RUNNING)

        elif state == SquibStates.STOPPING:
            time_left = self.delay - now
            if time_left <= 0:
                self.kill("KILL")

    def launch (self):
        if self.pid:
            self.log.warn("Process %r already running" % self.name)
            return

        self.laststart = time.time()

        self.assert_state(SquibStates.EXITED, SquibStates.FATAL,
                          SquibStates.BACKOFF, SquibStates.STOPPED)

        self.change_state(SquibStates.STARTING)

        try:
            self.pipes = make_pipes()
        except OSError, why:
            code = why[0]
            if code == errno.EMFILE:
                # too many open file descriptors
                msg = 'too many open files to launch %r' % self.name
            else:
                msg = 'unknown error: %s' % errno.errorcode.get(code, code)
            self.log.warn("launch error: %s" % msg)
            self.assert_state(SquibStates.STARTING)
            self.change_state(SquibStates.BACKOFF)
            return

        utility.close_fd(self.pipes['p_stdin'])
        self.reactables.append(MetricsReader(self.pipes['p_stdout'], self.metrics_recorder))
        self.reactables.append(SquibController(self.pipes['p_stderr']))

        try:
            pid = os.fork()
        except OSError, why:
            code = why[0]
            if code == errno.EAGAIN:
                errmsg = 'Too many processes in process table to sawn %r' % (self.name)
            else:
                errmsg = 'unknown error: %s' % errno.errorcode.get(code, code)
            self.log.warn("launch error: %s" % errmsg)
            self.assert_state(SquibStates.STARTING)
            self.change_state(SquibStates.BACKOFF)
            close_parent_pipes(self.pipes)
            close_child_pipes(self.pipes)
            return

        if pid != 0:
            # Parent process
            self.pid = pid
            close_child_pipes(self.pipes)
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
                    close_parent_pipes(self.pipes)

                    for i in range(3, 1024):
                        utility.close_fd(i)

                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    signal.signal(signal.SIGINT,  signal.SIG_DFL)
                    signal.signal(signal.SIGQUIT, signal.SIG_DFL)
                    signal.signal(signal.SIGHUP,  signal.SIG_DFL)
                    
                    self.launch_squib()

                except Exception:
                    tb = get_exception_string()
                    os.write(2, 'could not launch squib %s: unknown exception: %s\n' % (self.name, tb))
            finally:
                os._exit(127)

    def launch_squib (self):
        raise NotImplementedError("Override launch_squib() to implement a real squib process")

    def stop (self):
        if self.state in (SquibStates.RUNNING, SquibStates.STARTING):
            self.kill(self.stopsignal)
        elif self.state == SquibStates.BACKOFF:
            self.give_up()

    def kill (self, sig):
        now = time.time()
        if not self.pid:
            msg = "attempting to kill %s with sig %s but is wasn't running" % (self.name, utility.signame(sig))
            self.log.debug(msg)
            return msg

        self.log.debug("killing %s (pid %s) with signal %s" % (self.name, self.pid, utility.signame(sig)))

        self.killing = 1
        self.delay = now + self.stopwaitsecs

        self.assert_state(SquibStates.RUNNING, SquibStates.STARTING, SquibStates.STOPPING)
        self.change_state(SquibStates.STOPPING)

        try:
            os.kill(self.pid, sig)
        except:
            tb = get_exception_string()
            msg = "unknown problem killing %s (%s): %s" % (self.name, self.pid, tb)
            self.log.critical(msg)
            self.change_state(SquibStates.UNKNOWN)
            self.pid = 0
            self.killing = 0
            self.delay = 0
            return msg
        
        return None

    def is_stopped (self):
        return self.state in (SquibStates.STOPPED, SquibStates.EXITED, SquibStates.FATAL, SquibStates.UNKNOWN)

    def give_up (self):
        self.delay = 0
        self.backoff = 0
        self.assert_state(SquibStates.BACKOFF)
        self.change_state(SquibStates.FATAL)

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

        es, msg = utility.decode_wait_status(sts)

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
            self.assert_state(SquibStates.STOPPING)
            self.change_state(SquibStates.STOPPED)

        elif tooquickly:
            # the program did not stay up long enough to make it to RUNNING
            self.exitstatus = None
            msg = "exited: %s (%s)" % (processname, msg + "; not expected")
            self.assert_state(SquibStates.STARTING)
            self.change_state(SquibStates.BACKOFF)

        else:
            # this finish was not the result of a stop request, the program
            # was in the RUNNING state but exited
            self.delay = 0
            self.backoff = 0
            self.exitstatus = es

            # hack
            if self.state == SquibStates.STARTING:
                self.change_state(SquibStates.RUNNING)

            self.assert_state(SquibStates.RUNNING)

            if exit_expected:
                # expected exit code
                msg = "exited: %s (%s)" % (processname, msg + "; expected")
                self.change_state(SquibStates.EXITED, expected=True)
            else:
                # unexpected exit code
                msg = "exited: %s (%s)" % (processname, msg + "; not expected")
                self.change_state(SquibStates.EXITED, expected=False)

        self.log.info(msg)

        self.pid = 0
        close_parent_pipes(self.pipes)
        self.pipes = {}
        self.reactables = []


##############################################################################

class PythonSquib (Squib):

    def launch_squib (self):
        klass = self.config.get("class")
        if klass is None:
            raise ConfigError("No class defined for this squib")

        module = self.get_squib_module(klass)
        run_method = getattr(module, 'run')
        run_method(10.0)

    def get_squib_module (self, fqclass):
        __import__(fqclass)
        return sys.modules[fqclass]

##############################################################################

class ReadOnlyFileDescriptorReactable (Reactable):

    closed = False
    output_buffer = ''

    def __init__ (self, fd, reactor=None):
        assert fd is not None, "Must supply a valid file descriptor"
        Reactable.__init__(self, reactor)
        self.fd = fd
        self.add_to_reactor()

    def close (self):
        if self.closed == False:
            self.closed = True
            self.del_from_reactor()
            utility.close_fd(self.fd)

    def readable (self):
        return self.closed == False

    def writable (self):
        return False

    def handle_read_event (self):
        try:
            data = os.read(self.fd, 8192)
        except OSError, why:
            if why[0] not in (errno.EWOULDBLOCK, errno.EBADF, errno.EINTR):
                raise
            data = ''
        if data:
            self.data_received(data)
        else:
            # no data back means the child process has ended
            self.close()

    def handle_exception_event (self):
        self.handle_read_event()

    def data_received (self, data):
        pass

##############################################################################

class MetricsReader (ReadOnlyFileDescriptorReactable):

    def __init__ (self, fd, metrics_recorder, reactor=None):
        ReadOnlyFileDescriptorReactable.__init__(self, fd, reactor)
        self.metrics_recorder = metrics_recorder

    def data_received (self, data):
        for line in data.split('\n'):
            if not line: continue
            mname, mvalue = line.split(' ', 1)
            self.metrics_recorder.record(mname, mvalue)

##############################################################################

class SquibController (ReadOnlyFileDescriptorReactable):

    def data_received (self, data):
        pass

##############################################################################

def make_pipes (stderr=True):
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
                utility.close_fd(fd)
        raise e

def close_parent_pipes (pipes):
    for fdname in ('p_stdin', 'p_stdout', 'p_stderr'):
        fd = pipes[fdname]
        if fd is not None:
            utility.close_fd(fd)

def close_child_pipes (pipes):
    for fdname in ('c_stdin', 'c_stdout', 'c_stderr'):
        fd = pipes[fdname]
        if fd is not None:
            utility.close_fd(fd)

##############################################################################

def get_exception_string ():
    if hasattr(traceback, 'format_exc'):
        return traceback.format_exc()
    else:
        import StringIO
        io = StringIO.StringIO()
        traceback.print_exc(file=io)
        return io.getvalue()

##############################################################################
## THE END
