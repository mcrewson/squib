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

import errno, fcntl, heapq, os, select, sys, time

try:
    from collections import deque
except ImportError:
    # backport the 2.4 queue into 2.3
    from backport import deque

try:
    from OpenSSL import SSL
    ssl_supported = True
except ImportError:
    ssl_supported = False

from squib.core.log import getlog

##############################################################################

_reraised_exceptions = (KeyboardInterrupt, SystemExit)

class Reactable (object):

    in_buffer_size  = 4096
    out_buffer_size = 4096

    def __init__ (self, reactor=None):
        super(Reactable, self).__init__()
        self.reactor = reactor or get_reactor()
        self.closed = False
        self.fifo = deque()

    def on_data_read (self, data):
        pass

    def on_closed (self):
        pass

    def write_data (self, data):
        obs = self.out_buffer_size
        if len(data) > obs:
            for i in xrange(0, len(data), obs):
                self.fifo.append(data[i:i+obs])
        else:
            self.fifo.append(data)

    def close (self):
        self.closed = True
        self.del_from_reactor()
        self.handle_close()
        self.on_closed()

    def close_when_done (self):
        self.fifo.append(None)

    ##############################################

    def add_to_reactor (self):
        self.reactor.add_reactable(self)

    def del_from_reactor (self):
        self.reactor.del_reactable(self)

    def fileno (self):
        raise NotImplementedError

    def readable (self):
        return not self.closed

    def writable (self):
        return not self.closed and self.fifo

    def handle_read_event (self):
        data = self.handle_read()
        if data:
            self.on_data_read(data)
        else:
            self.close()

    def handle_write_event (self):
        while self.fifo and self.closed == False:
            first = self.fifo[0]

            # handle empty string/buffer or None entry
            if first is None:
                self.close()
                return

            # find data to write
            obs = self.out_buffer_size
            try:
                data = buffer(first, 0, obs)
            except TypeError:
                data = first.more()
                if daa:
                    self.fifo.appendleft(data)
                else:
                    del self.fifo[0]
                continue

            # write it!
            num_written = self.handle_write(data)
            if num_written:
                if num_written < len(data) or obs < len(first):
                    self.fifo[0] = first[num_written:]
                else:
                    del self.fifo[0]

    def handle_exception_event (self):
        self.close()

    ##############################################

    def handle_read (self):
        getlog().warning("Unhandled reactable read event")

    def handle_write (self, data):
        getlog().warning("Unhandled reactable write event")

    def handle_close (self):
        getlog().warning("Unhandled reactable close event")
        
    def handle_error (self):
        try:
            self_repr = repr(self)
        except:
            self_repr = '<__repr__(self) failed for object at %0x>' % id(self)
        t, v, tb = sys.exc_info()
        while tb.tb_next:
            tb = tb.tb_next
        tbinfo = '[%s|%s|%s]' % (tb.tb_frame.f_code.co_filename,
                                 tb.tb_frame.f_code.co_name,
                                 str(tb.tb_lineno))
        getlog().warning("Unhandled python exception: %s (%s:%s %s)" % (self_repr, t, v, tbinfo))

##############################################################################

class FileDescriptorReactable (Reactable):

    def __init__ (self, fd=None, reactor=None):
        super(FileDescriptorReactable, self).__init__(reactor=reactor)
        if fd is not None:
            self.set_filedescriptor(fd)

    def set_filedescriptor (self, fd):
        self.fd = fd
        flags = fcntl.fcntl(self.fd, fcntl.F_GETFL)
        flags = flags | os.O_NONBLOCK
        fcntl.fcntl(self.fd, fcntl.F_SETFL, flags)
        self.add_to_reactor()

    ###########################################

    def fileno (self):
        return self.fd

    def handle_read (self):
        try:
            return os.read(self.fd, self.in_buffer_size)
        except OSError, err:
            if err[0] in (errno.EWOULDBLOCK, errno.EBADF, errno.EINTR):
                return ''
            else:
                raise

    def handle_write (self, data):
        try:
            return os.write(self.fd, data)
        except OSError, err:
            if err[0] not in (errno.EBADF):
                raise

    def handle_close (self):
        try:
            os.close(self.fd)
        except OSError:
            pass

##############################################################################

class ReadOnlyFileDescriptorReactable (FileDescriptorReactable):

    def writable (self):
        return False

##############################################################################

class ProtocolMixin (Reactable):

    message_delimiter = None
    message_max_size  = 16384

    __buffer = ''

    def on_message_received (self, message):
        pass

    def on_message_size_exceeded (self):
        self.close_when_done()

    def on_data_read (self, data):
        self.__buffer = self.__buffer + data
        try:
            message, self.__buffer = self.__buffer.split(self.message_delimiter, 1)
        except ValueError:
            if len(self.__buffer) > self.max_message_size:
                self.__buffer = ''
                return self.on_message_size_exceeded()
            return

        message_size = len(message)
        if message_size > self.message_max_size:
            return self.on_message_size_exceeded(message)

        self.on_message_received(message)

##############################################################################

class LineOrientedProtocolMixin (ProtocolMixin):

    message_delimiter = '\n'
    
    def write_line (self, line):
        self.write_data(line + self.message_delimiter)

##############################################################################

class TimeoutMixin (object):
    """
    Mixin for reactables that wish to have timeouts.
    """

    timeout = None

    __timeout_call = None

    def set_timeout (self, period):
        """
        Change the timeout period
        """
        prev = self.timeout
        self.timeout = period

        if self.__timeout_call is not None:
            if period is None:
                self.__timeout_call.cancel()
                self.__timeout_call = None
            else:
                self.__timeout_call.reset(period)
        elif period is not None:
            self.__timeout_call = self.reactor.call_later(period, self.__timed_out)

        return prev

    def reset_timeout (self):
        """
        Reset the timeout countdown
        """
        if self.__timeout_call is not None and self.timeout is not None:
            self.__timeout_call.reset(self.timeout)

    def on_timeout (self):
        """
        Called when the connection times out. Override this method to
        define behavior other than dropping the connection.
        """
        self.close()

    def __timed_out (self):
        self.__timeout_call = None
        self.on_timeout()

##############################################################################

class CalledLater (object):

    def __init__ (self, time, func, args, kw, cancel, reset):
        self.time, self.func, self.args, self.kw = time, func, args, kw
        self.canceller = cancel
        self.resetter = reset
        self.cancelled = self.called = False
        self.delayed_time = 0

    def get_time (self):
        return self.time + self.delayed_time

    def cancel (self):
        assert self.cancelled == False, "already cancelled"
        assert self.called == False, "already called"
        self.canceller(self)
        self.cancelled = True
        del self.func, self.args, self.kw

    def reset (self, seconds_from_now):
        assert self.cancelled == False, "already cancelled"
        assert self.called == False, "already called"
        new_time = time.time() + seconds_from_now
        if new_time < self.time:
            self.delayed_time = 0
            self.time = new_time
            self.resetter(self)
        else:
            self.delayed_time = new_time - self.time

    def delay (self, seconds_later):
        assert self.cancelled == False, "already cancelled"
        assert self.called == False, "already called"
        self.delayed_time += seconds_later
        if self.delayed_time < 0:
            self.activate_delay()
            self.resetter(self)

    def activate_delay (self):
        self.time += self.delayed_time
        self.delayed_time = 0

    def active (self):
        return not (self.cancelled or self.called)

    def __le__ (self, other):
        return self.time <= other.time

##############################################################################

class Reactor (object):

    def __init__ (self):
        self.reactables = dict()

        self._pending_timed_calls = list()
        self._new_timed_calls = list()
        self._cancellations = 0

        self.running = False
    
    def start (self):
        assert self.running == False, "reactor already started"
        self.running = True
        self._run()

    def stop (self):
        self.running = False

    def add_reactable (self, r):
        assert isinstance(r, Reactable), "Must only add Reactable instances"
        fd = r.fileno()
        if fd is not None:
            self.reactables[fd] = r

    def del_reactable (self, r):
        assert isinstance(r, Reactable), "Must only del Reactable instances"
        fd = r.fileno()
        if fd is not None:
            del self.reactables[fd]

    def call_later (self, seconds, func, *args, **kw):
        assert callable(func), "%s is not callable" % func
        assert sys.maxint >= seconds >= 0, "%s is not greater than or equals to 0 seconds" % seconds
        tple = CalledLater(time.time() + seconds, func, args, kw,
                           self._cancel_calllater,
                           self._move_calllater_sooner)
        self._new_timed_calls.append(tple)
        return tple

    def _cancel_calllater (self, tple):
        self._cancellations += 1

    def _move_calllater_sooner (self, tple):
        heap = self._pending_timed_calls
        try:
            pos = heap.index(tple)

            # move elt up the heap until it rests at the right place
            elt = heap[pos]
            while pos != 0:
                parent = (pos - 1) // 2
                if heap[parent] <= elt:
                    break
                # move parent down
                heap[pos] = heap[parent]
                pos = parent
            heap[pos] = elt
        except ValueError:
            # element was not found on heap - oh well ...
            pass

    def _insert_new_calledlaters (self):
        for call in self._new_timed_calls:
            if call.cancelled:
                self._cancellations += 1
            else:
                call.activate_delay()
                heapq.heappush(self._pending_timed_calls, call)
        self._new_timed_calls = list()

    def _run (self):
        while self.running:
            self._run_until_current()
            timeout = float(self.running and self._calc_timeout())
            self._do_iteration(timeout)

    def _run_until_current (self):
        self._insert_new_calledlaters()
        now = time.time()
        while self._pending_timed_calls and (self._pending_timed_calls[0].time <= now):
            call = heapq.heappop(self._pending_timed_calls)
            if call.cancelled:
                self._cancellations -= 1
                continue

            if call.delayed_time > 0:
                call.activate_delay()
                heapq.heappush(self._pending_timed_calls, call)
                continue

            try:
                call.called = 1
                call.func(*call.args, **call.kw)
            except _reraised_exceptions:
                raise
            except:
                getlog().exception("CallLater failed")

        if (self._cancellations > 50 and
            self._cancellations > len(self._pending_timed_calls) >> 1):
            self._cancellations = 0
            self._pending_timed_calls = [ x for x in self._pending_timed_calls if not x.cancelled ]
            heapq.heapify(self._pending_timed_calls)

    def _calc_timeout (self):
        self._insert_new_calledlaters()
        if not self._pending_timed_calls:
            return 0
        return max(0, self._pending_timed_calls[0].time - time.time())

    def _do_iteration (self, timeout=0.0):
        raise NotImplementedError("override this function in your reactors")

    def _reactable_read (self, fd):
        reactable = self.reactables.get(fd)
        if reactable is None: return
        try:
            reactable.handle_read_event()
        except _reraised_exceptions:
            raise
        except:
            reactable.handle_error()

    def _reactable_write (self, fd):
        reactable = self.reactables.get(fd)
        if reactable is None: return
        try:
            reactable.handle_write_event()
        except _reraised_exceptions:
            raise
        except:
            reactable.handle_error()

    def _reactable_exception (self, fd):
        reactable = self.reactables.get(fd)
        if reactable is None: return
        try:
            reactable.handle_exception_event()
        except _reraised_exceptions:
            raise
        except:
            reactable.handle_error()

##############################################################################

class SelectReactor (Reactor):

    def _do_iteration (self, timeout=0.0):
        r = []; w = []; e = []
        for fd, reactable in self.reactables.items():
            if reactable.readable():
                r.append(fd)
            if reactable.writable():
                w.append(fd)
            if reactable.readable() or reactable.writable():
                e.append(fd)

        if not r and not w and not e: return

        try:
            r,w,e = select.select(r, w, e, timeout)
        except (IOError, select.error), err:
            if err.args[0] != errno.EINTR:
                raise
            return

        for fd in r:
            self._reactable_read(fd)
        for fd in w:
            self._reactable_write(fd)
        for fd in e:
            self._reactable_exception(fd)

##############################################################################

class PollReactor (Reactor):

    def _do_iteration (self, timeout=0.0):
        if timeout is not None:
            timeout = timeout * 1000

        poller = select.poll()
        for fd, reactable in self.reactables.items():
            flags = 0
            if reactable.readable():
                flags |= select.POLLIN | select.POLLPRI
            if reactable.writable():
                flags |= select.POLLOUT
            if flags:
                flags |= select.POLLERR | select.POLLHUP | select.POLLNVAL
                poller.register(fd, flags)

        try:
            r = poller.poll(timeout)
        except (IOError, select.error), err:
            if err.args[0] != errno.EINTR:
                raise
            r = []

        for fd, flags in r:
            if flags & (select.POLLIN | select.POLLPRI):
                self._reactable_read(fd)
            if flags & (select.POLLOUT):
                self._reactable_write(fd)
            if flags & (select.POLLERR | select.POLLHUP | select.POLLNVAL):
                self._reactable_exception(fd)

##############################################################################

class EpollReactor (Reactor):

    def _do_iteration (self, timeout=0.0):
        poller = select.epoll()
        for fd, reactable in self.reactables.items():
            flags = 0
            if reactable.readable():
                flags |= select.EPOLLIN | select.EPOLLPRI
            if reactable.writable():
                flags |= select.EPOLLOUT
            if flags:
                flags |= select.EPOLLERR | select.EPOLLHUP
                poller.register(fd, flags)

        try:
            r = poller.poll(timeout)
        except (IOError, select.error), err:
            if err.args[0] != errno.EINTR:
                raise
            r = []

        for fd, flags in r:
            if flags & (select.EPOLLIN | select.EPOLLPRI):
                self._reactable_read(fd)
            if flags & (select.EPOLLOUT):
                self._reactable_write(fd)
            if flags & (select.EPOLLERR | select.EPOLLHUP):
                self._reactable_exception(fd)

##############################################################################

class KqueueReactor (Reactor):
    pass

##############################################################################

def get_reactor ():
    global __the_reactor
    try:
        if __the_reactor is not None:
            return __the_reactor
    except NameError:
        pass

    # Calculate the best reactor to use
    if hasattr(select, "epoll"):
        __the_reactor = EpollReactor()
#    elif hasattr(select, "kqueue"):
#        __the_reactor = KqueueReactor()
    elif hasattr(select, "poll"):
        __the_reactor = PollReactor()
    else:
        __the_reactor = SelectReactor()
    return __the_reactor

def set_reactor (reactor):
    global __the_reactor
    assert isinstance(reactor, Reactor), "Must be an instance of Reactor"
    __the_reactor = reactor
    return

def free_reactor ():
    global __the_reactor
    __the_reactor = None

##############################################################################
## THE END
