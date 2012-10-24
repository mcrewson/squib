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

import errno, socket, sys

try:
    from OpenSSL import SSL
    ssl_supported = True
except ImportError:
    ssl_supported = False

from squib.core.async import Reactable

##############################################################################

class SocketReactable (Reactable):

    connected = False
    accepting = False

    def __init__ (self, addr=None, sock=None, reactor=None):
        super(SocketReactable, self).__init__(reactor=reactor)
        self.address = addr
        if sock is not None:
            self.set_socket(sock)

    def set_socket (self, sock):
        self.socket = sock
        self.socket.setblocking(0)
        self.add_to_reactor()

    def create_socket (self, family=socket.AF_INET, type=socket.SOCK_STREAM):
        self.set_socket(socket.socket(family, type))

    def on_accept (self, sock, addr):
        pass

    def on_connect (self):
        pass

    ##############################################

    def fileno (self):
        return self.socket.fileno()

    def writable (self):
        if not self.connected: return True
        return super(SocketReactable, self).writable()

    def handle_read_event (self):
        if self.accepting:
            if not self.connected:
                self.connected = True
            self.handle_accept()
            return
        if not self.connected:
            self.handle_connect()
            self.connected = True
        super(SocketReactable, self).handle_read_event()

    def handle_write_event (self):
        if not self.connected:
            self.handle_connect()
            self.connected = True
        super(SocketReactable, self).handle_write_event()

    def handle_accept (self):
        try:
            sock, addr = self.socket.accept()
            self.on_accept(sock, addr)
        except socket.error, err:
            if err.args[0] != errno.EWOULDBLOCK:
                raise

    def handle_connect (self):
        assert self.address is not None, "no address specified for connect"
        self.connected = False
        err = self.socket.connect_ex(self.address)
        if err in (errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK):
            return
        if err in (errno.EISCONN, 0):
            self.connected = True
            self.on_connect()
        else:
            raise socket.error(err, errorcode[err])

    def handle_read (self):
        try:
            return self.socket.recv(self.in_buffer_size)
        except socket.error, err:
            if err.args[0] in (errno.ECONNRESET, errno.ENOTCONN, errno.ESHUTDOWN):
                return ''
            else:
                raise

    def handle_write (self, data):
        try:
            return self.socket.send(data)
        except socket.error, err:
            if err.args[0] != errno.EWOULDBLOCK:
                raise
            return 0

    def handle_close (self):
        self.connected = False
        self.accepting = False
        try:
            self.socket.close()
        except socket.error, err:
            if err.args[0] not in (errno.ENOTCONN, errno.EBADF):
                raise

    ##############################################

    # Socket object methods

    def bind (self, addr):
        self.socket.bind(addr)

    def listen (self, num):
        self.accepting = True
        return self.socket.listen(num)

    def set_reuse_addr (self):
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                   self.socket.getsockopt(socket.SOL_SOCKET, 
                                                          socket.SO_REUSEADDR) | 1)
        except socket.error:
            pass

##############################################################################

if ssl_supported == True:
    class SSLSocketReactable (SocketReactable):

        def __init__ (self, cert, pkey, cacert=None, addr=None, sock=None, reactor=None):
            self.ssl_ctx = None
            self.set_ssl_certificate(cert, pkey, cacert)
            super(SSLSocketReactable, self).__init__(addr, sock, reactor)

        def set_ssl_certificae (self, cert, pkey, cacert=None):
            self.set_ssl_context(self._create_ssl_context(cert, pkey, cacert))

        def set_ssl_context (self, ctx):
            self.ssl_ctx = ctx

        def create_socket (self, family, type):
            sock = socket.socket(family, type)
            if self.ssl_ctx is not None:
                sock = SSL.Connection(self.ssl_ctx, sock)
            self.set_socket(sock)

        def handle_read (self):
            while True:
                try:
                    return super(SSLSocketReactable, self).handle_read()
                except SSL.ZeroReturnError:
                    return ''
                except SSL.WantReadError:
                    time.sleep(0.2)
                    continue
                except SSL.Error, e:
                    if self._can_ignore_ssl_error(e):
                        return ''
                    raise

        def handle_write (self, data):
            while True:
                try:
                    return super(SSLSocketReactable, self).handle_write(data)
                except SSL.SysCallError, e:
                    if e.args[0] == errno.EPIPE:
                        self.close()
                        return 0
                    elif e.args[0] == errno.EWOULDBLOCK:
                        return 0
                    else:
                        raise socket.error(e)
                except (SSL.WantWriteError, SSL.WantReadError):
                    time.sleep(0.2)
                    continue

        def _create_ssl_context (self, cert, pkey, cacert=None):
            ctx = SSL.Context(SSL.SSLv3_METHOD)
            ctx.use_certificate_file(cert)
            ctx.use_privatekey_file(pkey)
            if cacert is not None:
                ctx.load_client_ca(cacert)
                ctx.load_verify_locations(cacert)
                verify = SSL.VERIFY_PEER | SSL.VERIFY_FAIL_IF_NO_PEER_CERT
                def our_verify (connection, x509, errNum, errDepth, preverifyOK):
                    return preverifyOK
                ctx.set_verify(verify, our_verify)
                ctx.set_verify_depth(10)
            ctx.set_options(SSL.OP_NO_SSLv2 | SSL.OP_NO_TLSv1)
            return ctx

        def _can_ignore_ssl_error (e):
            if e.args[0] in (errno.ECONNRESET, ECONNREFUSED):
                return True
            s = "%s" % e
            if s == "no certificate returned":
                return True
            elif s == "wrong version number":
                return True
            elif s == "unexpected eof":
                return True
            return False

##############################################################################

class Server (SocketReactable):

    def __init__ (self, bind_address, protocol, sock=None, reactor=None):
        self.bind_address = bind_address
        self.protocol = protocol
        super(Server, self).__init__(sock=sock, reactor=reactor)
        if not sock:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.set_reuse_addr()

    def activate (self):
        self.bind(self.bind_address)
        self.listen(5)

    def writable (self):
        return False

    def on_accept (self, sock, addr):
        self.protocol(addr, sock, self.reactor)

##############################################################################

if ssl_supported == True:
    class SSLServer (SSLSocketReactable, Server):

        def __init__ (self, bind_address, protocol, cert, pkey,
                      cacert=None, sock=None, reactor=None):
            SSLSocketReactable.__init__(self, cert, pkey, cacert, sock, reactor)
            Server.__init__(self, bind_address, protocol, sock, reactor)

##############################################################################

def __test ():

    from squib.core.async import get_reactor, LineOrientedProtocolMixin, TimeoutMixin

    class ChatProtocol (SocketReactable, LineOrientedProtocolMixin, TimeoutMixin):

        channels = dict()

        idletime = 10

        def __init__ (self, sock, addr, reactor):
            super(ChatProtocol, self).__init__(sock, addr, reactor)
            ChatProtocol.channels[self] = 1
            self.nick = None
            self.write_data('nickname: ')
            self.set_timeout(self.idletime)

        def on_closed (self):
            del ChatProtocol.channels[self]

        def on_timeout (self):
            self.write_line("Connection timed out. Goodbye.")
            self.handle_talk("[quit - timed out]")
            self.close_when_done()

        def on_message_received (self, line):
            self.reset_timeout()
            if self.nick is None:
                try:
                    self.nick = line.split()[0]
                except IndexError:
                    self.nick = None
                if not self.nick:
                    self.write_line("Huh?")
                    self.write_data('nickname: ')
                else:
                    # Greet
                    self.write_line("Hello, %s" % self.nick)
                    self.handle_talk("[joined]")
                    self.cmd_callers(None)
            else:
                if not line: pass
                elif line[0] != '/':
                    self.handle_talk(line)
                else:
                    self.handle_command(line)

        def handle_talk (self, line):
            for channel in ChatProtocol.channels.keys():
                if channel is not self:
                    channel.write_line("%s: %s" % (self.nick, line))

        def handle_command (self, line):
            command = line.split()
            name = 'cmd_%s' % command[0][1:]
            if hasattr(self, name):
                method = getattr(self, name)
                if callable(method):
                    method(command[1:])
                    return
            self.write_line('unknown command: %s' % command[0])

        def cmd_quit (self, args):
            if args:
                self.handle_talk('[quit] (%s)' % ' '.join(args))
            else:
                self.handle_talk('[quit]')
            self.write_line('goodbye.')
            self.close_when_done()

        cmd_q = cmd_quit

        def cmd_callers (self, args):
            num_channels = len(ChatProtocol.channels)
            if num_channels == 1:
                self.write_line("[You're the only caller]")
            else:
                self.write_line("[There are %d callers]" % (num_channels))
                nicks = [ x.nick or '<unknown>' for x in ChatProtocol.channels.keys() ]
                self.write_data(' ' + '\r\n '.join(nicks) + '\r\n')

    Server(bind_address=('', 8518), protocol=ChatProtocol).activate()
    get_reactor().start()

if __name__ == "__main__":
    __test()

##############################################################################
## THE END
