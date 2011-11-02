# vim:set ts=4 sw=4 et nowrap syntax=python ff=unix:
#
# $Id: utility.py 8613 2011-07-15 20:33:36Z markc $
#

import errno, grp, logging, os, pwd, resource, signal, socket, sys, tempfile

##############################################################################

def close_fd (fd):
    try:
        os.close(fd)
    except OSError:
        pass

##############################################################################

def daemonize ():
    pid = os.fork()
    if pid != 0:
        # Parent
        os._exit(0)

    # Child
    os.close(0)
    sys.stdin = sys.__stdin__ = open("/dev/null")
    os.close(1)
    sys.stdout = sys.__stdout__ = open("/dev/null", "w")
    os.close(2)
    sys.stderr = sys.__stderr__ = open("/dev/null", "w")
    os.setsid()

##############################################################################

def decode_wait_status (sts):
    """
    Decode the status returned by wait() or waitpid()
    """
    if os.WIFEXITED(sts):
        es = os.WEXITSTATUS(sts) & 0xffff
        msg = "exit status %s" % es
        return es, msg
    elif os.WIFSIGNALED(sts):
        sig = os.WTERMSIG(sts)
        msg = "terminated by %s" % signame(sig)
        if hasattr(os, "WCOREDUMP"):
            iscore = os.WCOREDUMP(sts)
        else:
            iscore = sts & 0x80
        if iscore:
            msg += " (core dumped)"
        return -1, msg
    else:
        msg = "unknown termination cause 0x%04x" % sts
        return -1, msg

##############################################################################

_signames = None

def signame (sig):
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

def signum (sig):
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

##############################################################################

def waitpid ():
    # need pthread_sigmask here to avoid concurrent sigchld, but Python doesn't
    # offer it as its not standard across UNIX versions. There is still a race
    # condition here; we can get a sigchld while we're sitting in the waitpid
    # call
    try:
        pid, sts = os.waitpid(-1, os.WNOHANG)
    except OSError, why:
        err = why[0]
        if err not in (errno.ECHILD, errno.EINTR):
            logging.critical("waitpid error; a process may not be cleaned up properly")
        if err == errno.EINTR:
            logging.debug("EINTR during reap")
        pid, sts = None, None
    return pid, sts

##############################################################################

def calculate_hostname ():
    name = socket.gethostname()
    try:
        hostname, aliases, ipaddrs = socket.gethostbyaddr(name)
    except socket.error:
        pass
    else:
        aliases.insert(0, hostname)
        for name in aliases:
            if '.' and name and not name.startswith('localhost'):
                break
        else:
            name = hostname
    return name

##############################################################################
## THE END
