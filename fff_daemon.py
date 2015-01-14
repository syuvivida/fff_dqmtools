#!/usr/bin/env python

import os
import sys
import logging
import socket
import time
import select, stat, signal, errno, fcntl
from StringIO import StringIO

def close_on_exec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    flags |= fcntl.FD_CLOEXEC
    fcntl.fcntl(fd, fcntl.F_SETFD, flags)

# use a named socket check if we are running
# this is very clean and atomic and leave no files
# from: http://stackoverflow.com/a/7758075
def socket_lock(pname):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        sock.bind('\0' + pname)
        return sock
    except socket.error:
        return None

# same as in fff_deleter.py
def daemon_detach(logfile, pidfile):
    # do the double fork
    pid = os.fork()
    if pid != 0:
        sys.exit(0)

    os.setsid()

    fl = open(logfile, "a")
    os.dup2(fl.fileno(), sys.stdin.fileno())
    os.dup2(fl.fileno(), sys.stdout.fileno())
    os.dup2(fl.fileno(), sys.stderr.fileno())
    fl.close()

    pid = os.fork()
    if pid != 0:
        sys.exit(0)

    if pidfile:
        f = open(pidfile, "w")
        f.write("%d\n" % os.getpid())
        f.close()

def clear_fds():
    try:
        maxfd = os.sysconf("SC_OPEN_MAX")
    except (AttributeError, ValueError):
        maxfd = 1024

    os.closerange(3, maxfd)


# launch a fork, and restarts it if it fails
def daemon_run_supervised(f):
    try:
        f()
    except:
        log = logging.getLogger("root")
        log.warning("Daemon failure, will restart in 15s.:", exc_info=True)

        # wait 30s so we don't restart too often
        time.sleep(1)
        clear_fds()

        args = [sys.executable] + sys.argv
        os.execv(sys.executable, args)

class LogCaptureHandler(logging.StreamHandler):
    def __init__(self):
        self.string_io = StringIO()
        logging.StreamHandler.__init__(self, self.string_io)

    def retrieve(self):
        log_out = self.string_io.getvalue()
        self.string_io.truncate(0)

        return log_out

def daemon_setup_log_capture(root):
    # log to stderr (it might be redirected)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    flog_ch = logging.StreamHandler()
    flog_ch.setLevel(logging.INFO)
    flog_ch.setFormatter(formatter)
    root.addHandler(flog_ch)

    # create log capture handler
    log_capture_handler = LogCaptureHandler()
    log_capture_handler.setLevel(logging.INFO)
    log_capture_handler.setFormatter(formatter)
    root.addHandler(log_capture_handler)

    root.setLevel(logging.INFO)
    return log_capture_handler

if __name__ == "__main__":
    pass
