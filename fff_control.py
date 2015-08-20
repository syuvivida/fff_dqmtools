# Provides control socket for fff_dqmtools' applets.
# This socket is already used for locking, this just listens to it.

# Must exist after lock wrapper:
## @fff_dqmtools.lock_wrapper
## @fff_control.enable_control_socket()
## def __run__(opts, **kwargs):
##     pass

# And the applet must not block (ie use gevent sleeps and gevent selects)

# Connectable via socat, eg. socat - ABSTRACT-CONNECT:bd68c7bb.fff_filemonitor

import gevent
import struct

class Ctrl(object):
    def __init__(self, log, sock, lkey):
        self.log = log
        self.sock = sock
        self.lkey = lkey

    def handle_line(self, line, write_f):
        # override me
        line = line.strip()

        if line == "status":
            write_f("ok\n")
        else:
            write_f("r: %s\n" % line)
        pass

    def handle_conn(self, cli_sock):
        # this function runs in separate greenlet
        f = None
        try:
            self.log.info("Accepted control connection: %s", cli_sock)

            f = cli_sock.makefile("rw")
            def write_f(data):
                f.write(data)
                f.flush()

            while True:
                l = f.readline()
                if not l:
                    break

                self.handle_line(l, write_f)

            self.log.info("Closed control connection: %s", cli_sock)
        finally:
            if f:
                f.close()
            cli_sock.close()


    def run_greenlet(self):
        self.sock.listen(15)
        self.log.info("Control socket open: %s / %s", self.sock, self.lkey)

        while True:
            cli, addr = self.sock.accept()
            gevent.spawn(self.handle_conn, cli)

def enable_control_socket(ctrl_class=Ctrl):
    """ This is decorator for function.
    """

    def wrapper(actual_f, *args, **kwargs):
        log = kwargs["logger"]

        try:
            sock = kwargs["lock_socket"]
            lkey = kwargs["lock_key"]
        except:
            if log:
                log.warning("Couldn't find a 'lock' socket, make sure fff_dqmtools.lock_wrapper() is enabled.", exc_info=True)
            raise

        ctrl = Ctrl(log, sock, lkey)
        kwargs["control_handler"] = ctrl

        actual_t = gevent.spawn(actual_f, *args, **kwargs)
        control_t = gevent.spawn(ctrl.run_greenlet)
        # do not wait for control_t, it runs forever
        gevent.joinall([actual_t, ], raise_error=True)

    from functools import partial
    def wrapping_func(actual_f):
        return partial(wrapper, actual_f)
    return wrapping_func

if __name__ == "__main__":
    import sys
    from fff_dqmtools import get_lock_key
    from gevent import socket

    if len(sys.argv) != 3:
        sys.stderr.write("Usage: %s applet_name command\n")
        sys.exit(-1)

    lkey = get_lock_key(sys.argv[1])
    cmd = sys.argv[2]

    #sys.stderr.write("Using socket: %s\n" % lkey)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("\0" + lkey)

    sock.sendall(cmd + "\n")
    sock.shutdown(socket.SHUT_WR)

    while True:
        data = sock.recv(4096)
        if len(data) == 0:
            break

        sys.stdout.write(data)
        sys.stdout.flush()

    sock.close()
