#!/usr/bin/env python

import sys, os, time
import logging
from StringIO import StringIO

def prepare_imports():
    # minihack
    sys.path.append('/opt/hltd/python')
    sys.path.append('/opt/hltd/lib')

    sys.path.append('./env/lib/python2.7/site-packages/inotify/')

    thp = os.path.dirname(__file__)
    sys.path.append(os.path.join(thp, "./"))
    sys.path.append(os.path.join(thp, "./lib"))

prepare_imports()

log = logging.getLogger("root")
log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

class LogCaptureHandler(logging.StreamHandler):
    def __init__(self):
        self.string_io = StringIO()
        logging.StreamHandler.__init__(self, self.string_io)

        self.setLevel(logging.INFO)
        self.setFormatter(log_format)

    def retrieve(self):
        log_out = self.string_io.getvalue()
        self.string_io.truncate(0)

        return log_out

    def emit(self, record):
        logging.StreamHandler.emit(self, record)

        s = self.string_io.tell()
        # we should never reach more than a meg of output
        # unless retrieve isn't called
        if s > (1024*1024*16):
            self.string_io.truncate(0)
            self.string_io.write("\n\n... log was truncated ...\n\n")

class StderrHandler(logging.StreamHandler):
    def __init__(self):
        logging.StreamHandler.__init__(self)

        self.setLevel(logging.INFO)
        self.setFormatter(log_format)

class Server(object):
    def __init__(self, opt):
        self.running = {}
        self.opts = opt

        self.log_handler_capture = LogCaptureHandler()
        self.log_handler_stderr = StderrHandler()

        self.log_capture = self.log_handler_capture

    def get_instance(self, name):
        return self.running.get(name, None)

    def start_applet(self, name):
        # spawn the logger with the same name
        alog = logging.getLogger(name)
        self.config_log(alog)

        mod = __import__(name)
        r = mod.__run__(self, self.opts)

        self.running[name] = r

    def joinall(self):
        import gevent

        greenlets = map(lambda x: x[0], self.running.values())

        try:
            gevent.joinall(greenlets, raise_error=True)
        except KeyboardInterrupt:
            return

    def config_log(self, logger):
        logger.addHandler(self.log_handler_capture)
        logger.addHandler(self.log_handler_stderr)
        logger.setLevel(logging.INFO)

    def run(self):
        applets = self.opts["applets"]
        log.info("Found applets: %s", applets)

        for applet in applets:
            self.start_applet(applet)

        srv.joinall()

def detach(logfile, pidfile):
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

def run_supervised(f):
    def clear_fds():
        try:
            maxfd = os.sysconf("SC_OPEN_MAX")
        except (AttributeError, ValueError):
            maxfd = 1024

        os.closerange(3, maxfd)

    try:
        f()
    except:
        log = logging.getLogger("root")
        log.warning("Daemon failure, will restart in 15s.:", exc_info=True)

        # wait 30s so we don't restart too often
        time.sleep(15)
        clear_fds()

        args = [sys.executable] + sys.argv
        os.execv(sys.executable, args)


if __name__ == "__main__":
    opt = {
        'do_foreground': False,
        'path': "/tmp/dqm_monitoring/",
        'applets': ["fff_web", "fff_filemonitor", "fff_selftest", "fff_logcleaner"],

        "logfile": "/var/log/fff_dqmtools.log",
        "pidfile": "/var/run/fff_dqmtools.pid",

        "web.db": "/var/lib/fff_dqmtools/db.sqlite3",
        "web.port": 9215,

        "deleter.ramdisk": "/fff/ramdisk/",
        "deleter.tag": "fff_deleter",
        "deleter.fake": False,
    }

    key_types = {
        "logfile": str,
        "pidfile": str,

        "web.port": int,
        "web.db": str,

        "deleter.ramdisk": str,
        "deleter.tag": str,
    }

    import fff_cluster

    c = fff_cluster.get_node()
    log.info("Found node: %s", c.get("node", "unknown"))

    arg = sys.argv[1:]
    while arg:
        a = arg.pop(0)
        if a == "--foreground":
            opt["do_foreground"] = True
            continue

        if a == "--path":
            opt["path"] = arg.pop(0)
            continue

        if a == "--applets":
            opt["applets"] = arg.pop(0).split(",")
            continue

        if a.startswith("--") and key_types.has_key(a[2:]):
            key = a[2:]
            t = key_types[key]
            opt[key] = t(arg.pop(0))
            continue

        sys.stderr.write("Invalid parameter: %s.\n" % a);
        sys.stderr.flush()
        sys.exit(1)

    if not opt["do_foreground"]:
        detach(opt["logfile"], opt["pidfile"])

        args = [sys.executable] + sys.argv + ["--foreground"]
        os.execv(sys.executable, args)

    srv = Server(opt)
    srv.config_log(log)

    log.info("Service started, pid is %d.", os.getpid())
    run_supervised(srv.run)
