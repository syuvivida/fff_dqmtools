#!/usr/bin/env python
import sys
import os
import logging
import signal, errno, fcntl

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

def run_daemon(opt):
    import gevent
    import fff_web
    import fff_filemonitor

    fweb = fff_web.WebServer(db=opt["db"])

    fmon = fff_filemonitor.FileMonitor(
        path = opt["path"],
        web_process = fweb,
    )

    fm = gevent.spawn(lambda: fmon.run_greenlet())
    fw = gevent.spawn(lambda: fweb.run_greenlet(port=opt["port"]))

    try:
        gevent.joinall([fm, fw], raise_error=True)
    except KeyboardInterrupt:
        return

if __name__ == "__main__":
    import fff_daemon

    opt = {
        'do_foreground': False,
        'db': "/var/lib/fff_dqmtools/db.sqlite3",
        'path': "/tmp/dqm_monitoring/",
        'port': 9215,
    }

    arg = sys.argv[1:]
    while arg:
        a = arg.pop(0)
        if a == "--foreground":
            opt["do_foreground"] = True
            continue

        if a == "--db":
            opt["db"] = arg.pop(0)
            continue

        if a == "--port":
            opt["port"] = int(arg.pop(0))
            continue

        if a == "--path":
            opt["path"] = arg.pop(0)
            continue

        sys.stderr.write("Invalid parameter: %s." % a);
        sys.stderr.flush()
        sys.exit(1)

    if not opt["do_foreground"]:
        # try to take the lock or quit
        sock = fff_daemon.socket_lock("fff_monitoring")
        if sock is None:
            sys.stderr.write("Already running, exitting.\n")
            sys.stderr.flush()
            sys.exit(1)

        fff_daemon.daemon_detach("/var/log/fff_monitoring.log", "/var/run/fff_monitoring.pid")

        args = [sys.executable] + sys.argv + ["--foreground"]
        os.execv(sys.executable, args)

    log_capture = fff_daemon.daemon_setup_log_capture(log)

    log.info("Service started, pid is %d.", os.getpid())
    fff_daemon.daemon_run_supervised(lambda: run_daemon(opt))
