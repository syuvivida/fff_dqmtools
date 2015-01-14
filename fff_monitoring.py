#!/usr/bin/env python

import os
import sys

def prepare_imports():
    # minihack
    sys.path.append('/opt/hltd/python')
    sys.path.append('/opt/hltd/lib')

    sys.path.append('./env/lib/python2.7/site-packages/inotify/')

    thp = os.path.dirname(__file__)
    sys.path.append(os.path.join(thp, "./"))
    sys.path.append(os.path.join(thp, "./lib"))

prepare_imports()

import logging
import select, signal, errno, fcntl

log = logging.getLogger("root")

def run_daemon():
    import fff_web
    import fff_filemonitor

    fweb = fff_web.WebServer()

    fmon = fff_filemonitor.FileMonitor(
        path = "/tmp/dqm_monitoring/",
        web_process = fweb,
    )

    fmon_fd = fmon.create_watcher()
    fweb_fd = fweb.create_wsgi_server()

    poll = select.poll()
    poll.register(fmon_fd, select.POLLIN)
    poll.register(fweb_fd, select.POLLIN)

    try:
        while True:
            ready = poll.poll(30*1000)
            if not ready:
                # timeout actions
                pass

            for fd, event in ready:
                if fd == fmon_fd.fileno():
                    fmon.handle_watcher(fmon_fd)

                if fd == fweb_fd.fileno():
                    fweb.handle_request(fweb_fd)

    except select.error as e:
        if e[0] == errno.EINTR:
            return

        raise
    except KeyboardInterrupt:
        return

if __name__ == "__main__":
    import fff_daemon

    do_mode = "daemon"
    do_foreground = False

    if "--foreground" in sys.argv:
        do_foreground = True

    if "--reindex" in sys.argv:
        do_mode = "reindex"
        do_foreground = True

    if not do_foreground:
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

    if do_mode == "daemon":
        fff_daemon.daemon_run_supervised(run_daemon)
    #elif do_mode == "reindex":
    #    service.recreate_index()
    else:
        raise Exception("Unknown action.")
