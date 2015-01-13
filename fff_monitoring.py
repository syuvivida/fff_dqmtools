#!/usr/bin/env python

import os
import sys

def prepare_imports():
    # minihack
    sys.path.append('/opt/hltd/python')
    sys.path.append('/opt/hltd/lib')

    thp = os.path.dirname(__file__)
    sys.path.append(os.path.join(thp, "./"))
    sys.path.append(os.path.join(thp, "./lib"))

prepare_imports()

import stat
import logging
import re
import datetime
import subprocess
import socket
import time
import select, signal, errno, fcntl
import json
import sqlite3
from StringIO import StringIO

log = logging.getLogger("root")

class FileMonitor(object):
    def __init__(self, top_path, log_capture=None, rescan_timeout=30):
        self.path = top_path
        self.rescan_timeout = rescan_timeout
        self.log_capture = log_capture

        hostname = socket.gethostname()

        self.self_sequence = 0
        self.self_doc = {
            "test_timestamp": time.time(),
            "sequence": 0,
            "hostname": hostname,
            "extra": {},
            "pid": os.getpid(),
            "_id": "dqm-stats-%s" % hostname,
            "type": "dqm-stats"
        }

        try:
            os.makedirs(self.path)
        except OSError:
            pass

        try:
            os.chmod(self.path,
                stat.S_IRWXU |  stat.S_IRWXG | stat.S_IRWXO | stat.S_ISVTX)
        except OSError:
            pass

    def test():
        conn = sqlite3.connect("mydb.db")

        cur.execute("DROP TABLE IF EXISTS Monitoring")
        cur.execute("""
        CREATE TABLE Monitoring (
            id TEXT PRIMARY KEY NOT NULL,
            type TEXT,
            runkey TEXT,
            body TEXT
        )""")

        cur.execute("CREATE INDEX M_type_index ON Monitoring (type)")


    def upload_file(self, fp, preprocess=None):
        log.info("Uploading: %s", fp)

        try:
            f = open(fp, "r")
            document = json.load(f)
            f.close()

            if preprocess:
                document = preprocess(document)

            ret = self.es.index(self.index_name, document["type"], document, id=document["_id"])
            return True
        except:
            log.warning("Failure to upload the document: %s", fp, exc_info=True)
            #raise Exception("Please restart.")
            return False

    def process_file(self, fp):
        fname = os.path.basename(fp)

        if fname.startswith("."):
            return

        if not fname.endswith(".jsn"):
            return

        ret = self.upload_file(fp)
        #if ret:
        os.unlink(fp)

    def process_dir(self):
        for f in os.listdir(self.path):
            fp = os.path.join(self.path, f)
            self.process_file(fp)

    def make_selftest(self):
        doc = self.self_doc

        if (time.time() - doc["test_timestamp"]) < 30:
            return

        self.self_sequence += 1
        doc["test_timestamp"] = time.time()
        doc["sequence"] = self.self_sequence
        doc["pid"] = os.getpid()

        if self.log_capture is not None:
            log = self.log_capture.retrieve()
            doc["extra"]["stdlog"] = log.strip().split("\n")

        meminfo = list(open("/proc/meminfo", "r").readlines())
        def entry_to_dict(line):
            key, value = line.split()[:2]
            value = int(value)
            return (key.strip(":"), value, )
        meminfo = dict(map(entry_to_dict, meminfo))
        doc["extra"]["meminfo"] = meminfo

        p = subprocess.Popen(["df", "-hP"], stdout=subprocess.PIPE)
        doc["extra"]["df"] = p.communicate()[0]
        del p

        doc["memory_used"] = (meminfo["MemTotal"] - meminfo["MemFree"]) * 1024
        doc["memory_free"] = meminfo["MemFree"] * 1024
        doc["memory_total"] = meminfo["MemTotal"] * 1024

        final_fp = os.path.join(self.path, doc["_id"] + ".jsn")
        tmp_fp = final_fp + ".tmp"

        fd = open(tmp_fp, "w")
        json.dump(doc, fd, indent=True)
        fd.write("\n")
        fd.close()

        os.rename(tmp_fp, final_fp)

    def wait_n_run(self):
        import _inotify as inotify
        import watcher

        mask = inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO
        w = watcher.Watcher()

        # by default inotify is openned without FD_CLOSEXEC
        # We use execve to restart and can run out of inotify instances.
        close_on_exec(w.fileno())

        w.add(self.path, mask)

        poll = select.poll()
        poll.register(w, select.POLLIN)

        while True:
            poll.poll(self.rescan_timeout*1000)

            # clear the events
            # this sometimes fails due to a bug in inotify
            for event in w.read(bufsize=0):
                pass

            self.process_dir()

            # make report
            self.make_selftest()


    def run_daemon(self):
        self.process_dir()

        while True:
            try:
                service.wait_n_run()
            except select.error as e:
                if e[0] == errno.EINTR:
                    break

class WebServer(object):
    pass

if __name__ == "__main__":
    import daemon

    do_mode = "daemon"
    do_foreground = False

    if "--foreground" in sys.argv:
        do_foreground = True

    if "--reindex" in sys.argv:
        do_mode = "reindex"
        do_foreground = True

    if not do_foreground:
        # try to take the lock or quit
        sock = socket_lock("fff_monitoring")
        if sock is None:
            sys.stderr.write("Already running, exitting.\n")
            sys.stderr.flush()
            sys.exit(1)

        daemon.daemon_detach("/var/log/fff_monitoring.log", "/var/run/fff_monitoring.pid")

        args = [sys.executable] + sys.argv + ["--foreground"]
        os.execv(sys.executable, args)

    log_capture = daemon.daemon_setup_log_capture(log)

    service = FileMonitor(
        top_path = "/tmp/dqm_monitoring/",
        log_capture = log_capture,
    )

    log.info("Service started, pid is %d.", os.getpid())

    if do_mode == "reindex":
        service.recreate_index()
    else:
        daemon.daemon_run_supervised(service.run_daemon)
