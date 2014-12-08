#!/usr/bin/env python

import os
import sys
import stat
import logging
import re
import datetime
import subprocess
import socket
import time
import select, signal, errno, fcntl
import json
from StringIO import StringIO

log = logging.getLogger("root")

def prepare_imports():
    # minihack
    sys.path.append('/opt/hltd/python')
    sys.path.append('/opt/hltd/lib')

    global inotify, watcher, es_client

    import _inotify as inotify
    import watcher
    import pyelasticsearch.client as es_client

def close_on_exec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    flags |= fcntl.FD_CLOEXEC
    fcntl.fcntl(fd, fcntl.F_SETFD, flags)

class FileMonitor(object):
    def __init__(self, top_path, log_capture=None, rescan_timeout=30):
        self.path = top_path
        self.rescan_timeout = rescan_timeout
        self.log_capture = log_capture
        self.es = es_client.ElasticSearch("http://127.0.0.1:9200")
        self.index_name = "dqm_online_monitoring"

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


    def recreate_index(self):
        self.delete_index()
        self.create_index()

    def delete_index(self):
        log.info("Deleting index: %s", self.index_name)
        self.es.delete_index(self.index_name)

    def create_index(self):
        log.info("Creating index: %s", self.index_name)

        self.settings = {
            "index":{
                'number_of_shards' : 16,
                'number_of_replicas' : 1
            }
        }

        self.mappings = {
            'dqm-source-state' : {
                'properties' : {
                    'type' : {'type' : 'string' },
                    'pid' : { 'type' : 'integer' },
                    'hostname' : { 'type' : 'string' },
                    'sequence' : { 'type' : 'integer', 'index' : 'no' },
                    'run' : { 'type' : 'integer' },
                    'lumi' : { 'type' : 'integer' },

                    'extra' : { 'type': 'object', 'index' : 'no', 'enabled': False },

                    # should not be used, they are now under 'extra'
                    'ps_info' : { 'type': 'object', 'index' : 'no', 'enabled': False },
                    'lumiSeen' : {  'type': 'object', 'index' : 'no', 'enabled': False },
                    'stderr' : {'type' : 'string', 'index' : 'no', 'enabled': False },

                },
                '_timestamp' : { 'enabled' : True, 'store' : True, },
                '_ttl' : { 'enabled' : True, 'default' : '15d' }
            },
            'dqm-diskspace' : {
                'properties' : {
                    'type' : {'type' : 'string' },
                    'pid' : { 'type' : 'integer' },
                    'hostname' : { 'type' : 'string' },
                    'sequence' : { 'type' : 'integer', 'index' : 'no' },

                    'extra' : { 'type': 'object', 'index' : 'no', 'enabled': False },
                },
                '_timestamp' : { 'enabled' : True, 'store' : True, },
                '_ttl' : { 'enabled' : True, 'default' : '15d' }
            },
            'dqm-stats' : {
                'properties' : {
                    'type' : {'type' : 'string' },
                    'pid' : { 'type' : 'integer' },
                    'hostname' : { 'type' : 'string' },
                    'sequence' : { 'type' : 'integer', 'index' : 'no' },

                    'extra' : { 'type': 'object', 'index' : 'no', 'enabled': False },
                },
                '_timestamp' : { 'enabled' : True, 'store' : True, },
                '_ttl' : { 'enabled' : True, 'default' : '15d' }
            },
        }

        try:
            self.es.create_index(self.index_name, settings={ 'settings': self.settings, 'mappings': self.mappings })
        except es_client.IndexAlreadyExistsError:
            log.info("Index already exists.", exc_info=True)
            pass
        except:
            log.warning("Cannot create index", exc_info=True)

        log.info("Created index: %s", self.index_name)

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

class LogCaptureHandler(logging.StreamHandler):
    def __init__(self):
        self.string_io = StringIO()
        logging.StreamHandler.__init__(self, self.string_io)

    def retrieve(self):
        log_out = self.string_io.getvalue()
        self.string_io.truncate(0)

        return log_out

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

# launch a fork, and restarts it if it fails
def daemon_run_supervised(f):
    try:
        f()
    except:
        log.warning("Daemon failure, will restart in 15s.:", exc_info=True)

        # wait 30s so we don't restart too often
        time.sleep(15)
        args = [sys.executable] + sys.argv
        os.execv(sys.executable, args)

def daemon_setup_logging(root):
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
    do_mode = "daemon"
    do_foreground = False

    if "--foreground" in sys.argv:
        do_foreground = True

    if "--playback" in sys.argv:
        do_mode = "playback"
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

        daemon_detach("/var/log/fff_monitoring.log", "/var/run/fff_monitoring.pid")

        args = [sys.executable] + sys.argv + ["--foreground"]
        os.execv(sys.executable, args)

    log_capture = daemon_setup_logging(log)
    prepare_imports()

    service = FileMonitor(
        top_path = "/tmp/dqm_monitoring/",
        log_capture = log_capture,
    )

    log.info("Service started, pid is %d.", os.getpid())

    if do_mode == "reindex":
        service.recreate_index()
    elif do_mode == "playback":
        service.run_playback(sys.argv[2])
    else:
        daemon_run_supervised(service.run_daemon)
