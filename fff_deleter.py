#!/usr/bin/env python

import sys
import os
import logging
import re
import datetime
import subprocess
import socket
import time
import json
from collections import OrderedDict

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

re_files = re.compile(r"^run(?P<run>\d+)/run(?P<runf>\d+)_ls(?P<ls>\d+)(?P<leftover>_.+\.(dat|raw|pb))(\.deleted){0,1}$")
def parse_file_name(rl):
    m = re_files.match(rl)
    if not m:
        return None

    d = m.groupdict()
    sort_key = (int(d["run"]), int(d["runf"]), int(d["ls"]), d["leftover"])
    return sort_key


def collect(top):
    # entry format (sort_key, path, size)
    collected = []

    for root, dirs, files in os.walk(top, topdown=True):
        root_rl = os.path.relpath(root, top)

        # skip hidden stuff, usually ".snapshots"
        files = [f for f in files if not f.startswith(".")]
        dirs[:] = [d for d in dirs if not d.startswith(".")]

        # don't recurse into "deleted" runs
        dirs[:] = [d for d in dirs if not (d.startswith("run") and ".deleted" in d)]

        for name in files:
            fp = os.path.join(root, name)
            rl = os.path.join(root_rl, name)

            sort_key = parse_file_name(rl)
            if sort_key:
                stat = os.stat(fp)
                fsize = stat.st_size
                ftime = stat.st_mtime
                if fsize == 0:
                    continue

                sort_key = parse_file_name(rl)
                collected.append((sort_key, fp, fsize, ftime, ))

    # for now just use simple sort
    collected.sort(key=lambda x: x[0])
    return collected


class FileDeleter(object):
    def __init__(self, top, thresholds, report_directory, fake=True, log_capture=None, app_tag="fff_deleter"):
        self.top = top
        self.fake = fake
        self.thresholds = thresholds
        self.report_directory = report_directory
        self.sequence = 0
        self.log_capture = log_capture
        self.app_tag = app_tag

        self.hostname = socket.gethostname()

        if self.fake:
            log.info("Starting in fake (read only) mode.")

    def rename(self, f):
        if f.endswith(".deleted"):
            return f

        fn = f + ".deleted"

        if self.fake:
            log.warning("Renaming file (fake): %s -> %s", f,
                os.path.relpath(fn, os.path.dirname(f)))
        else:
            log.warning("Renaming file: %s -> %s", f,
                os.path.relpath(fn, os.path.dirname(f)))

            os.rename(f, fn)

        return fn

    def delete(self, f):
        if not f.endswith(".deleted"):
            return f

        if self.fake:
            log.warning("Truncating file (fake): %s", f)
        else:
            log.warning("Truncating file: %s", f)
            open(f, "w").close()

        return f

    def calculate_threshold(self, type_string):
        """ Calculates how much bytes we have to delete
            in order to reach the threshold percentange.

            If the threshold is set to 80% and the disk
            at 89%, it fill return 9% in bytes.
        """

        threshold = self.thresholds[type_string]
        st = os.statvfs(self.top)
        total = st.f_blocks * st.f_frsize
        used = total - (st.f_bavail * st.f_frsize)
        stopSize = used - float(total * threshold) / 100

        log.info("Using %d (%.02f%%) of %d space, %d (%.02f%%) above %s threshold.",
            used, float(used) * 100 / total, total, stopSize, float(stopSize) * 100 / total, type_string)

        return stopSize

    def do_the_cleanup(self):
        self.sequence += 1
        if not os.path.isdir(self.top):
            log.warning("Directory %s does not exists.", self.top)
            return

        stopSizeRename = self.calculate_threshold("rename")
        stopSizeDelete = self.calculate_threshold("delete")

        assert stopSizeRename > stopSizeDelete

        # do the action until we reach the target sizd
        log.info("Started file collection at %s", self.top)
        start = time.time()
        collected = collect(self.top)
        log.info("Done file collection, took %.03fs.", time.time() - start)

        updated = []

        while collected:
            sort_key, fp, fsize, ftime = collected.pop(0)

            if stopSizeRename > 0:
                stopSizeRename -= fsize

                if fp.endswith(".deleted") and stopSizeDelete > 0:
                    # delete the files which have been previously marked
                    # and we have disk over-usage
                    stopSizeDelete -= fsize
                    new_fp = self.delete(fp)
                elif fp.endswith(".deleted"):
                    new_fp = fp
                else:
                    new_fp = self.rename(fp)

                updated.append((sort_key, new_fp, fsize, ftime, ))
            else:
                updated.append((sort_key, fp, fsize, ftime, ))
                updated += collected
                break

            # stopSizeDelete can still be positive after this
            # meaning some files have to be deleted, but have not been (only marked)
            # these files will be deleted next iteration (30s.)

        return updated


    def make_content_report(self, collected):
        streams = {}

        for sort_key, fp, fsize, ftime in collected:
            run, run_f, ls, leftover = sort_key
            key = (run, leftover.strip("_"), )

            entry = "%d %d %f:" % (
                ls,
                fsize,
                ftime
            )

            if fp.endswith(".deleted"):
                entry += " deleted"

            if not streams.has_key(key):
                streams[key] = []
            streams[key].append(entry)

        keys = list(streams.keys())
        keys.sort()
        runs = OrderedDict({})

        for key in keys:
            run, stream = key
            stream_files = streams[key]

            if not runs.has_key(run):
                runs[run] = {}
            runs[run][stream] = stream_files

        return runs

    def make_report(self, files, logout):
        if not os.path.isdir(self.report_directory):
            log.warning("Directory %s does not exists. Reports disabled.", self.report_directory)
            return

        meminfo = list(open("/proc/meminfo", "r").readlines())
        def entry_to_dict(line):
            key, value = line.split()[:2]
            value = int(value)
            return (key.strip(":"), value, )
        meminfo = dict(map(entry_to_dict, meminfo))

        # calculate the disk usage
        if os.path.isdir(self.top):
            st = os.statvfs(self.top)
            total = st.f_blocks * st.f_frsize
            free = st.f_bavail * st.f_frsize
            used = total - free
        else:
            used, free, total = -1, -1, -1

        if files:
            collected = self.make_content_report(files)
        else:
            collected = None

        doc = {
            "sequence": self.sequence,
            "memory_used": (meminfo["MemTotal"] - meminfo["MemFree"]) * 1024,
            "memory_free": meminfo["MemFree"] * 1024,
            "memory_total": meminfo["MemTotal"] * 1024,
            "disk_used": used,
            "disk_free": free,
            "disk_total": total,
            "hostname": self.hostname,
            "tag": self.app_tag,
            "extra": {
                "meminfo": meminfo,
                "stdlog": logout,
                "files_seen": collected,
            },
            "pid": os.getpid(),
            "_id": "dqm-diskspace-%s-%s" % (self.hostname, self.app_tag, ),
            "type": "dqm-diskspace"
        }

        fn = doc["_id"]
        tmp_fp = os.path.join(self.report_directory, "." + fn + ".tmp")
        final_fp = os.path.join(self.report_directory, fn + ".jsn")
        fd = open(tmp_fp, "w")

        json.dump(doc, fd, indent=True)
        fd.write("\n")
        fd.close()

        os.rename(tmp_fp, final_fp)
        log.info("Made report file: %s", final_fp)


    def run_daemon(self):
        delay_seconds = 30
        while True:
            files = self.do_the_cleanup()

            log_out = None
            if self.log_capture is not None:
                log = self.log_capture.retrieve()
                log_out = log.strip().split("\n")

            self.make_report(files, log_out)
            time.sleep(delay_seconds)

import sys, os
if __name__ == "__main__":
    import fff_daemon

    opt = {}
    # application tag, used in locking/logging
    opt["app_tag"] = "fff_deleter"
    opt["top"] = "/fff/ramdisk"
    opt["do_foreground"] = False
    opt["fake_deletes"] = False

    arg = sys.argv[1:]
    while arg:
        a = arg.pop(0)
        if a == "--foreground":
            opt["do_foreground"] = True
            continue

        if a == "--fake":
            opt["fake_deletes"] = True
            continue

        if a == "--tag":
            opt["app_tag"] = arg.pop(0)
            continue

        if a == "--top":
            opt["top"] = arg.pop(0)
            continue

        sys.stderr.write("Invalid parameter: %s." % a);
        sys.stderr.flush()
        sys.exit(1)

    if not opt["do_foreground"]:
        # try to take the lock or quit
        sock = fff_daemon.socket_lock(opt["app_tag"])
        if sock is None:
            sys.stderr.write("Already running, tag was %s, exitting.\n" % opt["app_tag"])
            sys.stderr.flush()
            sys.exit(1)

        fff_daemon.daemon_detach(
            "/var/log/%s.log" % opt["app_tag"],
            "/var/run/%s.pid" % opt["app_tag"])

        args = [sys.executable] + sys.argv + ["--foreground"]
        os.execv(sys.executable, args)

    log_capture = fff_daemon.daemon_setup_log_capture(log)

    # thresholds rename and delete must be in order
    # in other words, always: delete > rename
    # this is because delete only deletes renamed files

    service = FileDeleter(
        top = opt["top"],
        app_tag = opt["app_tag"],
        thresholds = {
            'rename': 60,
            'delete': 80,
        },
        report_directory = "/tmp/dqm_monitoring/",
        fake = opt["fake_deletes"],
        log_capture = log_capture,
    )

    log.info("Service started, pid is %d.", os.getpid())
    fff_daemon.daemon_run_supervised(service.run_daemon)
