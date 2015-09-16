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

import fff_dqmtools
import fff_filemonitor
import fff_cluster

re_files = re.compile(r"^run(?P<run>\d+)/(open\/){0,1}run(?P<runf>\d+)_ls(?P<ls>\d+)(?P<leftover>_.+\.(dat|raw|pb))(\.deleted){0,1}$")
def parse_file_name(rl):
    m = re_files.match(rl)
    if not m:
        return None

    d = m.groupdict()
    sort_key = (int(d["run"]), int(d["runf"]), int(d["ls"]), d["leftover"])
    return sort_key

def collect(top, parse_func):
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

            sort_key = parse_func(rl)
            if sort_key:
                stat = os.stat(fp)
                fsize = stat.st_size
                ftime = stat.st_mtime
                if fsize == 0:
                    continue

                collected.append((sort_key, fp, fsize, ftime, ))

    # for now just use simple sort
    collected.sort(key=lambda x: x[0])
    return collected

class FileDeleter(object):
    def __init__(self, top, thresholds, report_directory, log, fake=True, app_tag="fff_deleter"):
        self.top = top
        self.fake = fake
        self.thresholds = thresholds
        self.report_directory = report_directory
        self.sequence = 0
        self.app_tag = app_tag
        self.log = log
        self.delay_seconds = 30

        self.hostname = socket.gethostname()

        if self.fake:
            self.log.info("Starting in fake (read only) mode.")

    def rename(self, f):
        if f.endswith(".deleted"):
            return f

        fn = f + ".deleted"

        if self.fake:
            self.log.warning("Renaming file (fake): %s -> %s", f,
                os.path.relpath(fn, os.path.dirname(f)))
        else:
            self.log.warning("Renaming file: %s -> %s", f,
                os.path.relpath(fn, os.path.dirname(f)))

            os.rename(f, fn)

        return fn

    def delete(self, f):
        if not f.endswith(".deleted"):
            return f

        if self.fake:
            self.log.warning("Truncating file (fake): %s", f)
        else:
            self.log.warning("Truncating file: %s", f)
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

        self.log.info("Using %d (%.02f%%) of %d space, %d (%.02f%%) above %s threshold.",
            used, float(used) * 100 / total, total, stopSize, float(stopSize) * 100 / total, type_string)

        return stopSize

    def do_the_cleanup(self):
        self.sequence += 1
        if not os.path.isdir(self.top):
            self.log.warning("Directory %s does not exists.", self.top)
            return

        stopSizeRename = self.calculate_threshold("rename")
        stopSizeDelete = self.calculate_threshold("delete")

        assert stopSizeRename > stopSizeDelete

        # do the action until we reach the target sizd
        self.log.info("Started file collection at %s", self.top)
        start = time.time()
        collected = collect(self.top, parse_file_name)
        self.log.info("Done file collection, took %.03fs.", time.time() - start)

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

    def make_report(self, files):
        if not os.path.isdir(self.report_directory):
            self.log.warning("Directory %s does not exists. Reports disabled.", self.report_directory)
            return

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
            "disk_used": used,
            "disk_free": free,
            "disk_total": total,
            "hostname": self.hostname,
            "tag": self.app_tag,
            "extra": {
                "files_seen": collected,
            },
            "pid": os.getpid(),
            "_id": "dqm-diskspace-%s-%s" % (self.hostname, self.app_tag, ),
            "type": "dqm-diskspace"
        }

        final_fp = os.path.join(self.report_directory, doc["_id"] + ".jsn")
        body = json.dumps(doc, indent=True)
        fff_filemonitor.atomic_create_write(final_fp, body)

        self.log.info("Made report file: %s", final_fp)

    def run_greenlet(self):
        import gevent

        while True:
            files = self.do_the_cleanup()

            self.make_report(files)
            gevent.sleep(self.delay_seconds)

## Applet code is no longer used, but serves as an example.
## Actual deleters applets should import this module

## @fff_cluster.host_wrapper(allow = ["bu-c2f13-31-01"])
## @fff_dqmtools.fork_wrapper(__name__)
## @fff_dqmtools.lock_wrapper
## def __run__(opts, **kwargs):
##     log = kwargs["logger"]
##
##     service = FileDeleter(
##         top = opts["deleter.ramdisk"],
##         app_tag = opts["deleter.tag"],
##         thresholds = {
##             'rename': 60,
##             'delete': 80,
##         },
##         log = log,
##         report_directory = opts["path"],
##         fake = opts["deleter.fake"],
##     )
##
##     service.run_greenlet()
