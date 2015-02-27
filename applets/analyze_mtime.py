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

from collections import OrderedDict, namedtuple

import fff_dqmtools
import fff_filemonitor
import fff_deleter
import fff_cluster

log = logging.getLogger(__name__)

RunEntry = namedtuple('RunEntry', ["run", "path", "start_time"])
FileEntry = namedtuple('FileEntry', ["ls", "stream", "mtime"])

def find_match(re, iter):
    xm = map(re.match, iter)
    return filter(lambda x: x is not None, xm)

def collect_run_timestamps(path):
    lst = os.listdir(path)

    path_dct = {}
    path_pattern_re = re.compile(r"^run(\d+)$")
    for m in find_match(path_pattern_re,  lst):
        path_dct[int(m.group(1))] = os.path.join(path, m.group(0))

    dct = []
    pattern_re = re.compile(r"^\.run(\d+)\.global$")
    for m in find_match(pattern_re, lst):
        run = int(m.group(1))
        if not path_dct.has_key(run):
            # no run folder for it yet
            continue

        f = os.path.join(path, m.group(0))
        stat = os.stat(f)
        ftime = stat.st_mtime

        dct.append(RunEntry(run, path_dct[run], ftime))

    dct.sort()
    return dct

def analyze_run_entry(e):
    lst = os.listdir(e.path)

    re_jsn = re.compile(r"^run(?P<run>\d+)_ls(?P<ls>\d+)(?P<leftover>_.+\.jsn)$")
    files = []
    for m in find_match(re_jsn, lst):
        d = m.groupdict()
        if int(d['run']) != e.run: continue

        f = os.path.join(e.path, m.group(0))

        stream = d["leftover"].strip("_")
        stream = re.sub(r".jsn$", r"", stream)
        mtime = os.stat(f).st_mtime

        files.append(FileEntry(int(d['ls']), stream, mtime))

    files.sort()
    return files


class Analyzer(object):
    def __init__(self, top, report_directory, app_tag):
        self.top = top
        self.report_directory = report_directory
        self.app_tag = app_tag
        self.hostname = socket.gethostname()

    def make_report(self, backlog=5):
        timestamps = collect_run_timestamps(self.top)

        # only last 5 entries
        for entry in timestamps[-backlog:]:
            files = analyze_run_entry(entry)

            # group by stream name in order to save space
            # and make a dictionary for json
            grouped = {}
            for f in files:
                if "EoR" in f.stream:
                    # don't include EoR file
                    continue

                lst = grouped.setdefault(f.stream, {
                    'lumis': [],
                    'mtimes': [],
                })

                lst['lumis'].append(f.ls)
                lst['mtimes'].append(f.mtime)

            id = "dqm-timestamps-%s-%s-run%d" % (self.hostname, self.app_tag, entry.run)

            doc = {
                "sequence": 0,
                "hostname": self.hostname,
                "tag": self.app_tag,
                "run": entry.run,
                "extra": {
                    "streams": grouped,
                    "global_start": entry.start_time,
                    #"run_timestamps": run_dct,
                },
                "pid": os.getpid(),
                "_id": id,
                "type": "dqm-timestamps"
            }

            final_fp = os.path.join(self.report_directory, doc["_id"] + ".jsn")
            body = json.dumps(doc, indent=None)
            fff_filemonitor.atomic_create_write(final_fp, body)

            log.info("Made report file: %s", final_fp)

    def run_greenlet(self):
        while True:
            if os.path.isdir(self.report_directory):
                self.make_report()
            else:
                log.warning("Directory %s does not exists. Reports disabled.", self.report_directory)

            time.sleep(105)

@fff_cluster.host_wrapper(allow = ["bu-c2f13-31-01"])
@fff_dqmtools.fork_wrapper(__name__, uid="dqmpro", gid="dqmpro")
@fff_dqmtools.lock_wrapper
def __run__(opts, logger, **kwargs):
    global log
    log = logger

    s = Analyzer(
        top = "/fff/ramdisk/",
        app_tag = kwargs["name"],
        report_directory = opts["path"],
    )

    s.run_greenlet()

if __name__ == "__main__":
    log = fff_dqmtools.LogCaptureHandler.create_logger_subprocess("root")

    s = Analyzer(
        top = "/fff/ramdisk/",
        app_tag = "analyze_mtime",
        report_directory = "/tmp/dqm_monitoring/",
    )

    s.make_report(backlog=500)


