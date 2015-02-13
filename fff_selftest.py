#!/usr/bin/env python

import os
import sys
import socket
import time
import subprocess
import json
import logging
import fff_filemonitor

log = logging.getLogger(__name__)

class FFFMonitoringTest():
    def __init__(self, path, server=None):
        hostname = socket.gethostname()

        self.path = path
        self.server = server
        self.self_sequence = 0
        self.self_doc = {
            "sequence": 0,
            "hostname": hostname,
            "extra": {},
            "pid": os.getpid(),
            "_id": "dqm-stats-%s" % hostname,
            "type": "dqm-stats"
        }

    def make_selftest(self):
        doc = self.self_doc

        self.self_sequence += 1
        doc["timestamp"] = time.time()
        doc["sequence"] = self.self_sequence
        doc["pid"] = os.getpid()

        if self.server is not None:
            log = self.server.log_capture.retrieve()
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

        body = json.dumps(doc, indent=True)

        final_fp = os.path.join(self.path, doc["_id"] + ".jsn")
        fff_filemonitor.atomic_create_write(final_fp, body)

        return final_fp

    def run_greenlet(self):
        import gevent

        while True:
            try:
                fp = self.make_selftest()
                log.info("Created a report: %s", fp)
            except:
                log.warning("Failed to create a report!", exc_info=True)

            gevent.sleep(60)

def __run__(server, opts):
    import gevent

    f = FFFMonitoringTest(path = opts["path"], server = server)
    return (gevent.spawn(f.run_greenlet), f, )

if __name__ == "__main__":
    x = FFFMonitoringTest(path="./")
    fp = x.make_selftest()

    print "Made report:", fp
