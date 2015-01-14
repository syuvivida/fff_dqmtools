#!/usr/bin/env python

import os
import sys
import socket
import time
import subprocess
import json

class FFFMonitoringTest():
    def __init__(self, path):
        hostname = socket.gethostname()

        self.path = path
        self.self_sequence = 0
        self.self_doc = {
            "timestamp": time.time(),
            "sequence": 0,
            "hostname": hostname,
            "extra": {},
            "pid": os.getpid(),
            "_id": "dqm-stats-%s" % hostname,
            "type": "dqm-stats"
        }

    def make_selftest(self, force=False, log_capture=None):
        doc = self.self_doc

        if (not force) and ((time.time() - doc["timestamp"]) < 30):
            return

        self.self_sequence += 1
        doc["timestamp"] = time.time()
        doc["sequence"] = self.self_sequence
        doc["pid"] = os.getpid()

        if log_capture is not None:
            log = log_capture.retrieve()
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

        return final_fp

if __name__ == "__main__":
    x = FFFMonitoringTest(path="./")
    fp = x.make_selftest(force=True)

    print "Made report:", fp
