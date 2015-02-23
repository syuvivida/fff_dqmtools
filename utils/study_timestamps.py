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

def make_dict(collected):
    dct = {}
    for entry in collected:
        key = entry[0]
        dct[key] = entry[1:]

    return dct


if __name__ == "__main__":
    print "# Collect /fff/ramdisk"
    ramdisk = collect("/fff/ramdisk/")

    print "# Collect /fff/output/transfer/"
    output = collect("/fff/output/transfer/")

    ramdisk = make_dict(ramdisk)
    output = make_dict(output)

    #import pprint
    #pprint.pprint(ramdisk)
    #pprint.pprint(output)

    print "# format is: time_diff, file, date"
    keys = list(output.keys())
    keys.sort()
    for key in keys:
        if not ramdisk.has_key(key):
            # /fff/ramdisk is usually more aggressive to delete files
            # this *likely* means file is already deleted
            tdiff = "---"
        else:
            # the time difference in seconds
            tdiff = "%.02f" % (output[key][2] - ramdisk[key][2])

        # file name and modification time
        filename = output[key][0]
        ftime = datetime.datetime.fromtimestamp(output[key][2])

        print "%s %s %s" % (tdiff, filename, ftime)
