#!/usr/bin/env python

import os
import sys
import stat
import logging
import json
import subprocess

import fff_daemon

log = logging.getLogger("root")

class FileMonitor(object):
    def __init__(self, path, web_process=None):
        self.path = path
        self.web_servers = []
        self.web_process = web_process

        try:
            os.makedirs(self.path)
        except OSError:
            pass

        # try to create a ramdisk there
        try:
            self.try_create_ramdisk()
        except OSError:
            pass

        try:
            os.chmod(self.path,
                stat.S_IRWXU |  stat.S_IRWXG | stat.S_IRWXO | stat.S_ISVTX)
        except OSError:
            pass

    def try_create_ramdisk(self):
        path = self.path

        ret = subprocess.call(["mountpoint", "-q", path])
        if ret == 0:
            log.info("Mountpoint found at %s.",  path)
        elif ret == 1 and os.geteuid() == 0:
            lock = os.path.join(path, ".dqm_ramdisk")
            if os.path.exists(lock):
                log.warning("Mountpoint not mounted, but .dqm_ramdisk found in %s.",  lock)
                return 0

            ret = subprocess.call(["mount", "-t", "tmpfs", "-o", "dev,noexec,nosuid,size=256m", "dqm_monitoring_ramdisk", path])
            open(lock, "w").close()
            log.info("Mountpoint mounted at %s, exit code: %d", path, ret)
        else:
            log.info("Mountpoint not found and we can't mount.")

    def upload_file(self, fp):
        log.info("Uploading: %s", fp)

        try:
            f = open(fp, "r")
            json_text = f.read()
            document = json.loads(json_text)
            f.close()

            if self.web_process is not None:
                self.web_process.direct_upload(document, json_doc=json_text)

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
        ret = os.unlink(fp)

    def process_dir(self):
        lst = os.listdir(self.path)
        for f in lst:
            fp = os.path.join(self.path, f)
            self.process_file(fp)

    def run_greenlet(self):
        import gevent.select
        import _inotify as inotify
        import watcher

        mask = inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO
        w = watcher.Watcher()

        w.add(self.path, mask)

        fd = w.fileno()

        while True:
            r = gevent.select.select([fd], [], [], timeout=30)
            print "Ret", r
            if len(r[0]) == 0:
                # timeout
                self.process_dir()
            elif r[0][0] == fd:
                # clear the events
                # this sometimes fails due to a bug in inotify
                for event in w.read(bufsize=0):
                    pass

                self.process_dir()
            else:
                log.warning("bad return from select: %s", str(r))

if __name__ == "__main__":
    fmon = FileMonitor(
        path = "/tmp/dqm_monitoring/",
    )
    fmon.create_greenlet()
