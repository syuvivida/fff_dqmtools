#!/usr/bin/env python

import os
import sys
import stat
import logging
import json

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

        try:
            os.chmod(self.path,
                stat.S_IRWXU |  stat.S_IRWXG | stat.S_IRWXO | stat.S_ISVTX)
        except OSError:
            pass

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
        #os.unlink(fp)

    def process_dir(self):
        for f in os.listdir(self.path):
            fp = os.path.join(self.path, f)
            self.process_file(fp)

    def create_watcher(self):
        import _inotify as inotify
        import watcher

        mask = inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO
        w = watcher.Watcher()

        w.add(self.path, mask)
        self.w = w

        return w

    def handle_watcher(self, w):
        # clear the events
        # this sometimes fails due to a bug in inotify
        for event in w.read(bufsize=0):
            pass

        self.process_dir()
