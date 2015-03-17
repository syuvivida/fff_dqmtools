#!/usr/bin/env python

import os
import sys
import stat
import logging
import json
import subprocess
import socket
import struct

import fff_dqmtools

def atomic_read_delete(fp):
    import os, stat, fcntl, errno

    tmp_fp = fp
    tmp_fp += ".open_pid%d" % os.getpid()
    tmp_fp += "_tag" + os.urandom(32).encode("hex").upper()

    os.rename(fp, tmp_fp)

    # ensure the file is regular!
    flags = os.O_RDWR|os.O_NOFOLLOW|os.O_NOCTTY|os.O_NONBLOCK|os.O_NDELAY
    try:
        fd = os.open(tmp_fp, flags)
    except OSError, e:
        # check if it was a symbolic link and remove if so
        if e.errno == errno.ELOOP:
            os.unlink(tmp_fp)

        raise

    # i guess this can already be done
    os.unlink(tmp_fp)

    s = os.fstat(fd)
    if not stat.S_ISREG(s.st_mode):
        os.close(fd)
        raise Exception("Not a regular file!")

    # hardlink count should be zero, since we have deleted it
    if s.st_nlink != 0:
        os.close(fd)
        raise Exception("Too many hardlinks: %d!" % s.st_nlink)

    flags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
    flags &= ~os.O_NONBLOCK
    flags &= ~os.O_NDELAY
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)

    f = os.fdopen(fd, "rb")
    b = f.read()
    f.close()

    return b

def atomic_create_write(fp, body):
    import tempfile

    f = tempfile.NamedTemporaryFile(prefix=fp + ".", suffix=".tmp", delete=False)
    tmp_fp = f.name
    f.write(body)
    f.close()

    os.rename(tmp_fp, fp)

def socket_upload(lst, log=None):
    # find the socket and transmit
    sock_name = fff_dqmtools.get_lock_key("fff_web")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock_connected = False

    try:
        for body in lst:
            if not sock_connected:
                sock.connect("\0" + sock_name)
                sock_connected = True
                #if log: log.info("Connected: %s", sock.fileno())

            sock.sendall(struct.pack("!Q", len(body)).encode("hex"))
            sock.sendall(body)

    except socket.error:
        if log: log.warning("Couldn't upload files to a web instance: %s", sock_name, exc_info=True)
        raise
    finally:
        sock.close()

class FileMonitor(object):
    def __init__(self, path, log):
        self.path = path
        self.log = log

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
            self.log.info("Mountpoint found at %s.",  path)
        elif ret == 1 and os.geteuid() == 0:
            lock = os.path.join(path, ".dqm_ramdisk")
            if os.path.exists(lock):
                self.log.warning("Mountpoint not mounted, but .dqm_ramdisk found in %s.",  lock)
                return 0

            ret = subprocess.call(["mount", "-t", "tmpfs", "-o", "dev,noexec,nosuid,size=256m", "dqm_monitoring_ramdisk", path])
            open(lock, "w").close()
            self.log.info("Mountpoint mounted at %s, exit code: %d", path, ret)
        else:
            self.log.info("Mountpoint not found and we can't mount.")

    def scan_dir(self, max_count=None):
        lst = os.listdir(self.path)
        for f in lst:
            fp = os.path.join(self.path, f)

            if max_count is not None:
                if max_count <= 0:
                    return

                max_count -= 1

            fname = os.path.basename(fp)
            if fname.startswith("."): continue
            if not fname.endswith(".jsn"): continue

            #self.log.info("Uploading: %s", fp)
            try:
                body = atomic_read_delete(fp)

                # this is absolutely unnecessary
                # but it's really good to detect json errors early
                #_json = json.loads(body)

                yield body
            except:
                self.log.warning("Failure to read the document: %s", fp, exc_info=True)
                #raise Exception("Please restart.")

    def process_dir(self):
        bodydoc_generator = self.scan_dir(max_count=150)
        return socket_upload(bodydoc_generator, self.log)

    def run_greenlet(self):
        self.process_dir()

        import select
        import _inotify as inotify
        import watcher

        mask = inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO
        w = watcher.Watcher()

        w.add(self.path, mask)

        fd = w.fileno()

        while True:
            r = select.select([fd], [], [], 30)

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
                self.log.warning("bad return from select: %s", str(r))

@fff_dqmtools.fork_wrapper(__name__)
@fff_dqmtools.lock_wrapper
def __run__(opts, **kwargs):
    global log
    log = kwargs["logger"]
    path = opts["path"]

    fmon = FileMonitor(path = path, log = log)
    fmon.run_greenlet()
