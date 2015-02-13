import os
import logging
import sys
import glob
import time

log = logging.getLogger(__name__)

def do_the_log_cleanup(fake=False):
    MAX_SIZE = 1024*1024*1024 # max file size, 1gb
    KEEP_SIZE = 16*1024*1024 # position we truncate to and start overwritting from, 16m

    files = glob.glob("/var/log/hltd/pid/hlt_run*_pid*.log")
    for file in files:
        st = os.stat(file)
        if st.st_size > MAX_SIZE:
            log.info("File %s is too big, %d > %d, truncating." % (file, st.st_size, MAX_SIZE))

            if fake:
                continue

            f = open(file, "rb+")
            f.seek(KEEP_SIZE)
            f.truncate()
            f.write("\n\n... file was truncated at this point ...\n\n")
            f.close()

    return len(files)

def __run__(server, opts):
    import gevent

    def run_greenlet():
        while True:
            try:
                start = time.time()
                files = do_the_log_cleanup()
                took = time.time() - start
                log.info("Log cleaner finished, checked %d files, took %.02f seconds.", files, took)
            except:
                log.warning("Log cleaner crashed!", exc_info=True)

            gevent.sleep(150)

    return (gevent.spawn(run_greenlet),  )


