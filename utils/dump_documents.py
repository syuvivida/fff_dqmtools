import json
import sqlite3
import os, sys, time
import socket
import logging

cd = os.path.dirname(__file__)
sys.path.append(os.path.join(cd, "../"))

import applets.fff_filemonitor as fff_filemonitor

if __name__  == "__main__":
    if len(sys.argv) != 3:
        print "Usage: %s database_file output_directory" % sys.argv[0]
        sys.exit(1)

    db = sys.argv[1]
    path = sys.argv[2]


    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("SELECT id, body FROM Monitoring")

    for x in iter(c.fetchone, None):
        n_id = x[0]
        n_body = x[1]

        fp = os.path.join(path, n_id + ".jsn")
        print "Creating file %s size=%d" % (fp, len(n_body))
        fff_filemonitor.atomic_create_write(fp, n_body)

    c.close()
