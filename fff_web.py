#!/usr/bin/env python

import logging
import json
import sqlite3
import os, sys, time
import socket

log = logging.getLogger("root")

def test():
    conn = sqlite3.connect("mydb.db")
    return conn

    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS Monitoring")
    cur.execute("""
    CREATE TABLE Monitoring (
        id TEXT PRIMARY KEY NOT NULL,
        type TEXT,
        host TEXT,
        run  INT,
        body BLOB 
    )""")

    cur.execute("CREATE INDEX M_type_index ON Monitoring (type)")
    cur.execute("CREATE INDEX M_host_index ON Monitoring (host)")
    cur.execute("CREATE INDEX M_run_index ON Monitoring (run)")

    files = os.listdir("/tmp/dqm_monitoring/")
    for f in files:
        if not f.endswith(".jsn"): continue
    
        fp = os.path.join("/tmp/dqm_monitoring/", f)
        body = json.load(open(fp, "r"))
        print f, body["_id"]

        cur.execute("INSERT OR REPLACE INTO Monitoring (id, type, host, run, body) VALUES (?, ?, ?, ?, ?)", (
            body.get("_id"),
            body.get("type"),
            body.get("hostname"),
            body.get("run"),
            json.dumps(body),
        ))

    conn.commit()
    return conn

db = test()

import fff_monitoring

class WebServer(object):
    def __init__(self, db):
        self.db = db
        self.bottle = __import__('bottle')

        self.setup_routes()

    def setup_routes(self):
        static_path = os.path.dirname(__file__)
        static_path = os.path.join(static_path, "./static/")
        bottle = self.bottle

        @bottle.route('/static/<filepath:path>')
        def static(filepath):
            return bottle.static_file(filepath, root=static_path)

        @bottle.route('/')
        def index():
            bottle.redirect("/static/index.html")

        @bottle.post('/update/<id>')
        def update(id):
            print "post", id
            pass

        
        @bottle.get("/info/")
        def info():
            return {
                'hostname': socket.gethostname(),
                'timestamp': time.time(),
            }

        @bottle.get("/aggregate/runs/")
        def aggregate_runs():
            c = self.db.cursor()
            c.execute("SELECT DISTINCT run FROM Monitoring ORDER BY run DESC")

            runs = map(lambda x: x[0], c.fetchall())
            runs = filter(lambda x: x is not None, runs)

            return { 'runs': runs }


    def run(self, *kargs, **kwargs):
        self.bottle.run(*kargs, **kwargs)

w = WebServer(db=db)
w.run(host='localhost', port=8080, reloader=True)

print "x"
