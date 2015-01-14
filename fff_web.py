#!/usr/bin/env python

import logging
import json
import sqlite3
import os, sys, time
import socket

log = logging.getLogger("root")

#def test():
#    files = os.listdir("/tmp/dqm_monitoring/")
#    for f in files:
#        if not f.endswith(".jsn"): continue
#
#        fp = os.path.join("/tmp/dqm_monitoring/", f)
#        body = json.load(open(fp, "r"))
#
#        cur.execute("INSERT OR REPLACE INTO Monitoring (id, timestamp, type, host, tag, run, body) VALUES (?, ?, ?, ?, ?, ?, ?)", (
#            body.get("_id"),
#            body.get("timestamp", time.time()),
#            body.get("type"),
#            body.get("hostname"),
#            body.get("tag"),
#            body.get("run"),
#            json.dumps(body),
#        ))
#
#    conn.commit()
#    return conn
#
#db = test()

import fff_monitoring

class WebServer(object):
    def __init__(self, db=None):
        self.db = db
        if self.db is None:
            self.db = sqlite3.connect(":memory:")
            self.create_tables()

        self.bottle = __import__('bottle')
        self.setup_routes()

    def create_tables(self):
        cur = self.db.cursor()

        cur.execute("DROP TABLE IF EXISTS Monitoring")
        cur.execute("""
        CREATE TABLE Monitoring (
            id TEXT PRIMARY KEY NOT NULL,
            timestamp TIMESTAMP,
            type TEXT,
            host TEXT,
            tag  TEXT,
            run  INT,
            body BLOB
        )""")

        cur.execute("CREATE INDEX M_type_index ON Monitoring (type)")
        cur.execute("CREATE INDEX M_host_index ON Monitoring (host)")
        cur.execute("CREATE INDEX M_run_index ON Monitoring (run)")

        self.db.commit()

    def direct_upload(self, document, json_doc=None):
        cur = self.db.cursor()

        if json_doc is None:
            json_doc = json.dumps(document)

        cur.execute("INSERT OR REPLACE INTO Monitoring (id, timestamp, type, host, tag, run, body) VALUES (?, ?, ?, ?, ?, ?, ?)", (
            document.get("_id"),
            document.get("timestamp", time.time()),
            document.get("type"),
            document.get("hostname"),
            document.get("tag"),
            document.get("run"),
            json_doc,
        ))

        self.db.commit()

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

        @bottle.get("/info")
        def info():
            return {
                'hostname': socket.gethostname(),
                'timestamp': time.time(),
            }

        @bottle.route("/list/runs", method=['GET', 'POST'])
        def list_runs():
            c = self.db.cursor()
            c.execute("SELECT DISTINCT run FROM Monitoring ORDER BY run DESC")

            runs = map(lambda x: x[0], c.fetchall())
            runs = filter(lambda x: x is not None, runs)

            return { 'runs': runs }

        def prepare_docs(c):
            columns = list(map(lambda x: x[0], c.description))
            hits = []

            for x in c.fetchall():
                hit = dict(zip(columns, x))

                body = hit["body"]
                del hit["body"]
                body = json.loads(body)
                body["_meta"] = hit
                hits.append(body)

            return hits

        @bottle.route("/list/run/<run:int>", method=['GET', 'POST'])
        def list_run(run):
            c = self.db.cursor()
            c.execute("SELECT * FROM Monitoring WHERE run = ? ORDER BY tag ASC", (run, ))
            return { 'hits': prepare_docs(c) }

        @bottle.route("/list/stats", method=['GET', 'POST'])
        def list_stats():
            c = self.db.cursor()
            c.execute("SELECT * FROM Monitoring WHERE run IS NULL ORDER BY tag ASC")
            return { 'hits': prepare_docs(c) }

    def run_test(self):
        self.bottle.run(host="localhost", port=8080, reloader=True)

    def create_wsgi_server(self, host="::0", port=9215):
        from wsgiref.simple_server import make_server
        from wsgiref.simple_server import WSGIRequestHandler, WSGIServer
        import socket

        app = self.bottle.default_app()

        class FixedHandler(WSGIRequestHandler):
            def address_string(self): # Prevent reverse DNS lookups please.
                return self.client_address[0]

            def log_request(*args, **kw):
                return WSGIRequestHandler.log_request(*args, **kw)

        handler_cls = FixedHandler
        server_cls  = WSGIServer

        if ':' in host: # Fix wsgiref for IPv6 addresses.
            if getattr(server_cls, 'address_family') == socket.AF_INET:
                class server_cls(server_cls):
                    address_family = socket.AF_INET6

        srv = make_server(host, port, app, server_cls, handler_cls)
        return srv

        # this is for the reference:
        #try:
        #    srv.serve_forever()
        #except KeyboardInterrupt:
        #    srv.server_close() # Prevent ResourceWarning: unclosed socket
        #    raise

    def handle_request(self, srv):
        srv.handle_request()

if __name__ == "__main__":
    db = sqlite3.connect("mydb.db")
    w = WebServer(db=db)

    srv = w.create_wsgi_server()
    srv.serve_forever()
