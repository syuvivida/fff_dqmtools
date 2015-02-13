#!/usr/bin/env python
import json
import sqlite3
import os, sys, time
import socket
import logging

import fff_monitoring
import fff_cluster

# fff_monitoring fixed the imports for us
import bottle


# global variable to access from fff_filemonitor.py
instances = []

class WebServer(object):
    def __init__(self, db=None):
        self.db_str = db
        self.log = logging.getLogger(__name__)

        if not self.db_str:
            self.db_str = ":memory:"
        
        self.db = sqlite3.connect(self.db_str)
        self.create_tables()
        self.setup_routes()

    def drop_tables(self):
        cur = self.db.cursor()
        cur.execute("DROP TABLE IF EXISTS Monitoring")

        self.db.commit()
        cur.close()

    def create_tables(self):
        cur = self.db.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS Monitoring (
            id TEXT PRIMARY KEY NOT NULL,
            timestamp TIMESTAMP,
            type TEXT,
            host TEXT,
            tag  TEXT,
            run  INT,
            body BLOB
        )""")

        cur.execute("CREATE INDEX IF NOT EXISTS M_type_index ON Monitoring (type)")
        cur.execute("CREATE INDEX IF NOT EXISTS M_host_index ON Monitoring (host)")
        cur.execute("CREATE INDEX IF NOT EXISTS M_run_index ON Monitoring (run)")

        self.db.commit()
        cur.close()

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
        cur.close()

    def setup_routes(self):
        static_path = os.path.dirname(__file__)
        static_path = os.path.join(static_path, "./static/")

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
            c = self.db.cursor()
            c.execute("PRAGMA page_size")
            ps = c.fetchone()[0]
            c.execute("PRAGMA page_count")
            pc = c.fetchone()[0]
            c.close()

            return {
                'hostname': socket.gethostname(),
                'timestamp': time.time(),
                'cluster': fff_cluster.get_node(),
                'db_size': ps*pc,
            }

        @bottle.route("/list/runs", method=['GET', 'POST'])
        def list_runs():
            c = self.db.cursor()
            c.execute("SELECT DISTINCT run FROM Monitoring ORDER BY run DESC")

            runs = map(lambda x: x[0], c.fetchall())
            runs = filter(lambda x: x is not None, runs)
            c.close()

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
            docs = { 'hits': prepare_docs(c) }
            c.close()
            return docs

        @bottle.route("/list/stats", method=['GET', 'POST'])
        def list_stats():
            c = self.db.cursor()
            c.execute("SELECT * FROM Monitoring WHERE run IS NULL ORDER BY tag ASC")
            docs = { 'hits': prepare_docs(c) }
            c.close()
            return docs

        @bottle.route("/show/log/<id>", method=['GET', 'POST'])
        def list_stats(id):
            c = self.db.cursor()
            c.execute("SELECT body FROM Monitoring WHERE id = ?", (id, ))
            doc = c.fetchone()
            c.close()

            b = json.loads(doc[0])
            fn = b["stdout_fn"]
            fn = os.path.realpath(fn)

            allowed = ["/var/log/hltd/pid/"]
            for p in allowed:
                if os.path.commonprefix([fn, p]) == p:
                    relative = os.path.relpath(fn, p)
                    #print "in allowed", p, r
                    return bottle.static_file(relative, root=p, mimetype="text/plain")

            raise bottle.HTTPError(500, "Log path not found.")


        @bottle.route("/utils/kill_proc/<id>", method=['POST'])
        def kill_proc(id):
            from bottle import request
            data = json.loads(request.body.read())

            # check if id known to us
            c = self.db.cursor()
            c.execute("SELECT body FROM Monitoring WHERE id = ?", (id, ))
            doc = c.fetchone()
            c.close()

            if not doc:
                raise bottle.HTTPResponse("Process not found.", status=404)

            b = json.loads(doc[0])
            pid = int(b["pid"])

            if pid != int(data["pid"]):
                raise bottle.HTTPResponse("Process and pid not found.", status=404)

            if b.has_key("exit_code"):
                raise bottle.HTTPResponse("Process already died.", status=404)

            signal = int(data["signal"])
            if signal not in [9, 15, 11, 3, 2, 12]:
                raise bottle.HTTPResponse("Invalid signal number.", status=500)

            import subprocess
            r = subprocess.call(["kill", "-s", str(signal), str(pid)])

            body = "Process killed, kill exit_code: %d" % r
            return body

        @bottle.route("/utils/drop_run", method=['POST'])
        def drop_run():
            from bottle import request
            data = json.loads(request.body.read())
            run = int(data["run"])

            # check if id known to us
            c = self.db.cursor()
            c.execute("DELETE FROM Monitoring WHERE run = ?", (run, ))
            doc = c.fetchone()
            c.close()
            self.db.commit()

            return "Rows deleted for run%08d!" % run

    def run_test(self, port=9315):
        bottle.run(host="0.0.0.0", port=port, reloader=True)

    def run_greenlet(self, host="::0", port=9215, **kwargs):
        from gevent import wsgi, pywsgi, local
        #if not self.options.pop('fast', None): wsgi = pywsgi

        addr = socket.getaddrinfo(host, port, socket.AF_INET6, 0, socket.SOL_TCP)
        listener = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(addr[0][-1])
        listener.listen(15)

        #listener = (host, port, )

        app = bottle.default_app()
        server = wsgi.WSGIServer(listener, app, **kwargs)

        self.log.info("Using db: %s." % (self.db_str))
        self.log.info("Started web server at [%s]:%d" % (host, port))
        self.log.info("Go to http://%s:%d/" % (socket.gethostname(), port))

        server.serve_forever()

def __run__(server, opts):
    import gevent

    db = opts["db"]
    port = opts["port"]

    fweb = WebServer(db = db)
    fw = gevent.spawn(lambda: fweb.run_greenlet(port = port))

    return (fw, fweb, )

if __name__ == "__main__":
    db = sqlite3.connect("./db.sqlite3")
    w = WebServer(db=db)

    #w.run_greenlet(port=9315)
    w.run_test()
