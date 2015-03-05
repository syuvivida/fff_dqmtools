#!/usr/bin/env python
import json
import sqlite3
import os, sys, time
import socket
import logging

import fff_dqmtools
import fff_cluster
import fff_filemonitor

# fff_dqmtools fixed the imports for us
import bottle

log = logging.getLogger(__name__)


from geventwebsocket import WebSocketServer, Resource, WebSocketError
from geventwebsocket.handler import WebSocketHandler

class SyncClient(object):
    STATE_NONE      = 1
    STATE_INSYNC    = 2
    STATE_LISTEN    = 3
    STATE_CLOSED    = -1

    def __init__(self, app, ws):
        self.app = app
        self.ws = ws
        self.address = ws.handler.client_address

        self.backlog = []
        self.state = self.STATE_NONE
        self.close_reason = None

    def kill(self, reason):
        if self.state == self.STATE_CLOSED:
            return

        self.state = self.STATE_CLOSED
        self.close_reason = reason
    
    def receiveMessage(self, msg):
        #print "recv:", self, msg
        #sys.stdout.flush()

        if msg is None:
            self.kill("closed")
            return

        jsn = json.loads(msg)
        if jsn["event"] == "sync_request":
            known_rev = jsn["known_rev"]
            log.info("WebSocket client (%s) requested sync from rev %s", self.address, known_rev)
            self.state = self.STATE_INSYNC

            # send the current state
            # if any changes happen during this time
            # they go into backlog
            with self.app.db as db:
                c = db.cursor()

                c.execute("SELECT id, rev, timestamp, type, hostname, tag, run FROM Monitoring ORDER BY rev ASC")
                headers = list(self.app.prepare_headers(c))
                self.backlog.insert(0, headers)
                c.close()

            while len(self.backlog):
                h = self.backlog.pop(0)
                self.sendHeaders(h)

            self.state = self.STATE_LISTEN
        
        if jsn["event"] == "request_documents":
            ids = set(jsn["ids"])

            with self.app.db as db:
                c = db.cursor()

                IN = "(" + ",".join("?"*len(ids)) + ")"
                c.execute("SELECT * FROM Monitoring WHERE id IN " + IN, list(ids))

                docs = list(self.app.prepare_docs(c))
                c.close()

            jsn = json.dumps({
                'event': 'update_documents',
                'documents': docs,
            }).encode("utf8")

            log.info("WebSocket client (%s) requested %d documents (%d bytes)", self.address, len(ids), len(jsn))
            self.ws.send(jsn)

    def sendHeaders(self, headers):
        # split sending into multiple messages
        # this should be extremely helpful with users on bad connections
        cp = list(headers)
        max_size = 1000 
        last_rev = cp[-1]["_rev"]

        while cp:
            to_send, cp = cp[:max_size], cp[max_size:]

            self.ws.send(json.dumps({
                'event': 'update_headers',
                'rev': [to_send[0]["_rev"], to_send[-1]["_rev"]],
                'sync_to_rev': last_rev,
                'headers': to_send,
            }))

    def handleUpdate(self, headers):
        if self.state == self.STATE_INSYNC:
            self.backlog.append(headers)
        elif self.state == self.STATE_LISTEN:
            self.sendHeaders(headers)
        else:
            pass

class WebServer(object):
    def __init__(self, db=None):
        self.db_str = db

        if not self.db_str:
            self.db_str = ":memory:"

        self.db = sqlite3.connect(self.db_str)

        self.ws_clients = set()

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
            rev INT,
            timestamp TIMESTAMP,
            hostname TEXT,
            type TEXT,
            tag  TEXT,
            run  INT,
            body BLOB
        )""")

        cur.execute("CREATE INDEX IF NOT EXISTS M_type_index ON Monitoring (type)")
        cur.execute("CREATE INDEX IF NOT EXISTS M_host_index ON Monitoring (hostname)")
        cur.execute("CREATE INDEX IF NOT EXISTS M_run_index ON Monitoring (run)")
        cur.execute("CREATE INDEX IF NOT EXISTS M_rev_index ON Monitoring (rev)")

        self.db.commit()
        cur.close()

    def make_header(self, doc):
        header = {
            "_id":          doc.get("_id"),
            "_rev":         doc.get("_rev", None),
            "timestamp":    doc.get("timestamp", time.time()),
            "hostname":     doc.get("hostname", None),
            "type":         doc.get("type", None),
            "tag":          doc.get("tag", None),
            "run":          doc.get("run", None),
        }

        return header
        
    def make_header_from_entry(self, dct):
        header = dict(dct)
        header["_id"] = header["id"]
        header["_rev"] = header["rev"]

        del header["id"]
        del header["rev"]

        return header

    def prepare_docs(self, c):
        columns = list(map(lambda x: x[0], c.description))

        for x in c.fetchall():
            hit = dict(zip(columns, x))

            body = hit["body"]
            del hit["body"]
            body = json.loads(body)
            body["_header"] = self.make_header_from_entry(hit)

            yield body

    def prepare_headers(self, c):
        columns = list(map(lambda x: x[0], c.description))

        for x in c.fetchall():
            hit = dict(zip(columns, x))
            hit = self.make_header_from_entry(hit)

            yield hit

    def direct_transactional_upload(self, bodydoc_generator):
        headers = []
        with self.db as db:
            rev = None

            def get_last_rev():
                cur = db.cursor()
                x = cur.execute("SELECT MAX(rev) FROM Monitoring")
                r = (x.fetchone()[0] or 0)
                cur.close()
                return r

            for (body, doc, ) in bodydoc_generator:
                if rev is None:
                    rev = get_last_rev()

                # not that we ever overflow it ...
                rev = (rev + 1) & ((2**63)-1)
                header = self.make_header(doc)
                header["_rev"] = rev

                db.execute("INSERT OR REPLACE INTO Monitoring (id, rev, timestamp, type, hostname, tag, run, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (
                    header.get("_id"),
                    header.get("_rev"),
                    header.get("timestamp"),
                    header.get("type"),
                    header.get("hostname"),
                    header.get("tag"),
                    header.get("run"),
                    body,
                ))

                headers.append(header)

        self.update_listeners(headers)

    def setup_routes(self):
        app = bottle.Bottle()
        self.app = app

        static_path = os.path.dirname(__file__)
        static_path = os.path.join(static_path, "../web.static/")

        @app.route('/static/<filepath:path>')
        def static(filepath):
            return bottle.static_file(filepath, root=static_path)

        @app.route('/')
        def index():
            bottle.redirect("/static/index.html")

        @app.get("/info")
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

        @app.route("/list/runs", method=['GET', 'POST'])
        def list_runs():
            c = self.db.cursor()
            c.execute("SELECT DISTINCT run FROM Monitoring ORDER BY run DESC")

            runs = map(lambda x: x[0], c.fetchall())
            runs = filter(lambda x: x is not None, runs)
            c.close()

            return { 'runs': runs }

        @app.route("/list/run/<run:int>", method=['GET', 'POST'])
        def list_run(run):
            c = self.db.cursor()
            c.execute("SELECT * FROM Monitoring WHERE run = ? ORDER BY tag ASC", (run, ))
            docs = { 'hits': list(self.prepare_docs(c)) }
            c.close()
            return docs

        @app.route("/list/stats", method=['GET', 'POST'])
        def list_stats():
            c = self.db.cursor()
            c.execute("SELECT * FROM Monitoring WHERE run IS NULL ORDER BY tag ASC")
            docs = { 'hits': list(self.prepare_docs(c)) }
            c.close()
            return docs

        @app.route("/show/log/<id>", method=['GET', 'POST'])
        def show_log(id):
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


        @app.route("/utils/kill_proc/<id>", method=['POST'])
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

        @app.route("/utils/drop_run", method=['POST'])
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

        @app.route("/sync")
        def sync():
            from bottle import request
            ws = request.environ.get('wsgi.websocket')
            if not ws:
                abort(400, 'Expected WebSocket request.')
        
        
            c = SyncClient(self, ws)
            log.info("WebSocket connected: %s", c.address)

            try:
                self.ws_clients.add(c)
                
                while True:
                    if c.state == c.STATE_CLOSED:
                        break

                    msg = ws.receive()
                    log.info("Receive")
                    c.receiveMessage(msg)
                    log.info("Receive end")
            except:
                log.info("WebSocket error", exc_info=True)
                c.kill("receive exception")
            finally:
                c.kill("closed")

                if c in self.ws_clients:
                    self.ws_clients.remove(c)

                log.info("WebSocket disconnected: %s, reason: \"%s\"", c.address, c.close_reason)

    def update_listeners(self, headers):
        # it is possible that listeners list will change
        # in a different greenlet (ie, exit or something)

        # we don't care much because changes to the db 
        # cannot happen outside this greenlet (and before this function is run)

        if len(headers) == 0:
            return
    
        copy = list(self.ws_clients)
        for client in copy:
            client.handleUpdate(headers)

    def run_greenlet(self, host="0.0.0.0", port=9215, **kwargs):
        from gevent import wsgi, pywsgi, local
        #if not self.options.pop('fast', None): wsgi = pywsgi

        ## this was for the ipv6 support, but it does not work with websockets
        ## need a new version of gevent, which is not yet in slc

        #addr = socket.getaddrinfo(host, port, socket.AF_INET6, 0, socket.SOL_TCP)
        #listener = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        #listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #listener.bind(addr[0][-1])
        #listener.listen(15)

        listener = (host, port, )
        server = pywsgi.WSGIServer(listener, self.app, handler_class=WebSocketHandler)

        # this is needed for the sync
        self.sync_server = server

        log.info("Using db: %s." % (self.db_str))
        log.info("Started web server at [%s]:%d" % (host, port))
        log.info("Go to http://%s:%d/" % (socket.gethostname(), port))

        server.serve_forever()

@fff_dqmtools.fork_wrapper(__name__)
@fff_dqmtools.lock_wrapper
def __run__(opts, **kwargs):
    global log
    log = kwargs["logger"]

    import gevent

    db = opts["web.db"]
    port = opts["web.port"]
    path = opts["path"]

    fweb = WebServer(db = db)
    fmon = fff_filemonitor.FileMonitor(path = path, fweb = fweb, log = log)

    fwt = gevent.spawn(lambda: fweb.run_greenlet(port = port))
    fmt = gevent.spawn(lambda: fmon.run_greenlet())

    gevent.joinall([fwt, fmt], raise_error=True)

if __name__ == "__main__":
    print "unrecable"
    db = sqlite3.connect("./db.sqlite3")
    w = WebServer(db=db)

    #w.run_greenlet(port=9315)
    w.run_test()
