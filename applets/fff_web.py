#!/usr/bin/env python
import json
import sqlite3
import os, sys, time
import socket
import logging
import StringIO
import gzip

import fff_dqmtools
import fff_cluster
import fff_filemonitor

# fff_dqmtools fixed the imports for us
import bottle

log = logging.getLogger(__name__)

from ws4py.websocket import WebSocket

class Database(object):
    def __init__(self, db=None):
        self.db_str = db

        if not self.db_str:
            self.db_str = ":memory:"

        self.listeners = []
        self.conn = sqlite3.connect(self.db_str)

        # create the header cache for now
        self.header_cache = None
        self.get_headers()

    def drop_tables(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS Monitoring")

        self.conn.commit()
        cur.close()

    def create_tables(self):
        cur = self.conn.cursor()

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

        self.conn.commit()
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

    def get_headers(self, reload=False):
        if self.header_cache is None or reload:
            with self.conn as db:
                c = db.cursor()
                c.execute("SELECT id, rev, timestamp, type, hostname, tag, run FROM Monitoring ORDER BY rev ASC")
                self.header_cache = list(self.prepare_headers(c))
                c.close()

        return self.header_cache


    def direct_transactional_upload(self, bodydoc_generator):
        headers = [] # this is used to notify websockets
        with self.conn as db:
            rev = None

            def get_last_rev():
                cur = db.cursor()
                x = cur.execute("SELECT MAX(rev) FROM Monitoring")
                r = (x.fetchone()[0] or 0)
                cur.close()
                return r

            for body in bodydoc_generator:
                if rev is None:
                    rev = get_last_rev()

                # get the document
                doc = json.loads(body)

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

                self.header_cache.append(header)
                headers.append(header)

        self.update_headers(headers)

    def add_listener(self, listener):
        self.listeners.append(listener)

    def remove_listener(self, listener):
        self.listeners.remove(listener)

    def update_headers(self, headers):
        # it is possible that listeners list will change
        # in a different greenlet (ie, exit or something)

        # we don't care, since client should handle
        # the websocket errors.
        if len(headers) == 0:
            return

        copy = list(self.listeners)
        for client in copy:
            client.updateHeaders(headers)

class SyncSocket(WebSocket):
    STATE_NONE      = 1
    STATE_INSYNC    = 2
    STATE_LISTEN    = 3
    STATE_CLOSED    = -1

    def opened(self):
        self.backlog = []
        self.state = self.STATE_NONE
        self.close_reason = None

        self.db.add_listener(self)

        log.info("WebSocket connected: %s", self.peer_address)

    def closed(self, code, reason=None):
        self.db.remove_listener(self)

        log.info("WebSocket disconnected: %s code=%s reason=%s", self.peer_address, code, reason)

    def kill(self, reason):
        if self.state == self.STATE_CLOSED:
            return

        self.state = self.STATE_CLOSED
        self.close_reason = reason

    def received_message(self, msg):
        #print "recv:", self, msg, type(msg)
        #sys.stdout.flush()

        jsn = json.loads(msg.data)
        if jsn["event"] == "sync_request":
            known_rev = jsn["known_rev"]
            self.state = self.STATE_INSYNC

            log.info("WebSocket client (%s) requested sync from rev %s", self.peer_address, known_rev)

            # send the current state
            # if any changes happen during this time
            # they go into backlog
            self.backlog.insert(0, self.db.get_headers())
            while len(self.backlog):
                h = self.backlog.pop(0)
                self.sendHeaders(h)

            self.state = self.STATE_LISTEN

        if jsn["event"] == "request_documents":
            ids = set(jsn["ids"])

            with self.db.conn as db:
                c = db.cursor()

                IN = "(" + ",".join("?"*len(ids)) + ")"
                c.execute("SELECT * FROM Monitoring WHERE id IN " + IN, list(ids))

                docs = list(self.db.prepare_docs(c))
                c.close()

            jsn = json.dumps({
                'event': 'update_documents',
                'documents': docs,
            }).encode("utf8")

            log.info("WebSocket client (%s) requested %d documents (%d bytes)", self.peer_address, len(ids), len(jsn))
            self.send(jsn, False)

    def sendHeaders(self, headers):
        # split sending into multiple messages
        # this should be extremely helpful with users on bad connections
        cp = list(headers)
        max_size = 1000
        last_rev = cp[-1]["_rev"]

        while cp:
            to_send, cp = cp[:max_size], cp[max_size:]

            self.send(json.dumps({
                'event': 'update_headers',
                'rev': [to_send[0]["_rev"], to_send[-1]["_rev"]],
                'sync_to_rev': last_rev,
                'headers': to_send,
            }), False)

    def updateHeaders(self, headers):
        # this cannot throw
        # or it will kill the input server

        try:
            if self.state == self.STATE_INSYNC:
                self.backlog.append(headers)
            elif self.state == self.STATE_LISTEN:
                self.sendHeaders(headers)
            else:
                pass
        except:
            log.warning("WebSocket error.", exc_info=True)

class WebServer(bottle.Bottle):
    def __init__(self, db=None):
        bottle.Bottle.__init__(self)

        self.db = db
        self.setup_routes()


    def setup_routes(self):
        app = self

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
            c = self.db.conn.cursor()
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

        @app.route("/get/<id>", method=['GET', 'POST'])
        def get_id(id):
            # check if id known to us
            with self.db.conn as db:
                c = db.cursor()
                c.execute("SELECT * FROM Monitoring WHERE id = ?", (id, ))
                docs = list(self.db.prepare_docs(c))
                c.close()

                if len(docs) == 0:
                    raise bottle.HTTPResponse("Doc id not found.", status=404)

                return docs[0]

        @app.get("/headers/cached/")
        def header_cache():
            from bottle import request, response
            headers = self.db.get_headers()
            body = json.dumps({ 'headers': headers })

            response.content_type = 'application/json'
            if 'gzip' in request.headers.get('Accept-Encoding', []):
                response.add_header("Content-Encoding", "gzip")

                s = StringIO.StringIO()
                f = gzip.GzipFile(fileobj=s, mode='w')
                f.write(body)
                f.flush()
                f.close()

                return f.getvalue()
            else:
                return body

        @app.route("/utils/kill_proc/<id>", method=['POST'])
        def kill_proc(id):
            from bottle import request
            data = json.loads(request.body.read())

            # check if id known to us
            c = self.db.conn.cursor()
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

        @app.route("/utils/drop_ids", method=['POST'])
        def drop_ids():
            from bottle import request
            data = json.loads(request.body.read())
            ids = data["ids"]

            with self.db.conn as db:
                for id in ids:
                    db.execute("DELETE FROM Monitoring WHERE id= ?", (id, ))

            self.db.get_headers(reload=True)
            return "Deleted %s rows!" % len(ids)

        @app.route("/utils/show_log/<id>", method=['GET', 'POST'])
        def show_log(id):
            c = self.db.conn.cursor()
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



def run_web_greenlet(db, host="0.0.0.0", port=9215, **kwargs):
    listener = (host, port, )

    from ws4py.server.geventserver import WSGIServer, WebSocketWSGIHandler
    from ws4py.server.wsgiutils import WebSocketWSGIApplication
    from ws4py.websocket import EchoWebSocket

    SyncSocket.db = db

    static_app = WebServer(db = db)
    static_app.mount('/sync', WebSocketWSGIApplication(handler_cls = SyncSocket))

    server = WSGIServer(listener, static_app)

    log.info("Using db: %s." % (db.db_str))
    log.info("Started web server at [%s]:%d" % (host, port))
    log.info("Go to http://%s:%d/" % (socket.gethostname(), port))

    server.serve_forever()

import gevent
import struct

def run_socket_greenlet(db, sock):
    log.info("Started input listener: %s", sock)

    def recvall(sock, count):
        buf = b''
        while count:
            r = sock.recv(count)
            if not r: return None
            buf += r 
            count -= len(r)

        return buf

    def message_loop(cli_sock):
        while True:
            msg_size = recvall(cli_sock, 16)
            if msg_size is None: return

            msg_size = struct.unpack("!Q", msg_size.decode("hex"))[0]
            body = recvall(cli_sock, msg_size)
            if body is None: return

            yield body

    def handle_conn(cli_sock):
        try:
            # log.info("Accepted input connection: %s", cli_sock)

            gen = message_loop(cli_sock)
            db.direct_transactional_upload(gen)
        finally:
            cli_sock.close()

    sock.listen(15)
    while True:
        cli, addr = sock.accept()
        gevent.spawn(handle_conn, cli)

@fff_dqmtools.fork_wrapper(__name__)
@fff_dqmtools.lock_wrapper
def __run__(opts, **kwargs):
    global log
    log = kwargs["logger"]
    sock = kwargs["lock_socket"]

    db_string = opts["web.db"]
    port = opts["web.port"]

    db = Database(db = db_string)

    fweb = WebServer(db = db)

    fwt = gevent.spawn(run_web_greenlet, db, port = port)
    fsl = gevent.spawn(run_socket_greenlet, db, sock)

    gevent.joinall([fwt, fsl], raise_error=True)

if __name__ == "__main__":
    print "unrecable"
    db = sqlite3.connect("./db.sqlite3")
    w = WebServer(db=db)

    #w.run_greenlet(port=9315)
    w.run_test()
