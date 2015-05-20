#!/usr/bin/env python
import json
import sqlite3
import os, sys, time
import socket
import logging
import StringIO

import fff_dqmtools
import fff_cluster
import fff_filemonitor

# fff_dqmtools fixed the imports for us
import bottle
import zlib
import itertools

log = logging.getLogger(__name__)

from ws4py.websocket import WebSocket

class Database(object):
    def __init__(self, db=None):
        self.db_str = db

        if not self.db_str:
            self.db_str = ":memory:"

        self.listeners = []
        self.conn = sqlite3.connect(self.db_str)

        # create tables if none
        self.create_tables()

    def drop_tables(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS Headers")
        cur.execute("DROP TABLE IF EXISTS Documents")

        self.conn.commit()
        cur.close()

    def create_tables(self):
        cur = self.conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS Headers (
            id TEXT PRIMARY KEY NOT NULL,
            rev INT NOT NULL,

            timestamp TIMESTAMP,
            hostname TEXT,
            type TEXT,
            tag  TEXT,
            run  INT,
            FOREIGN KEY(id) REFERENCES Documents(id)
        )""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS Documents (
            id TEXT PRIMARY KEY NOT NULL,
            rev INT,
            body BLOB
        )""")

        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS M_rev_index ON Headers (rev)")
        cur.execute("CREATE INDEX IF NOT EXISTS M_timestamp_index ON Headers (timestamp)")

        self.conn.commit()
        cur.close()

    def make_header(self, doc, rev=None, write_back=False):
        header = {
            "_id":          doc.get("_id"),
            "_rev":         doc.get("_rev", rev),
            "timestamp":    doc.get("timestamp", doc.get("report_timestamp", time.time())),
            "hostname":     doc.get("hostname", None),
            "type":         doc.get("type", None),
            "tag":          doc.get("tag", None),
            "run":          doc.get("run", None),
        }

        if write_back:
            for k, v in header.items():
                doc[k] = v

        return header

    def make_header_from_entry(self, dct):
        header = dict(dct)
        header["_id"] = header["id"]
        header["_rev"] = header["rev"]

        del header["id"]
        del header["rev"]

        return header

    def prepare_docs(self, c, decode=True):
        columns = list(map(lambda x: x[0], c.description))
        body_column = columns.index("body")

        for x in c.fetchall():
            body = x[body_column]

            if decode:
                body = zlib.decompress(body)
                body = json.loads(body)

            yield body

    def prepare_headers(self, c):
        columns = list(map(lambda x: x[0], c.description))

        for x in c.fetchall():
            hit = dict(zip(columns, x))
            hit = self.make_header_from_entry(hit)

            yield hit

    def get_headers(self, reload=False, from_rev=None):
        with self.conn as db:
            c = db.cursor()
            if from_rev is None:
                c.execute("SELECT id, rev, timestamp, type, hostname, tag, run FROM Headers ORDER BY rev ASC")
            else:
                from_rev = long(from_rev)
                c.execute("SELECT id, rev, timestamp, type, hostname, tag, run FROM Headers WHERE rev > ? ORDER BY rev ASC", (from_rev, ))

            headers = list(self.prepare_headers(c))
            c.close()

        #self.find_first_rev(3600*24*16)

        return headers

    def find_first_rev(self, seconds):
        x = time.time() - seconds
        with self.conn as db:
            c = db.cursor()
            c.execute("SELECT rev, timestamp FROM Headers WHERE timestamp >= ? ORDER BY rev ASC LIMIT 1", (x, ))
            r =  c.fetchone()

            log.info("Shit %s", r);
            c.close()

    def direct_transactional_upload(self, bodydoc_generator):
        headers = [] # this is used to notify websockets
        with self.conn as db:
            rev = None

            def get_last_rev():
                cur = db.cursor()
                x = cur.execute("SELECT MAX(rev) FROM Headers")
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

                # create the header and update the body
                header = self.make_header(doc, rev=rev, write_back=True)
                body = json.dumps(doc).encode("zlib")

                db.execute("INSERT OR REPLACE INTO Headers (id, rev, timestamp, type, hostname, tag, run) VALUES (?, ?, ?, ?, ?, ?, ?)", (
                    header.get("_id"),
                    header.get("_rev"),
                    header.get("timestamp"),
                    header.get("type"),
                    header.get("hostname"),
                    header.get("tag"),
                    header.get("run"),
                ))

                db.execute("INSERT OR REPLACE INTO Documents (id, rev, body) VALUES (?, ?, ?)", (
                    header.get("_id"),
                    header.get("_rev"),
                    sqlite3.Binary(body),
                ))

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

    def closed(self, code, reason=None, output_log=True):
        self.db.remove_listener(self)

        if output_log:
            log.info("WebSocket disconnected: %s code=%s reason=%s", self.peer_address, code, reason)

    def received_message(self, msg):
        #print "recv:", self, msg, type(msg)
        #sys.stdout.flush()

        jsn = json.loads(msg.data)
        if jsn["event"] == "sync_request":
            known_rev = jsn.get("known_rev", None)
            self.state = self.STATE_INSYNC

            log.info("WebSocket client (%s) requested sync from rev %s", self.peer_address, known_rev)

            # if know_rev is not zero, we have to send at least a single header
            # to let the web interface to know it is synchronized
            if known_rev is not None:
                known_rev = long(known_rev) - 1


            # send the current state
            # if any changes happen during this time
            # they go into backlog
            self.backlog.insert(0, self.db.get_headers(from_rev=known_rev))
            while len(self.backlog):
                h = self.backlog.pop(0)
                self.sendHeaders(h)

            self.state = self.STATE_LISTEN

        if jsn["event"] == "request_documents":
            ids = set(jsn["ids"])

            with self.db.conn as db:
                c = db.cursor()

                IN = "(" + ",".join("?"*len(ids)) + ")"
                c.execute("SELECT * FROM Documents WHERE id IN " + IN, list(ids))

                docs = list(self.db.prepare_docs(c))
                c.close()

            jsn = json.dumps({
                'event': 'update_documents',
                'documents': docs,
            }).encode("utf8")

            log.info("WebSocket client (%s) requested %d documents (%d bytes)", self.peer_address, len(ids), len(jsn))
            self.send(jsn, False)

    def sendHeaders(self, headers):
        if not headers:
            return

        # split sending into multiple messages
        # this should be extremely helpful with users on bad connections
        cp = list(headers)

        total_avail = len(cp)
        total_sent = 0

        max_size = 1000
        last_rev = cp[-1]["_rev"]

        while cp:
            to_send, cp = cp[:max_size], cp[max_size:]
            total_sent += len(to_send)

            self.send(json.dumps({
                'event': 'update_headers',
                'rev': [to_send[0]["_rev"], to_send[-1]["_rev"]],
                'sync_to_rev': last_rev,
                'headers': to_send,

                'total_sent': total_sent,
                'total_avail': total_avail,
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

    @staticmethod
    def proxy_mode(input_messages, peer_address):
        # emulate a websocket
        # create it, parse messages and capture everything we send to it
        # we don't need to suppress self.db.add_listener as it has no effect
        # there should be no IO inside here

        # both input and output messages are strings (not json objects)
        output_messages = []

        class Proxy(SyncSocket):
            def __init__(self, peer_address):
                self._peer_address = peer_address

            def send(self, msg, binary=False):
                output_messages.append(msg)

            @property
            def peer_address(self):
                return self._peer_address

        class ProxyMessage(object):
            def __init__(self, str):
                self.data = str

        c = Proxy(peer_address)
        c.opened()
        for msg in input_messages:
            log.info("Proxy mode insert msg: %s", msg);
            c.received_message(ProxyMessage(msg))

        c.closed(code=1006, reason="Proxy mode end.", output_log=False)

        return output_messages

class WebServer(bottle.Bottle):
    def __init__(self, db=None):
        bottle.Bottle.__init__(self)

        self.db = db
        self.setup_routes()

    def setup_routes(self):
        app = self

        static_path = os.path.dirname(__file__)
        static_path = os.path.join(static_path, "../web.static/")

        # the decorator to enable cross domain communication
        # for http-proxy stuff
        # from: http://paulscott.co.za/blog/cors-in-python-bottle-framework/
        def enable_cors(fn):
            from bottle import request, response

            def _enable_cors(*args, **kwargs):
                # set CORS headers
                response.headers['Access-Control-Allow-Origin'] = '*'
                response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
                response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

                if bottle.request.method != 'OPTIONS':
                    # actual request; reply with the actual response
                    return fn(*args, **kwargs)

            return _enable_cors

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

        ### @app.route("/get/<id>", method=['GET', 'POST'])
        ### def get_id(id):
        ###     from bottle import request, response
        ###     # check if id known to us
        ###     with self.db.conn as db:
        ###         c = db.cursor()
        ###         c.execute("SELECT * FROM Documents WHERE id = ?", (id, ))
        ###         docs = list(self.db.prepare_docs(c, decode=False))
        ###         c.close()

        ###         if len(docs) == 0:
        ###             raise bottle.HTTPResponse("Doc id not found.", status=404)

        ###         response.content_type = 'application/json'
        ###         if 'deflate' in request.headers.get('Accept-Encoding', []):
        ###             response.add_header("Content-Encoding", "deflate")

        ###             return docs[0]
        ###         else:
        ###             return zlib.decompress(docs[0])

        ### @app.get("/headers/cached/")
        ### def headers():
        ###     from bottle import request, response
        ###     headers = self.db.get_headers()
        ###     body = json.dumps({ 'headers': headers })

        ###     response.content_type = 'application/json'
        ###     if 'deflate' in request.headers.get('Accept-Encoding', []):
        ###         response.add_header("Content-Encoding", "deflate")

        ###         return body.encode("zlib")
        ###     else:
        ###         return body

        @app.route("/utils/kill_proc/<id>", method=['POST'])
        def kill_proc(id):
            from bottle import request
            data = json.loads(request.body.read())

            # check if id known to us
            c = self.db.conn.cursor()
            c.execute("SELECT body FROM Documents WHERE id = ?", (id, ))
            doc = list(self.db.prepare_docs(c))
            c.close()

            if not doc:
                raise bottle.HTTPResponse("Process not found.", status=404)

            b = doc[0]
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
                    db.execute("DELETE FROM Headers WHERE id= ?", (id, ))
                    db.execute("DELETE FROM Documents WHERE id= ?", (id, ))

            self.db.get_headers(reload=True)
            return "Deleted %s rows!" % len(ids)

        def verify_logfile(fn):
            if not fn:
                raise bottle.HTTPResponse("Log entry not found, invalid esMonitoring.py?", status=500)

            fn = os.path.realpath(fn)
            allowed = ["/var/log/hltd/pid/"]

            for p in allowed:
                if os.path.commonprefix([fn, p]) == p:
                    return fn

            raise bottle.HTTPResponse("Log file is in a weird location, access denied.", status=401)

        def decode_zlog(fn):
            with open(fn, "rb") as f:
                decoder = zlib.decompressobj(16+zlib.MAX_WBITS)

                while True:
                    part = os.read(f.fileno(), 1024*1024)
                    if len(part) == 0:
                        break

                    yield decoder.decompress(part)

                yield decoder.flush()
                if decoder.unused_data:
                    yield u"<!-- gzip stream unfinished, bytes in buffer: %d -->\n" % len(decoded.unused_data)

        def decode_log(fn):
            with open(fn, "rb") as f:
                while True:
                    part = os.read(f.fileno(), 1024*1024)
                    if len(part) == 0:
                        break

                    yield part

        @app.route("/utils/show_log/<id>", method=['GET', 'POST'])
        def show_log(id):
            from bottle import response

            c = self.db.conn.cursor()
            c.execute("SELECT body FROM Documents WHERE id = ?", (id, ))
            doc = list(self.db.prepare_docs(c))
            c.close()

            b = doc[0]

            startup_fn = b.get("stdout_fn", None)
            startup_iter = []
            if startup_fn:
                startup_iter = decode_log(verify_logfile(startup_fn))

            gzip_fn = b.get("stdlog_gzip", None)
            gzip_iter = []
            if gzip_fn:
                gzip_iter = decode_zlog(verify_logfile(gzip_fn))

            response.add_header('Content-Type', 'text/plain; charset=UTF-8')
            chain = itertools.chain(startup_iter, gzip_iter)

            #return "".join(chain)
            return chain

        ### @app.route("/sync_proxy_get/<messages>", method=["GET"])
        ### def sync_proxy_get(messages):
        ###     # this is for debugging, msg is base64,base64,base64
        ###     from bottle import request, response

        ###     lst = map(lambda x: x.decode("base64"), messages.split(","))
        ###     output = SyncSocket.proxy_mode(lst, peer_address=request.remote_addr)

        ###     response.content_type = 'application/json'
        ###     return json.dumps({
        ###         'messages': output
        ###     })

        @app.route("/sync_proxy", method=["OPTIONS", "POST"])
        @enable_cors
        def sync_proxy():
            from bottle import request, response

            data = json.loads(request.body.read())
            lst = data.get("messages", [])

            output = SyncSocket.proxy_mode(lst, peer_address=request.remote_addr)

            response.content_type = 'application/json'
            return json.dumps({
                'messages': output
            })


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
            #log.info("Accepted input connection: %s", cli_sock)

            # fetch all the documents before making a transaction
            gen = list(message_loop(cli_sock))
            db.direct_transactional_upload(gen)

            log.info("Accepted %d document(s) from input connection: %s", len(gen), cli_sock)
        finally:
            cli_sock.close()

    sock.listen(15)
    while True:
        cli, addr = sock.accept()
        gevent.spawn(handle_conn, cli)

@fff_dqmtools.fork_wrapper(__name__, uid="dqmpro", gid="dqmpro")
@fff_dqmtools.lock_wrapper
def __run__(opts, **kwargs):
    global log
    log = kwargs["logger"]
    sock = kwargs["lock_socket"]

    db_string = opts["web.db"]
    port = opts["web.port"]

    db = Database(db = db_string)

    fwt = gevent.spawn(run_web_greenlet, db, port = port)
    fsl = gevent.spawn(run_socket_greenlet, db, sock)

    gevent.joinall([fwt, fsl], raise_error=True)

if __name__ == "__main__":
    print "unrecable"
    db = sqlite3.connect("./db.sqlite3")
    w = WebServer(db=db)

    #w.run_greenlet(port=9315)
    w.run_test()
