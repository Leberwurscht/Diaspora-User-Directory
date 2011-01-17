#!/usr/bin/env python

import entries

import SocketServer, wsgiref.simple_server, cgi
import threading

import binascii

class ThreadingWSGIServer(SocketServer.ThreadingMixIn, wsgiref.simple_server.WSGIServer):
    allow_reuse_address = True

    def __init__(self, *args, **kwargs):
        wsgiref.simple_server.WSGIServer.__init__(self, *args, **kwargs)

class WebServer(threading.Thread):
    def __init__(self, entrydb, interface="", port=20000):
        threading.Thread.__init__(self)

        self.entrydb = entrydb
        self.httpd = wsgiref.simple_server.make_server(interface, port, self.dispatch, ThreadingWSGIServer)

        self._synchronization_port = None

    def run(self):
        self.httpd.serve_forever()

    def terminate(self):
        self.httpd.shutdown()
        self.httpd.socket.close()

    # set synchronization port
    def set_synchronization_port(self, synchronization_port):
        self._synchronization_port = synchronization_port

    # WSGI applications
    def dispatch(self, environment, start_response):
        if environment["PATH_INFO"]=="/entrylist":
            func = self.entrylist
        elif environment["PATH_INFO"]=="/synchronization_port":
            func = self.synchronization_port
        else:
            func = self.not_found

        for chunk in func(environment, start_response): yield chunk

    def entrylist(self, environment, start_response):
        fs = cgi.FieldStorage(fp=environment['wsgi.input'],
                          environ=environment,
                          keep_blank_values=1)

        start_response("200 OK", [('Content-Type','text/plain')])

        binhashes = []
        for hexhash in fs.getlist("hexhash"):
            binhash = binascii.unhexlify(hexhash)
            binhashes.append(binhash)

        entrylist = entries.EntryList.from_database(self.entrydb, binhashes)

        # serve requested hashes
        json_string = entrylist.json()
        yield json_string
        
    def synchronization_port(self, environment, start_response):
        if self._synchronization_port==None:
            start_response("404 Not Found", [("Content-type", "text/plain")])
            yield "Synchronization disabled."
        else:
            start_response("200 OK", [('Content-Type','text/plain')])
            yield str(self._synchronization_port)
        
    def not_found(self, environment, start_response):
        start_response("404 Not Found", [("Content-type", "text/plain")])
        yield "%s not found." % environment["PATH_INFO"]
