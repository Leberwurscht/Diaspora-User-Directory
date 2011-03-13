#!/usr/bin/env python

import entries

import SocketServer, wsgiref.simple_server, cgi
import threading

import binascii, json

class ThreadingWSGIServer(SocketServer.ThreadingMixIn, wsgiref.simple_server.WSGIServer):
    allow_reuse_address = True

    def __init__(self, *args, **kwargs):
        wsgiref.simple_server.WSGIServer.__init__(self, *args, **kwargs)

class WebServer(threading.Thread):
    def __init__(self, context, interface="", port=20000):
        threading.Thread.__init__(self)

        self.context = context
        self.httpd = wsgiref.simple_server.make_server(interface, port, self.dispatch, ThreadingWSGIServer)

        self._synchronization_host = None

    def run(self):
        self.httpd.serve_forever()

    def terminate(self):
        self.httpd.shutdown()
        self.httpd.socket.close()

    # set synchronization port
    def set_synchronization_address(self, host, control_port):
        self._synchronization_host = host
        self._control_port = control_port

    # WSGI applications
    def dispatch(self, environment, start_response):
        if environment["PATH_INFO"]=="/":
            func = self.index
        elif environment["PATH_INFO"]=="/submit":
            func = self.submit
        elif environment["PATH_INFO"]=="/entrylist":
            func = self.entrylist
        elif environment["PATH_INFO"]=="/synchronization_address":
            func = self.synchronization_address
        else:
            func = self.not_found

        for chunk in func(environment, start_response): yield chunk

    def index(self, environment, start_response):
        start_response("200 OK", [('Content-Type','text/html')])

        yield '<form action="/submit" method="post">'
        yield '<input type="text" name="address">'
        yield '<input type="submit">'
        yield '</form>'

    def submit(self, environment, start_response):
        fs = cgi.FieldStorage(fp=environment['wsgi.input'],
                          environ=environment,
                          keep_blank_values=1)

        webfinger_address = fs.getvalue("address")


        success = self.context.process_submission(webfinger_address)

        if success:
            start_response("200 OK", [('Content-Type','text/plain')])
            yield "Added %s to queue." % webfinger_address
        else:
            start_response("500 Internal Server Error", [('Content-Type','text/plain')])
            yield "Queue is full - rejected %s." % webfinger_address

    def entrylist(self, environment, start_response):
        fs = cgi.FieldStorage(fp=environment['wsgi.input'],
                          environ=environment,
                          keep_blank_values=1)

        start_response("200 OK", [('Content-Type','text/plain')])

        binhashes = []
        for hexhash in fs.getlist("hexhash"):
            binhash = binascii.unhexlify(hexhash)
            binhashes.append(binhash)

        entrylist = entries.EntryList.from_database(self.context.entrydb, binhashes)

        # serve requested hashes
        json_string = entrylist.json()
        yield json_string
        
    def synchronization_address(self, environment, start_response):
        if self._synchronization_host==None:
            start_response("404 Not Found", [("Content-type", "text/plain")])
            yield "Synchronization disabled."
        else:
            start_response("200 OK", [('Content-Type','text/plain')])
            yield json.dumps((self._synchronization_host, self._control_port))
        
    def not_found(self, environment, start_response):
        start_response("404 Not Found", [("Content-type", "text/plain")])
        yield "%s not found." % environment["PATH_INFO"]
