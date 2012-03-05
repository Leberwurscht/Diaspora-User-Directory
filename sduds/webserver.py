#!/usr/bin/env python

import SocketServer, wsgiref.simple_server, cgi
import threading

import json

class ThreadingWSGIServer(SocketServer.ThreadingMixIn, wsgiref.simple_server.WSGIServer):
    allow_reuse_address = True

    def __init__(self, *args, **kwargs):
        wsgiref.simple_server.WSGIServer.__init__(self, *args, **kwargs)

class WebServer(threading.Thread):
    context = None
    httpd = None

    def __init__(self, context, interface="", port=20000):
        threading.Thread.__init__(self)

        self.context = context
        self.httpd = wsgiref.simple_server.make_server(interface, port, self.dispatch, ThreadingWSGIServer)

    def run(self):
        self.httpd.serve_forever()

    def terminate(self):
        self.httpd.shutdown()
        self.httpd.socket.close()

    # WSGI applications
    def dispatch(self, environment, start_response):
        if environment["PATH_INFO"]=="/":
            func = self.index
        elif environment["PATH_INFO"]=="/submit":
            func = self.submit
        elif environment["PATH_INFO"]=="/search":
            func = self.search
        elif environment["PATH_INFO"]=="/synchronization_address":
            func = self.synchronization_address
        else:
            func = self.not_found

        for chunk in func(environment, start_response): yield chunk

    def index(self, environment, start_response):
        start_response("200 OK", [('Content-Type','text/html')])

        yield '<h1>Submit entry</h1>'
        yield '<form action="/submit" method="post">'
        yield '<input type="text" name="address">'
        yield '<input type="submit">'
        yield '</form>'

        yield '<h1>Search entry</h1>'
        yield '<form action="/search" method="post" accept-charset="utf-8">'
        yield '<input type="text" name="words">'
        yield '<input type="submit">'
        yield '</form>'

    def submit(self, environment, start_response):
        fs = cgi.FieldStorage(fp=environment['wsgi.input'],
                          environ=environment,
                          keep_blank_values=1)

        webfinger_address = fs.getvalue("address")

        success = self.context.submit_address(webfinger_address)

        if success:
            start_response("200 OK", [('Content-Type','text/plain')])
            yield "Added %s to queue." % webfinger_address
        else:
            start_response("500 Internal Server Error", [('Content-Type','text/plain')])
            yield "Queue is full - rejected %s." % webfinger_address

    def search(self, environment, start_response):
        fs = cgi.FieldStorage(fp=environment['wsgi.input'],
                          environ=environment,
                          keep_blank_values=1)

        words = fs.getvalue("words")
        words = unicode(words, "utf-8")
        words = words.split()

        start_response("200 OK", [('Content-Type','text/plain')])

        for state in self.context.statedb.search(words):
            yield str(state)
            yield "\n"
        
    def synchronization_address(self, environment, start_response):
        try:
            fqdn_best, port = self.context.synchronization_address
            fqdn_alternative = environment["HTTP_HOST"].rsplit(":", 1)[0]
            fqdn_worst = environment["SERVER_NAME"]

            fqdn = fqdn_best or fqdn_alternative or fqdn_worst
            if not fqdn:
                start_response("404 Not Found", [("Content-type", "text/plain")])
                yield "Synchronization address cannot be determined."
                return

        except TypeError:
            start_response("404 Not Found", [("Content-type", "text/plain")])
            yield "Synchronization disabled."
        else:
            start_response("200 OK", [('Content-Type','text/plain')])
            yield json.dumps((fqdn, port))
        
    def not_found(self, environment, start_response):
        start_response("404 Not Found", [("Content-type", "text/plain")])
        yield "%s not found." % environment["PATH_INFO"]
