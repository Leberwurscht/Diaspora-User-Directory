#!/usr/bin/env python

import logging

import threading

def hexify(s):
    r = ""
    for i in s:
        r += "%x" % ord(i)
    return r

def addhashes(l):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect("client.ocaml2py.sock")

    for h in l:
        s.sendall(h+"\n")

    s.close()

class HandleHashes(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

        serversocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        serversocket.bind("hashes.ocaml2py.sock")
        serversocket.listen(1)

        self.serversocket = serversocket

        self.start()

    def run(self):
        (clientsocket, address) = serversocket.accept()
        f = clientsocket.makefile()

        l = []

        while True:
            h = f.read(17)
            assert h[-1]=='\n'
            h = h[:-1]   # remove newline
            logging.debug("Received hash %s" % hexify(h))
            l.append(h)

        f.close()
        clientsocket.close()
        self.serversocket.close()

        # ...process list...

        # add hashes to own trie
        addhashes(l)
