#!/usr/bin/env python

import logging

import threading
import binascii

import addhashes

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
            logging.debug("Received hash %s" % binascii.hexlify(h))
            l.append(h)

        f.close()
        clientsocket.close()
        self.serversocket.close()

        # ...process list...

        # add hashes to own trie
        addhashes.addhashes(l)
