#!/usr/bin/env python

import logging

import threading
import socket
import binascii

import addhashes

class HandleHashes(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

        logging.debug("Try to start a server on unix socket hashes.ocaml2py.sock.")
        serversocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        serversocket.bind("hashes.ocaml2py.sock")
        serversocket.listen(1)
        logging.debug("Started a server on unix socket hashes.ocaml2py.sock.")

        self.serversocket = serversocket

        self.start()

    def run(self):
        (clientsocket, address) = self.serversocket.accept()
        f = clientsocket.makefile()

        l = []

        for h in f:
            h = h[:-1]   # remove newline
            assert len(h)==32
            logging.debug("Received hash %s" % h)
            l.append(binascii.unhexlify(h))

        f.close()
        clientsocket.close()
        self.serversocket.close()
        logging.debug("Closed server socket on hashes.ocaml2py.sock.")

        # ...process list...
        logging.debug("...process the received hashes...")

        # add hashes to own trie
        logging.debug("Calling addhashes on the processed hashes.")
        addhashes.addhashes(l)
