#!/usr/bin/env python

import logging

import socket
import binascii

def addhashes(l):
    logging.debug("Connect to unix socket add.ocaml2py.sock.")
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect("add.ocaml2py.sock")

    for h in l:
        hexhash = binascii.hexlify(h)
        s.sendall(hexhash+"\n")
        logging.debug("Sent hash %s to unix socket add.ocaml2py.sock." % hexhash)

    s.close()
    logging.debug("Closed unix socket add.ocaml2py.sock.")

if __name__=="__main__":
    import sys

    logging.basicConfig(level=logging.DEBUG)

    hashes = [binascii.unhexlify(i) for i in sys.argv[1:]]
    addhashes(hashes)
