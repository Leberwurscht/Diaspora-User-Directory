#!/usr/bin/env python

import socket

def addhashes(l):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect("add.ocaml2py.sock")

    for h in l:
        s.sendall(h+"\n")

    s.close()

if __name__=="__main__":
    import sys, binascii

    hashes = [binascii.unhexlify(i) for i in sys.argv[1:]]
    addhashes(hashes)
