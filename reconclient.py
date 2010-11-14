#!/usr/bin/python

import logging
logging.basicConfig(level=logging.DEBUG)

import sys
import socket
import select

import hashreceive

def connect(host="localhost", port=20000):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host,port))

    osocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    osocket.connect("client.ocaml2py.sock")

    running = True

    try:
        hashreceive.HandleHashes()
    except:
        logging.debug("Connection to add.ocaml2py.sock failed; socket busy.")
        s.close()
        osocket.close()
        return False

    while running:
        inputready,outputready,exceptready = select.select([s,osocket],[],[])

        for sock in inputready:
            if sock==s:
                b = s.recv(1024)
                if not b:
                    running = False
                else:
                    osocket.sendall(b)
            elif sock==osocket:
                b = osocket.recv(1024)
                if not b:
                    running = False
                else:
                    s.sendall(b)

    osocket.close()
    s.close()

    return True
