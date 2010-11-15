#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)

import socket
import select

import hashreceive

interface = "localhost"
port = 20000

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
serversocket.bind((interface, port))
serversocket.listen(5)
logging.info("Listening on %s:%d." % (interface, port))

while True:
    (clientsocket, address) = serversocket.accept()
    logging.info("Client %s connected." % str(address))

    try:
        hashreceive.HandleHashes()
    except:
        logging.warning("Busy. Rejected reconciliation attempt.")
        clientsocket.close()
        continue

    print "accepted", address

    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect("server.ocaml2py.sock")

    print "forwarding traffic via unix socket"

    inputsockets = [s,clientsocket]

    while inputsockets:
        inputready,outputready,exceptready = select.select(inputsockets,[],[])

        for sock in inputready:
            if sock==s:
                b = s.recv(1024)
                if not b:
                    inputsockets.remove(s)
                else:
                    clientsocket.sendall(b)
            elif sock==clientsocket:
                b = clientsocket.recv(1024)
                if not b:
                    inputsockets.remove(clientsocket)
                else:
                    s.sendall(b)

    clientsocket.close()
    s.close()
