#!/usr/bin/env python

import socket
import select

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
serversocket.bind(("localhost", 20000))
serversocket.listen(5)

while True:
    (clientsocket, address) = serversocket.accept()

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
