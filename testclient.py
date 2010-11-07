#!/usr/bin/python

import socket
import select

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost",20000))

osocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
osocket.connect("client.ocaml2py.sock")

running = True

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
