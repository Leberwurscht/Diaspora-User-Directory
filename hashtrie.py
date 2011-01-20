#!/usr/bin/env python

import logging

import threading, socket
import os, binascii

import subprocess
import time, uuid, select

import shutil

import SocketServer, lib

class HashServerRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        logger = self.server.logger

        f = self.request.makefile()

        # get identifier
        identifier = f.readline().strip()

        # receive hexhashes
        hashlist = set()

        for hexhash in f:
            hexhash = hexhash.strip() # remove newline
            logger.debug("Hashserver received  %s from %s" % (hexhash, identifier))
            hashlist.add(binascii.unhexlify(hexhash))

        # set the data for the identifier
        self.server.set_data(identifier, hashlist)

        logger.debug("Data ready for %s in HashServer" % identifier)

class HashServer(lib.NotifyingServer):
    address_family = socket.AF_UNIX

    def __init__(self, address):
        if os.path.exists(address): os.remove(address)

        lib.NotifyingServer.__init__(self, address, HashServerRequestHandler)

        self.logger = logging.getLogger(str(self.server_address))

def tunnel(partnersocket, ocamlsocket):
    """ Tunnels traffic from one socket over the other socket, so that the other end will
        know that the connection was closed on the socket to be tunneled without the other
        socket being closed. """

    sockets = [partnersocket, ocamlsocket]

    message_length = None
    message_buffer = ""

    while sockets:
        # wait until at least one socket has data ready
        inputready,outputready,exceptready = select.select(sockets,[],[])

        for input_socket in inputready:
            # receive data
            data = input_socket.recv(255)

            if input_socket==partnersocket:
                # add data to the buffer
                message_buffer += data

                while True:
                    # check if a message is announced
                    if message_length==None and len(message_buffer)>0:
                        message_length = ord(message_buffer[0])
                        message_buffer = message_buffer[1:]

                    # check if we can stop listening on partnersocket
                    if message_length==0:
                        sockets.remove(partnersocket)
                        break

                    # check if an announced message if fully transmitted
                    if message_length and len(message_buffer)>=message_length:
                        ocamlsocket.sendall(message_buffer[:message_length])
                        message_buffer = message_buffer[message_length:]
                        message_length = None
                    else:
                        break

            elif input_socket==ocamlsocket:
                announcement = chr(len(data))
                partnersocket.sendall(announcement+data)

                if announcement=="\0":
                    sockets.remove(ocamlsocket)

class HashTrie:
    def __init__(self, suffix="", erase=False):
        self.logger = logging.getLogger("hashtrie"+suffix)

        self.dbdir = "PTree"+suffix

        self.server_socket = "server"+suffix+".ocaml2py.sock"
        self.client_socket = "client"+suffix+".ocaml2py.sock"
        self.add_socket = "add"+suffix+".ocaml2py.sock"
        self.delete_socket = "delete"+suffix+".ocaml2py.sock"
        hashes_socket = "hashes"+suffix+".ocaml2py.sock"

        self.opened = False

        # erase database if requested

        if erase and os.path.exists(self.dbdir):
            shutil.rmtree(self.dbdir)

        # delete remaining unix domain sockets
        if os.path.exists(self.server_socket): os.remove(self.server_socket)
        if os.path.exists(self.client_socket): os.remove(self.client_socket)
        if os.path.exists(self.add_socket): os.remove(self.add_socket)
        if os.path.exists(self.delete_socket): os.remove(self.delete_socket)

        # a hashtrie is opened until close() is called
        self.opened = True

        # run trieserver
        self.trieserver = subprocess.Popen(["./trieserver", self.dbdir, self.server_socket, self.client_socket, self.add_socket, self.delete_socket, hashes_socket])

        # run HashServer
        self.hashserver = HashServer(hashes_socket)
        hashserver_thread = threading.Thread(target=self.hashserver.serve_forever)
        hashserver_thread.start()

        # wait until all unix domain sockets are set up by trieserver
        while True:
            time.sleep(0.1)

            if not os.path.exists(self.server_socket): continue
            if not os.path.exists(self.client_socket): continue
            if not os.path.exists(self.add_socket): continue
            if not os.path.exists(self.delete_socket): continue

            break

    def _synchronize_common(self, partnersocket, address):
        # connect to the unix domain socket the trieserver listens on
        self.logger.debug("connect to %s" % address)
        ocamlsocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        ocamlsocket.connect(address)

        # transmit an identifier to be able to get the right entries from the hashserver
        identifier = uuid.uuid4().hex
        ocamlsocket.sendall(identifier+"\n")
        self.logger.debug("sent identifier %s to %s" % (identifier, address))

        # forward traffic on network socket to unix domain socket
        tunnel(partnersocket, ocamlsocket)

        # close sockets
        ocamlsocket.close()

        # await received hashes from the ocaml component (will block)
        self.logger.debug("Waiting for hashes for identifier %s on %s" % (identifier, address))
        hashlist = self.hashserver.get_data(identifier)
        self.logger.debug("Got %d hashes for identifier %s on %s" % (len(hashlist), identifier, address))

        return hashlist

    def synchronize_with_server(self, serversocket):
        return self._synchronize_common(serversocket, self.client_socket)

    def synchronize_with_client(self, clientsocket):
        return self._synchronize_common(clientsocket, self.server_socket)

    def add(self, binhashes):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(self.add_socket)

        for binhash in binhashes:
            hexhash = binascii.hexlify(binhash)
            s.sendall(hexhash+"\n")

        s.close()

    def delete(self, binhashes):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(self.delete_socket)

        for binhash in binhashes:
            hexhash = binascii.hexlify(binhash)
            s.sendall(hexhash+"\n")

        s.close()

    def close(self, erase=False):
        if not self.opened: return

        self.trieserver.terminate()
        self.hashserver.terminate()

        time.sleep(1.0)

        if os.path.exists(self.server_socket): os.remove(self.server_socket)
        if os.path.exists(self.client_socket): os.remove(self.client_socket)
        if os.path.exists(self.add_socket): os.remove(self.add_socket)
        if os.path.exists(self.delete_socket): os.remove(self.delete_socket)

        self.opened = False

        if erase and os.path.exists(self.dbdir):
            shutil.rmtree(self.dbdir)
