#!/usr/bin/env python

import logging

import threading, socket
import os, binascii

import subprocess
import time, uuid, select

class HashServer(threading.Thread):
    """ Creates a unix domain socket listening for incoming hashes from the
        synchronisation process. Provides a 'get' function that can be used
        to wait for hashes of a certain synchronisation identifier. """

    def __init__(self, address):
        threading.Thread.__init__(self)

        self.address = address

        if os.path.exists(address): os.remove(address)

        hashessocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        hashessocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        hashessocket.bind(address)
        hashessocket.listen(5)

        self.lock = threading.Lock() # lock for events and data dictionaries
        self.events = {} # identifiers -> event dict for "data ready" events
        self.data = {} # identifiers -> list of hashes dict

        self.hashessocket = hashessocket

        self.running = True
        self.start()

    def run(self):
        while True:
            (clientsocket, address) = self.hashessocket.accept()

            if not self.running: return

            thread = threading.Thread(
                target=self.handle_connection,
                args=(clientsocket, address)
            )

            thread.start()

    def get_event(self, identifier):
        """ Create event for an identifier if it does not exist and return it,
            otherwise return existing one. Does not lock the dicts, you must
            do this yourself! """

        if not identifier in self.events:
            self.events[identifier] = threading.Event()

        return self.events[identifier]

    def handle_connection(self, clientsocket, address):
        f = clientsocket.makefile()

        # get identifier
        identifier = f.readline().strip()

        # receive hexhashes
        hashlist = []

        for hexhash in f:
            hexhash = hexhash.strip() # remove newline
            logging.debug("Hashserver received  %s from %s" % (hexhash, identifier))
            hashlist.append(binascii.unhexlify(hexhash))

        # save hashlist to data dictionary and notify get function
        with self.lock:
            self.data[identifier] = hashlist
            self.get_event(identifier).set()

        logging.debug("Data ready for %s in HashServer" % identifier)

    def get(self, identifier):
        """ waits until data is ready and returns it """

        self.lock.acquire()
        event = self.get_event(identifier)
        self.lock.release()

        event.wait()

        self.lock.acquire()
        hashlist = self.data[identifier]
        del self.data[identifier]
        del self.events[identifier]
        self.lock.release()

        logging.debug("Data for identifier %s collected from HashServer" % identifier)
        return hashlist

    def terminate(self):
        if not self.running: return

        self.running = False

        # fake connection to unblock accept() in handle_connection
        hsocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        hsocket.connect(self.address)
        hsocket.close()

        self.hashessocket.close()
        os.remove(self.address)

def link_sockets(socket1, socket2):
    """ Forwards traffic from each socket to the other, until one socket
        is closed. Will block until this happens. """

    sockets = [socket1, socket2]

    while True:
        # wait until at least one socket has data ready
        inputready,outputready,exceptready = select.select(sockets,[],[])

        for input_socket in inputready:
            # receive data
            buf = input_socket.recv(1024)
            if not buf: return # connection got closed

            # forward it to the other socket
            other_socket = sockets[ ( sockets.index(input_socket) + 1 ) % 2 ]
            other_socket.sendall(buf)

class HashTrie:
    def __init__(self, suffix=""):
        self.server_socket = "server"+suffix+".ocaml2py.sock"
        self.client_socket = "client"+suffix+".ocaml2py.sock"
        self.add_socket = "add"+suffix+".ocaml2py.sock"

        # delete remaining unix domain sockets
        if os.path.exists(self.server_socket): os.remove(self.server_socket)
        if os.path.exists(self.client_socket): os.remove(self.client_socket)
        if os.path.exists(self.add_socket): os.remove(self.add_socket)

        # run trieserver
        self.trieserver = subprocess.Popen(["./trieserver", suffix])

        # run HashServer
        self.hashserver = HashServer("hashes"+suffix+".ocaml2py.sock")

        # wait until all unix domain sockets are set up by trieserver
        while True:
            time.sleep(0.1)

            if not os.path.exists(self.server_socket): continue
            if not os.path.exists(self.client_socket): continue
            if not os.path.exists(self.add_socket): continue

            break

    def _synchronize_common(self, partnersocket, address):
        # connect to the unix domain socket the trieserver listens on
        logging.debug("connect to %s" % address)
        ocamlsocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        ocamlsocket.connect(address)

        # transmit an identifier to be able to get the right entries from the hashserver
        identifier = uuid.uuid4().hex
        ocamlsocket.sendall(identifier+"\n")
        logging.debug("sent identifier %s to %s" % (identifier, address))

        # forward traffic on network socket to unix domain socket
        link_sockets(partnersocket, ocamlsocket)

        # close sockets
        ocamlsocket.close()
        partnersocket.close()

        # await received hashes from the ocaml component (will block)
        logging.debug("Waiting for hashes for identifier %s on %s" % (identifier, address))
        hashlist = self.hashserver.get(identifier)
        logging.debug("Got %d hashes for identifier %s on %s" % (len(hashlist), identifier, address))

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

    def close(self):
        self.trieserver.terminate()
        self.hashserver.terminate()

        time.sleep(1.0)

        if os.path.exists(self.server_socket): os.remove(self.server_socket)
        if os.path.exists(self.client_socket): os.remove(self.client_socket)
        if os.path.exists(self.add_socket): os.remove(self.add_socket)
