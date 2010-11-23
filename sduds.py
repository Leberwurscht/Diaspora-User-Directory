#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)

import threading
import os, socket, select

import binascii

import partners
import entries

class HashServer(threading.Thread):
    """ Creates a unix domain socket listening for incoming hashes from the
        synchronisation process. Provides a 'get' function that can be used
        to wait for hashes of a certain synchronisation identifier. """

    def __init__(self, address="hashes.ocaml2py.sock"):
        threading.Thread.__init__(self)

        if os.path.exists(address): os.remove(address)

        hashessocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        hashessocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        hashessocket.bind(address)
        hashessocket.listen(5)

        self.lock = threading.Lock() # lock for events and data dictionaries
        self.events = {} # identifiers -> event dict for "data ready" events
        self.data = {} # identifiers -> list of hashes dict

        self.hashessocket = hashessocket

        self.daemon = True  # terminate if main program exits
        self.start()

    def run(self):
        while True:
            (clientsocket, address) = self.hashessocket.accept()

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

def process_hashes(hashlist, entryserver_address):
    # get database entries
    entrylist = entries.EntryList.from_server(hashlist, entryserver_address)

    # verify captcha signatures
    for entry in entrylist:
        if not entry.captcha_signature_valid():
            entrylist.remove(entry)
            # ... kick the partner ...

    # ... take control samples ...

    # add valid entries to database
    entrylist.save()

def handle_connection(hashserver, clientsocket, address):
    # authentication
    f = clientsocket.makefile()
    username = f.readline().strip()
    password = f.readline().strip()
    f.close()

    if not username in partners.clients:
        logging.warning("Rejected synchronisation attempt from %s (%s) - unknown username." % (username, str(address)))
        clientsocket.close()
        return False
        
    if not partners.clients[username].password_valid(password):
        logging.warning("Rejected synchronisation attempt from %s (%s) - wrong password." % (username, str(address)))
        clientsocket.close()
        return False

    client = partners.clients[username]

    logging.debug("%s (from %s) authenticated successfully." % (username, str(address)))
    clientsocket.sendall("OK\n")

    # initialize the unix domain socket for communication with the ocaml component
    unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unix_socket.connect("server.ocaml2py.sock")

    # tell ocaml the username
    unix_socket.sendall(username+"\n")

    # forward traffic on network socket to unix domain socket
    logging.debug("Synchronising with %s" % str(address))
    link_sockets(clientsocket, unix_socket)
    logging.debug("Synchronising with %s" % str(address))

    # close sockets
    unix_socket.close()
    clientsocket.close()

    # await received hashes from the ocaml component (will block)
    logging.debug("Waiting for hashes for username %s" % username)
    hashlist = hashserver.get(username)

    process_hashes(hashlist, client.entryserver_address)

def connect(hashserver, server):
    logging.debug("Connecting to server %s" % str(server))

    # try establishing connection
    serversocket = server.authenticated_socket()
    if not serversocket: return False

    logging.debug("Got socket for %s" % str(server))

    # initialize the unix domain socket for communication with the ocaml component
    unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unix_socket.connect("client.ocaml2py.sock")

    # tell ocaml the identifier
    identifier = str(server.address)
    unix_socket.sendall(identifier+"\n")
    logging.debug("Told %s the identifier %s" % (str(server), identifier))

    # forward traffic on network socket to unix domain socket
    logging.debug("Synchronising with %s" % str(server))
    link_sockets(serversocket, unix_socket)
    logging.debug("Synchronising with %s done" % str(server))

    # close sockets
    unix_socket.close()
    serversocket.close()

    # await received hashes from the ocaml component (will block)
    logging.debug("Waiting for hashes for identifier %s" % identifier)
    hashlist = hashserver.get(identifier)

    process_hashes(hashlist, server.entryserver_address)

if __name__=="__main__":
    import os, sys, time, subprocess, signal

    # delete remaining unix domain sockets
    os.remove("server.ocaml2py.sock")
    os.remove("client.ocaml2py.sock")
    os.remove("add.ocaml2py.sock")

    # run trieserver
    trieserver = subprocess.Popen("./trieserver")

    # define exitfunc
    def exit():
        global trieserver

        trieserver.terminate()
        sys.exit(0)

    sys.exitfunc = exit

    # call exitfunc also for signals
    def signal_handler(signal, frame):
        print >>sys.stderr, "Server terminated by signal %d." % signal
        exit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

    # wait until all unix domain sockets are set up by trieserver
    while True:
        time.sleep(1)

        if not os.path.exists("server.ocaml2py.sock"): continue
        if not os.path.exists("client.ocaml2py.sock"): continue
        if not os.path.exists("add.ocaml2py.sock"): continue

        break

    # run hashserver and entryserver
    hashserver = HashServer()
    entryserver = entries.EntryServer()

    if len(sys.argv)>1:
        # initiate connection if host and port are passed
        try:
            command,host,port = sys.argv
        except ValueError:
            print >>sys.stderr, "Pass host and port for manually initiating a connection."
            sys.exit(1)

        try:
            port = int(port)
        except ValueError:
            print >>sys.stderr, "Invalid port."
            sys.exit(1)

        address = (host, port)

        if not address in partners.servers:
            print >>sys.stderr, "Address not in known servers list - add it with partners.py."
            sys.exit(1)

        server = partners.servers[address]

        try:
            connect(hashserver, server)
        except socket.error,e:
            print >>sys.stderr, "Connecting to %s failed: %s" % (str(server), str(e))
            sys.exit(1)

        time.sleep(3)
        sys.exit(0)

    # otherwise simply run a server
    interface = "localhost"
    port = 20000

    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serversocket.bind((interface, port))
    serversocket.listen(5)
    logging.info("Listening on %s:%d." % (interface, port))

    while True:
        (clientsocket, address) = serversocket.accept()
        thread = threading.Thread(target=handle_connection, args=(hashserver, clientsocket, address))
        thread.start()
