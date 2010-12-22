#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)

import threading
import os, socket, select

import random

import binascii

import partners
import entries

###

RESPONSIBILITY_TIME_SPAN = 3600*24*3

###

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

def process_hashes(hashlist, partner):
    entryserver_address = (partner.host, partner.entryserver_port)

    # get database entries
    entrylist = entries.EntryList.from_server(hashlist, entryserver_address)

    try:
        entrylist = entries.EntryList.from_server(hashlist, entryserver_address)
    except socket.error, error:
        offense = partners.ConnectionFailedOffense(error)
        partner.add_offense(offense)
    except InvalidHashError, error:
        violation = partners.InvalidHashViolation(error)
        partner.add_violation(violation)
    except InvalidListError, error:
        violation = partners.InvalidListViolation(error)
        partner.add_violation(violation)
    except WrongEntriesError, error:
        violation = partners.WrongEntriesViolation(error)
        partner.add_violation(violation)        

    new_entries = []

    # take control samples
    for entry in entrylist:
        # verify captcha signatures
        if not entry.captcha_signature_valid():
            violation = partners.InvalidCaptchaViolation("")
            partner.add_violation(violation)
            entrylist.remove(entry)

        # verify that entry was retrieved after it was submitted
        if not entry.retrieval_timestamp>entry.submission_timestamp:
            violation = partners.InvalidTimestampsViolation("")
            partner.add_violation(violation)
            entrylist.remove(entry)

        # the partner is only responsible if the entry was retrieved recently
        if entry.retrieval_timestamp > time.time()-RESPONSIBILITY_TIME_SPAN:
            responsible = True

            # Only re-retrieve the information with a certain probability
            if random.random()>partner.control_probability: continue
        else:
            responsible = False

            raise NotImplementedError, "Not implemented yet."
            # ... tell the admin that an old timestamp was encourtered, and track it to its origin to enable the admin to
            # shorten the chain of directory servers ...

        # re-retrieve the information
        address = entry.webfinger_address

        # try to get the profile
        try:
            entry_fetched = entries.Entry.from_webfinger_address(address, entry.submission_timestamp)
        except Exception, error:
            offense = InvalidProfileOffense(error, guilty=responsible)
            partner.add_offense(offense)
            entrylist.remove(entry)

            # TODO: remove entry from own database

        if not entry_fetched.hash==entry.hash:
            offense = NonConcurrenceOffense(entry_fetched, entry, guilty=responsible)
            partner.add_offense(offense)
            entrylist.remove(entry)

            new_entries.append(entry_fetched)

    # add valid entries to database
    entrylist.extend(new_entries)

    # add valid entries to database
    entrylist.save()

def handle_connection(hashserver, clientsocket, address):
    # authentication
    f = clientsocket.makefile()
    username = f.readline().strip()
    password = f.readline().strip()
    f.close()

    client = partners.Client.from_database(username=username)

    if not client:
        logging.warning("Rejected synchronisation attempt from %s (%s) - unknown username." % (username, str(address)))
        clientsocket.close()
        return False

    if client.kicked():
        logging.warning("Rejected synchronisation attempt from kicked client %s (%s)." % (username, str(address)))
        clientsocket.close()
        return False
        
    if not client.password_valid(password):
        logging.warning("Rejected synchronisation attempt from %s (%s) - wrong password." % (username, str(address)))
        clientsocket.close()
        return False

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

    try:
        process_hashes(hashlist, client)
    except partners.PartnerKickedException:
        logging.debug("Client %s got kicked." % str(client))

    client.log_conversation(len(hashlist))

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
    identifier = server.host+":"+str(server.synchronisation_port)
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

    try:
        process_hashes(hashlist, server)
    except partners.PartnerKickedException:
        logging.debug("Server %s got kicked." % str(server))

    server.log_conversation(len(hashlist))

if __name__=="__main__":
    """
    Command line interface.

    There are two ways to call this:
    - You can pass arguments HOST PORT [ENTRYSERVER_PORT] to connect to another
      server manually.
    - You can pass no argument or only ENTRYSERVER_PORT to wait for connections
      from other servers.

    The own EntryServer will run on the port ENTRYSERVER_PORT or on 20001 if
    ENTRYSERVER_PORT was not specified.
    """

    import sys, time

    # run the trieserver executable
    import trieserver

    # run hashserver
    hashserver = HashServer()

    if len(sys.argv)>2:
        # initiate connection if host and port are passed
        try:
            if len(sys.argv)==3:
                command,host,port = sys.argv
                entryserver_port = 20001
            else:
                command,host,port,entryserver_port = sys.argv
        except ValueError:
            print >>sys.stderr, "Pass host and port and (optionally) EntryServer port for manually initiating a connection."
            sys.exit(1)

        try:
            port = int(port)
        except ValueError:
            print >>sys.stderr, "Invalid port."
            sys.exit(1)

        try:
            entryserver_port = int(entryserver_port)
        except ValueError:
            print >>sys.stderr, "Invalid EntryServer port."
            sys.exit(1)

        # start EntryServer
        entryserver = entries.EntryServer("localhost", entryserver_port)

        # synchronize with another server
        address = (host, port)

        server = partners.Server.from_database(host=host, synchronisation_port=port)

        if not server:
            print >>sys.stderr, "Address not in known servers list - add it with partners.py."
            sys.exit(1)

        if server.kicked():
            print >>sys.stderr, "Will not connect - server is kicked!"
            sys.exit(1)

        try:
            connect(hashserver, server)
        except socket.error,e:
            print >>sys.stderr, "Connecting to %s failed: %s" % (str(server), str(e))
            sys.exit(1)

        # give trieserver some time to process the data
        time.sleep(3)

        sys.exit(0)

    # otherwise simply run a server
    interface = "localhost"

    if len(sys.argv)>1:
        try:
            entryserver_port = int(sys.argv[1])
        except ValueError:
            print >>sys.stderr, "Invalid EntryServer port."
            sys.exit(1)
    else:
        entryserver_port = 20001

    # start EntryServer
    entryserver = entries.EntryServer("localhost", entryserver_port)

    # start a server that waits for synchronisation attempts from others
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
