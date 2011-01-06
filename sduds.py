#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)

import threading
import os, socket, select, time

import random

import binascii

import partners
import entries

from hashtrie import HashTrie

###

RESPONSIBILITY_TIME_SPAN = 3600*24*3

###

class InvalidCaptchaSignatureError(Exception): pass

###

class SDUDS:
    def __init__(self, entryserver_address, suffix="", erase=False):
        self.partnerdb = partners.Database(suffix, erase=erase)
        self.entrydb = entries.Database(suffix, erase=erase)

        self.hashtrie = HashTrie(suffix, erase=erase)
        self.logger = logging.getLogger("sduds"+suffix) 

        entryserver_interface, entryserver_port = entryserver_address
        self.entry_server = entries.EntryServer(self.entrydb, entryserver_interface, entryserver_port)

        self.synchronization_socket = None

    def run_synchronization_server(self, interface, port):
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        serversocket.bind((interface, port))
        serversocket.listen(5)
        self.logger.info("Listening on %s:%d." % (interface, port))

        self.synchronization_socket = serversocket
        self.synchronization_address = (interface, port)

        while True:
            (clientsocket, address) = serversocket.accept()

            if not self.synchronization_socket: return
            
            thread = threading.Thread(target=self.handle_client, args=(clientsocket, address))
            thread.start()

    def handle_client(self, clientsocket, address):
        # authentication
        f = clientsocket.makefile()
        username = f.readline().strip()
        password = f.readline().strip()
        f.close()

        client = partners.Client.from_database(self.partnerdb, username=username)

        if not client:
            self.logger.warning("Rejected synchronization attempt from %s (%s) - unknown username." % (username, str(address)))
            clientsocket.close()
            return False

        if client.kicked():
            self.logger.warning("Rejected synchronization attempt from kicked client %s (%s)." % (username, str(address)))
            clientsocket.close()
            return False
            
        if not client.password_valid(password):
            self.logger.warning("Rejected synchronization attempt from %s (%s) - wrong password." % (username, str(address)))
            clientsocket.close()
            return False

        self.logger.debug("%s (from %s) authenticated successfully." % (username, str(address)))
        clientsocket.sendall("OK\n")

        hashlist = self.hashtrie.synchronize_with_client(clientsocket)

        try:
            self.fetch_entries_from_partner(hashlist, client)
        except partners.PartnerKickedError:
            self.logger.debug("Client %s got kicked." % str(client))

        client.log_conversation(len(hashlist))

    def fetch_entries_from_partner(self, hashlist, partner):
        entryserver_address = (partner.host, partner.entryserver_port)

        # get database entries
        entrylist = entries.EntryList.from_server(hashlist, entryserver_address)

        try:
            entrylist = entries.EntryList.from_server(hashlist, entryserver_address)
        except socket.error, error:
            offense = partners.ConnectionFailedOffense(error)
            partner.add_offense(offense)
        except entries.InvalidHashError, error:
            violation = partners.InvalidHashViolation(error)
            partner.add_violation(violation)
        except entries.InvalidListError, error:
            violation = partners.InvalidListViolation(error)
            partner.add_violation(violation)
        except entries.WrongEntriesError, error:
            violation = partners.WrongEntriesViolation(error)
            partner.add_violation(violation)        

        new_entries = []

        # take control samples
        for entry in entrylist:
            # verify captcha signatures
            if not entry.captcha_signature_valid():
                violation = partners.InvalidCaptchaViolation(entry)
                partner.add_violation(violation)
                entrylist.remove(entry)

            # verify that entry was retrieved after it was submitted
            if not entry.retrieval_timestamp>=entry.submission_timestamp:
                violation = partners.InvalidTimestampsViolation(entry)
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
                offense = partners.InvalidProfileOffense(address, error, guilty=responsible)
                partner.add_offense(offense)
                entrylist.remove(entry)

                # TODO: remove entry from own database

            if not entry_fetched.hash==entry.hash:
                offense = partners.NonConcurrenceOffense(entry_fetched, entry, guilty=responsible)
                partner.add_offense(offense)
                entrylist.remove(entry)

                new_entries.append(entry_fetched)

        # add valid entries to database
        entrylist.extend(new_entries)

        # add valid entries to database
        hashes = entrylist.save(self.entrydb)
        self.hashtrie.add(hashes)

    def connect_to_server(self, server):
        self.logger.debug("Connecting to server %s" % str(server))

        # try establishing connection
        serversocket = server.authenticated_socket()
        if not serversocket: return False

        self.logger.debug("Got socket for %s" % str(server))

        hashlist = self.hashtrie.synchronize_with_server(serversocket)

        server.log_conversation(len(hashlist))

        try:
            self.fetch_entries_from_partner(hashlist, server)
        except partners.PartnerKickedError:
            self.logger.debug("Server %s got kicked." % str(server))

    def submit_address(self, webfinger_address, submission_timestamp=None):
        if submission_timestamp==None:
            submission_timestamp = int(time.time())

        entry = entries.Entry.from_webfinger_address(webfinger_address, submission_timestamp)
        if not entry.captcha_signature_valid():
            raise InvalidCaptchaSignatureError, "%s is not a valid signature for %s" % (binascii.hexlify(entry.captcha_signature), entry.webfinger_address)

        entrylist = entries.EntryList([entry])

        hashes = entrylist.save(self.entrydb)
        self.hashtrie.add(hashes)

        return hashes

    def close(self):
        self.hashtrie.close()
        self.entry_server.terminate()
        
        if self.synchronization_socket:
            serversocket = self.synchronization_socket

            self.synchronization_socket = None

            # fake connection to unblock accept() in the run method
            fsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            fsocket.connect(self.synchronization_address)
            fsocket.close()

            serversocket.close()

    def erase(self):
        # no checking is needed if these are closed, as hashtrie check this by itself
        # and partnerdb and entrydb close the connections automatically
        self.hashtrie.erase()
        self.partnerdb.erase()
        self.entrydb.erase()

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

        # create SDUDS instance with an EntryServer
        sduds = SDUDS(("localhost", entryserver_port))

        # synchronize with another server
        address = (host, port)

        server = partners.Server.from_database(sduds.partnerdb, host=host, synchronization_port=port)

        if not server:
            print >>sys.stderr, "Address not in known servers list - add it with partners.py."
            sys.exit(1)

        if server.kicked():
            print >>sys.stderr, "Will not connect - server is kicked!"
            sys.exit(1)

        try:
            sduds.connect_to_server(server)
        except socket.error,e:
            print >>sys.stderr, "Connecting to %s failed: %s" % (str(server), str(e))
            sys.exit(1)

        sduds.close()
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

    # start servers
    sduds = SDUDS(("localhost", entryserver_port))

    import sys, signal

    # define exitfunc
    def exit():
        global sduds
        sduds.close()
        sys.exit(0)

    sys.exitfunc = exit

    # call exitfunc also for signals
    def signal_handler(signal, frame):
        print >>sys.stderr, "Terminated by signal %d." % signal
        exit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

    sduds.run_synchronization_server("localhost", 20000)
