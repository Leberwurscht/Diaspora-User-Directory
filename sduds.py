#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)

import threading
import os, socket, select

import random

import binascii

import partners
import entries

### setup hashtrie
from hashtrie import HashTrie
import sys, signal

hashtrie = HashTrie()

# define exitfunc
def exit():
    global hashtrie

    hashtrie.close()
    sys.exit(0)

sys.exitfunc = exit

# call exitfunc also for signals
def signal_handler(signal, frame):
    print >>sys.stderr, "Terminated by signal %d." % signal
    exit()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)

### setup partners database
partnerdb = partners.Database()

###

RESPONSIBILITY_TIME_SPAN = 3600*24*3

###
        
def process_hashes(hashlist, partner):
    global hashtrie

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
            offense = InvalidProfileOffense(address, error, guilty=responsible)
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
    hashes = entrylist.save()
    hashtrie.add(hashes)

def handle_connection(clientsocket, address):
    global partnerdb

    # authentication
    f = clientsocket.makefile()
    username = f.readline().strip()
    password = f.readline().strip()
    f.close()

    client = partners.Client.from_database(partnerdb, username=username)

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

    hashlist = hashtrie.synchronize_with_client(clientsocket)

    try:
        process_hashes(hashlist, client)
    except partners.PartnerKickedException:
        logging.debug("Client %s got kicked." % str(client))

    client.log_conversation(len(hashlist))

def connect(server):
    global hashtrie

    logging.debug("Connecting to server %s" % str(server))

    # try establishing connection
    serversocket = server.authenticated_socket()
    if not serversocket: return False

    logging.debug("Got socket for %s" % str(server))

    hashlist = hashtrie.synchronize_with_server(serversocket)

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

        server = partners.Server.from_database(partnerdb, host=host, synchronisation_port=port)

        if not server:
            print >>sys.stderr, "Address not in known servers list - add it with partners.py."
            sys.exit(1)

        if server.kicked():
            print >>sys.stderr, "Will not connect - server is kicked!"
            sys.exit(1)

        try:
            connect(server)
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
        thread = threading.Thread(target=handle_connection, args=(clientsocket, address))
        thread.start()
