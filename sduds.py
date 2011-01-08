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
from webserver import WebServer

###

RESPONSIBILITY_TIME_SPAN = 3600*24*3
RESUBMISSION_INTERVAL = 3600*24*3

###

class InvalidCaptchaSignatureError(Exception): pass

class TooFrequentResubmissionError(Exception): pass

###

class SDUDS:
    def __init__(self, server_address, suffix="", erase=False):
        self.partnerdb = partners.Database(suffix, erase=erase)
        self.entrydb = entries.Database(suffix, erase=erase)

        self.hashtrie = HashTrie(suffix, erase=erase)
        self.logger = logging.getLogger("sduds"+suffix) 

        interface, port = server_address
        self.webserver = WebServer(self.entrydb, interface, port)
        self.webserver.start()

        self.synchronization_socket = None

    def run_synchronization_server(self, interface, port):
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        serversocket.bind((interface, port))
        serversocket.listen(5)
        self.logger.info("Listening on %s:%d." % (interface, port))

        self.webserver.set_synchronization_port(port)

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
        partner_name = f.readline().strip()
        password = f.readline().strip()
        f.close()

        client = partners.Client.from_database(self.partnerdb, partner_name=partner_name)

        if not client:
            self.logger.warning("Rejected synchronization attempt from %s (%s) - unknown partner." % (partner_name, str(address)))
            clientsocket.close()
            return False

        if client.kicked():
            self.logger.warning("Rejected synchronization attempt from kicked client %s (%s)." % (partner_name, str(address)))
            clientsocket.close()
            return False
            
        if not client.password_valid(password):
            self.logger.warning("Rejected synchronization attempt from %s (%s) - wrong password." % (partner_name, str(address)))
            clientsocket.close()
            return False

        self.logger.debug("%s (from %s) authenticated successfully." % (partner_name, str(address)))
        clientsocket.sendall("OK\n")

        hashlist = self.hashtrie.synchronize_with_client(clientsocket)

        try:
            self.fetch_entries_from_partner(hashlist, client)
        except partners.PartnerKickedError:
            self.logger.debug("Client %s got kicked." % str(client))

        client.log_conversation(len(hashlist))

    def fetch_entries_from_partner(self, hashlist, partner):
        address = (partner.host, partner.port)

        # get database entries
        try:
            entrylist = entries.EntryList.from_server(hashlist, address)
        except IOError, error:
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
        serversocket = server.authenticated_synchronization_socket()
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

        try:
            # retrieve entry
            entry = entries.Entry.from_webfinger_address(webfinger_address, submission_timestamp)
        except Exception, e: # TODO: better error handling
            self.logger.debug("error retrieving %s: %s" % (webfinger_address, str(e)))

            # if online profile invalid/non-existant, delete the database entry
            binhash = self.entrydb.delete_entry(webfinger_address=webfinger_address)
            if not binhash==None:
                self.hashtrie.delete([binhash])

            return None

        # check captcha signature
        if not entry.captcha_signature_valid():
            raise InvalidCaptchaSignatureError, "%s is not a valid signature for %s" % (binascii.hexlify(entry.captcha_signature), webfinger_address)

        # delete old entry
        old_entry = entries.Entry.from_database(self.entrydb, webfinger_address=webfinger_address)

        if old_entry:
            # prevent too frequent resubmission
            if submission_timestamp < old_entry.submission_timestamp + RESUBMISSION_INTERVAL:
                raise TooFrequentResubmissionError, "Resubmission of %s failed because it was submitted too recently (timestamps: %d/%d)" % (webfinger_address, submission_timestamp, old_entry.submission_timestamp)

            binhash = self.entrydb.delete_entry(hash=old_entry.hash)
            assert not binhash==None
            self.hashtrie.delete([binhash])

        # add new entry
        entrylist = entries.EntryList([entry])

        hashes = entrylist.save(self.entrydb)
        self.hashtrie.add(hashes)

        assert len(hashes)==1
        binhash = hashes[0]

        return binhash

    def close(self):
        self.hashtrie.close()
        self.webserver.terminate()
        
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
    """

    import optparse, sys

    parser = optparse.OptionParser(
        usage = "%prog  [-p WEBSERVER_PORT] [-s SYNCHRONIZATION_PORT] [PARTNER]",
        description="run a sduds server or connect manually to another one"
    )
    
    parser.add_option( "-p", "--webserver-port", metavar="PORT", dest="webserver_port", help="the webserver port of the own server")
    parser.add_option( "-s", "--synchronization-port", metavar="PORT", dest="synchronization_port", help="the synchronization port of the own server")

    (options, args) = parser.parse_args()

    try:
        webserver_port = int(options.webserver_port)
    except TypeError:
        webserver_port = 20000
    except ValueError:
        print >>sys.stderr, "Invalid webserver port."
        sys.exit(1)

    try:
        synchronization_port = int(options.synchronization_port)
    except TypeError:
        synchronization_port = webserver_port + 1
    except ValueError:
        print >>sys.stderr, "Invalid synchronization port."
        sys.exit(1)

    if len(args)>0:
        ### initiate connection if a partner is passed
        try:
            partner_name, = args
        except ValueError:
            print >>sys.stderr, "Invalid number of arguments."
            sys.exit(1)

        # create SDUDS instance
        sduds = SDUDS(("localhost", webserver_port))

        # synchronize with another server
        server = partners.Server.from_database(sduds.partnerdb, partner_name=partner_name)

        if not server:
            print >>sys.stderr, "Unknown server - add it with partners.py."
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

    else:
        ### otherwise simply run a server
        interface = "localhost"

        # start servers
        sduds = SDUDS((interface, webserver_port))

        # define exitfunc
        def exit():
            global sduds
            sduds.close()
            sys.exit(0)

        sys.exitfunc = exit

        # call exitfunc also for signals
        import signal

        def signal_handler(sig, frame):
            print >>sys.stderr, "Terminated by signal %d." % sig
            exit()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGHUP, signal_handler)

        # run the synchronization server
        sduds.run_synchronization_server(interface, synchronization_port)
