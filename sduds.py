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

import SocketServer
import lib

###

RESPONSIBILITY_TIME_SPAN = 3600*24*3
RESUBMISSION_INTERVAL = 3600*24*3

###

class InvalidCaptchaSignatureError(Exception): pass

class TooFrequentResubmissionError(Exception): pass

###
# Authentication functionality

def authenticate_socket_to_partner(partnersocket, partner):
    # authenticate
    partnersocket.sendall(partner.identity+"\n")
    partnersocket.sendall(partner.password+"\n")

    f = partnersocket.makefile()
    answer = f.readline().strip()
    f.close()

    if answer=="OK":
        return True
    else:
        partnersocket.close()
        return False

class AuthenticatingRequestHandler(SocketServer.BaseRequestHandler):
    """ Tries to authenticate a partner and calls the method handle_partner in the case of success. Expects the server
        to have a context attribute. """

    def handle(self):
        context = self.server.context

        f = self.request.makefile()
        partner_name = f.readline().strip()
        password = f.readline().strip()
        f.close()

        partner = partners.Partner.from_database(context.partnerdb, partner_name=partner_name)

        if not partner:
            context.logger.warning("Rejected synchronization attempt from %s (%s) - unknown partner." % (partner_name, str(self.client_address)))
            return False

        if partner.kicked():
            context.logger.warning("Rejected synchronization attempt from kicked partner %s (%s)." % (partner_name, str(self.client_address)))
            return False
            
        if not partner.password_valid(password):
            context.logger.warning("Rejected synchronization attempt from %s (%s) - wrong password." % (partner_name, str(self.client_address)))
            return False

        self.request.sendall("OK\n")

        self.handle_partner(partner)

    def handle_partner(self, partner):
        raise NotImplementedError, "Override this function in subclasses!"

###

class SynchronizationControlServer(lib.BaseServer):
    """ The control server for synchronization, which is used to initiate the actual synchronization and to transmit
        the hashes that need to be deleted. The counterpart for this is SDUDS.exchange_hashes_with_partner. """

    def __init__(self, address, context):
        lib.BaseServer.__init__(self, address, SynchronizationControlRequestHandler)
        self.context = context

class SynchronizationControlRequestHandler(AuthenticatingRequestHandler):
    def handle_partner(self, partner):
        context = self.server.context

        context.logger.info("%s connected from %s to %s" % (str(partner), str(self.client_address), str(self.server)))

        socketfile = self.request.makefile()

        ### set up a socket for the synchronization control server of the partner

        # get synchronization port
        host, control_port, synchronization_port = partner.synchronization_address()

        # connect
        address = (host, synchronization_port)
        synchronization_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        synchronization_socket.connect(address)
        context.logger.debug("connection to %s (%s) established" % (str(partner), str(self.client_address)))

        # authenticate
        success = authenticate_socket_to_partner(synchronization_socket, partner)
        assert success

        context.logger.debug("successfully authenticated to %s" % str(partner))

        ### get the set of hashes the partner has but we haven't (conduct actual synchronization)
        context.logger.debug("conducting synchronization with %s" % str(partner))
        add_hashes = context.hashtrie.synchronize_with_server(synchronization_socket)
        context.logger.debug("Got %d hashes from %s" % (len(add_hashes), str(partner)))

        ### determine which hashes the partner must delete
        deleted_hashes = []

        for binhash in add_hashes:
            if context.entrydb.entry_deleted(binhash):
                add_hashes.remove(binhash)
                deleted_hashes.append(binhash)

        ### send these hashes to the partner
        for binhash in deleted_hashes:
            hexhash = binascii.hexlify(binhash)
            socketfile.write(hexhash+"\n")
        socketfile.write("\n")
        socketfile.flush()

        ### receive the hashes we should delete
        delete_hashes = []

        while True:
            hexhash = socketfile.readline().strip()
            if hexhash=="": break

            binhash = binascii.hexlify(hexhash)
            delete_hashes.append(binhash)

        context.process_hashes_from_partner(partner, add_hashes, delete_hashes)

###

class SynchronizationServer(lib.NotifyingServer):
    """ The actual synchronization server, which is only used to compute the hashes that
        are missing for each partner. The counterpart for this is implemented directly in
        SynchronizationControlRequestHandler.handle_partner. """

    def __init__(self, address, context):
        lib.NotifyingServer.__init__(self, address, SynchronizationRequestHandler)

        self.context = context

class SynchronizationRequestHandler(AuthenticatingRequestHandler):
    def handle_partner(self, partner):
        context = self.server.context
        context.logger.debug("%s connected for synchronization" % str(partner))
        binhashes = context.hashtrie.synchronize_with_client(self.request)
        context.logger.info("Got %d hashes after a synchronization request from %s" % (len(binhashes), str(partner)))
        self.server.set_data(partner.partner_name, binhashes)
        context.logger.debug("set_data called of %s" % str(partner))

###

class Context:
    """ A context is a collection of all necessary databases. It does not contain any synchronization
        code, that's the job of the SDUDS class. """
    def __init__(self, suffix="", erase=False):
        self.partnerdb = partners.Database(suffix, erase=erase)
        self.entrydb = entries.Database(suffix, erase=erase)
        self.hashtrie = HashTrie(suffix, erase=erase)

        self.logger = logging.getLogger("sduds"+suffix) 

    def close(self, erase=False):
        self.partnerdb.close(erase=erase)
        self.entrydb.close(erase=erase)
        self.hashtrie.close(erase=erase)

    def process_hashes_from_partner(self, partner, add_hashes, delete_hashes):
        """ This function can be called after synchronization with a partner. It will process
            the lists of hashes that should be added or deleted, get the missing entries from
            the web server of the partner, check if everything is valid, and update the own
            database. """

        # TODO: delete_hashes

        # get webserver address of the partner
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

class SDUDS:
    def __init__(self, server_address, suffix="", erase=False):
        self.context = Context(suffix, erase=erase)
#        self.partnerdb = partners.Database(suffix, erase=erase)
#        self.entrydb = entries.Database(suffix, erase=erase)
#
#        self.hashtrie = HashTrie(suffix, erase=erase)
#        self.logger = logging.getLogger("sduds"+suffix) 

        interface, port = server_address
        self.webserver = WebServer(self.context.entrydb, interface, port)
        self.webserver.start()

        self.synchronization_server = None
        self.control_server = None

    def run_synchronization_server(self, domain, interface="", control_port=20001, synchronization_port=20002):
        # set up servers
        self.synchronization_server = SynchronizationServer((interface, synchronization_port), self.context)
        self.control_server = SynchronizationControlServer((interface, control_port), self.context)

        # publish address so that partners can synchronize with us
        self.webserver.set_synchronization_address(domain, control_port, synchronization_port)

        # set up the server threads
        self.synchronization_thread = threading.Thread(target=self.synchronization_server.serve_forever)
        self.control_thread = threading.Thread(target=self.control_server.serve_forever)

        # run the servers
        self.synchronization_thread.start()
        self.control_thread.start()

#        self.control_server.serve_forever()

#        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#        serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#        serversocket.bind((interface, port))
#        serversocket.listen(5)
#        self.logger.info("Listening on %s:%d." % (interface, port))
#
#        self.webserver.set_synchronization_port(port)
#
#        self.synchronization_socket = serversocket
#        self.synchronization_address = (interface, port)
#
#        while True:
#            (clientsocket, address) = serversocket.accept()
#
#            if not self.synchronization_socket: return
#            
#            thread = threading.Thread(target=self.handle_client, args=(clientsocket, address))
#            thread.start()
#
#    def handle_client(self, clientsocket, address):
#        # authentication
#        f = clientsocket.makefile()
#        partner_name = f.readline().strip()
#        password = f.readline().strip()
#        f.close()
#
#        client = partners.Client.from_database(self.partnerdb, partner_name=partner_name)
#
#        if not client:
#            self.logger.warning("Rejected synchronization attempt from %s (%s) - unknown partner." % (partner_name, str(address)))
#            clientsocket.close()
#            return False
#
#        if client.kicked():
#            self.logger.warning("Rejected synchronization attempt from kicked client %s (%s)." % (partner_name, str(address)))
#            clientsocket.close()
#            return False
#            
#        if not client.password_valid(password):
#            self.logger.warning("Rejected synchronization attempt from %s (%s) - wrong password." % (partner_name, str(address)))
#            clientsocket.close()
#            return False
#
#        self.logger.debug("%s (from %s) authenticated successfully." % (partner_name, str(address)))
#        clientsocket.sendall("OK\n")
#
#        hashlist = self.hashtrie.synchronize_with_client(clientsocket)
#
#        try:
#            self.fetch_entries_from_partner(hashlist, client)
#        except partners.PartnerKickedError:
#            self.logger.debug("Client %s got kicked." % str(client))
#
#        client.log_conversation(len(hashlist))
#
#    def fetch_entries_from_partner(self, hashlist, partner):
#        # get deleted entries
#        deletedlist = []
#
#        for binhash in hashlist:
#            if self.entrydb.entry_deleted(binhash):
#                deletedlist.append(binhash)
#                hashlist.remove(binhash)
#
#            # Deleted entries should be pushed to the partner.
#            # But the partner needs to be able to verify that deletion is legal,
#            # for that the partner must have already fetched the entries.
#            # So I need a way to execute push-deletion after the partner requests
#            # the entries.


    def exchange_hashes_with_partner(self, partner):
        """ the client side for SynchronizationControlServer """

        # get the synchronization address
        host, control_port, synchronization_port = partner.synchronization_address()
        address = (host, control_port)

        # connect
        self.context.logger.info("Connecting to %s for synchronization with %s" % (str(address), str(partner)))
        partnersocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        partnersocket.connect(address)
        partnerfile = partnersocket.makefile()

        # authenticate
        self.context.logger.debug("Authenticating synchronization socket %s to partner %s" % (str(address), str(partner)))
        success = authenticate_socket_to_partner(partnersocket, partner)
        assert success

        # get the set of hashes the partner has but we haven't
        self.context.logger.debug("Getting hashes from %s" % str(partner))
        add_hashes = self.synchronization_server.get_data(partner.partner_name)
        self.context.logger.info("Got %d hashes from %s" % (len(add_hashes), str(partner)))

        # get the hashes that we should delete
        delete_hashes = []

        while True:
            hexhash = partnerfile.readline().strip()
            if hexhash=="": break

            binhash = binascii.hexlify(hexhash)
            delete_hashes.append(binhash)

        # determine which hashes the partner must delete
        deleted_hashes = []

        for binhash in add_hashes:
            if self.entrydb.entry_deleted(binhash):
                add_hashes.remove(binhash)
                deleted_hashes.append(binhash)

        # send these hashes to the partner
        for binhash in deleted_hashes:
            hexhash = binascii.hexlify(binhash)
            partnerfile.write(hexhash+"\n")
        partnerfile.write("\n")
        partnerfile.flush()

        return add_hashes, delete_hashes

    def synchronize_with_partner(self, partner):
#        self.logger.debug("Connecting to server %s" % str(server))
#
#        # try establishing connection
#        serversocket = server.authenticated_synchronization_socket()
#        if not serversocket: return False
#
#        self.logger.debug("Got socket for %s" % str(server))
#
#        hashlist = self.hashtrie.synchronize_with_server(serversocket)
        add_hashes, delete_hashes = self.exchange_hashes_with_partner(partner)

        partner.log_conversation(len(add_hashes), len(delete_hashes))

        try:
            self.context.process_hashes_from_partner(partner, add_hashes, delete_hashes)
#            self.fetch_entries_from_partner(hashlist, deletedlist, server)
        except partners.PartnerKickedError:
            self.logger.debug("Server %s got kicked." % str(partner))

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

    def terminate(self, erase=False):
        self.webserver.terminate()

        if self.synchronization_server:
            self.synchronization_server.terminate()
            self.synchronization_server = None

        if self.control_server:
            self.control_server.terminate()
            self.control_server = None

        self.context.close(erase=erase)

#        if self.synchronization_socket:
#            serversocket = self.synchronization_socket
#
#            self.synchronization_socket = None
#
#            # fake connection to unblock accept() in the run method
#            fsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#            fsocket.connect(self.synchronization_address)
#            fsocket.close()
#
#            serversocket.close()
#
#    def erase(self):
#        # no checking is needed if these are closed, as hashtrie check this by itself
#        # and partnerdb and entrydb close the connections automatically
#        self.hashtrie.erase()
#        self.partnerdb.erase()
#        self.entrydb.erase()

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
    parser.add_option( "-c", "--control-port", metavar="PORT", dest="control_port", help="the control port for synchronization of the own server")
    parser.add_option( "-s", "--synchronization-port", metavar="PORT", dest="synchronization_port", help="the actual synchronization port of the own server")

    (options, args) = parser.parse_args()

    try:
        webserver_port = int(options.webserver_port)
    except TypeError:
        webserver_port = 20000
    except ValueError:
        print >>sys.stderr, "Invalid webserver port."
        sys.exit(1)

    try:
        control_port = int(options.control_port)
    except TypeError:
        control_port = webserver_port + 1
    except ValueError:
        print >>sys.stderr, "Invalid control port."
        sys.exit(1)

    try:
        synchronization_port = int(options.synchronization_port)
    except TypeError:
        synchronization_port = control_port + 1
    except ValueError:
        print >>sys.stderr, "Invalid synchronization port."
        sys.exit(1)

    interface = "localhost"

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
        server = partners.Server.from_database(sduds.context.partnerdb, partner_name=partner_name)

        if not server:
            print >>sys.stderr, "Unknown server - add it with partners.py."
            sys.exit(1)

        if server.kicked():
            print >>sys.stderr, "Will not connect - server is kicked!"
            sys.exit(1)

        sduds.run_synchronization_server("localhost", interface, control_port, synchronization_port)

        try:
            sduds.synchronize_with_partner(server)
        except socket.error,e:
            print >>sys.stderr, "Connecting to %s failed: %s" % (str(server), str(e))
            sys.exit(1)

        sduds.terminate()

    else:
        ### otherwise simply run a server

        # start servers
        sduds = SDUDS((interface, webserver_port))

        # define exitfunc
        def exit():
            global sduds
            sduds.terminate()
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
        sduds.run_synchronization_server("localhost", interface, control_port, synchronization_port)

        # wait until program is interrupted
        while True: time.sleep(100)
