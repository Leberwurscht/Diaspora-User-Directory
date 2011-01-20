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

class SynchronizationServer(lib.BaseServer):
    """ The server partners can connect to for synchronizing. The hashes that must be added
        and the hashes that must be deleted are computed and context.process_hashes_from_partner
        is called. The counterpart for this is SDUDS.synchronize_with_partner. """

    def __init__(self, address, context):
        lib.BaseServer.__init__(self, address, SynchronizationRequestHandler)
        self.context = context

class SynchronizationRequestHandler(AuthenticatingRequestHandler):
    def handle_partner(self, partner):
        context = self.server.context

        context.logger.info("%s connected from %s to %s" % (str(partner), str(self.client_address), str(self.server)))

        ### get the set of hashes the partner has but we haven't (conduct actual synchronization)
        context.logger.debug("conducting synchronization with %s" % str(partner))
        add_hashes = context.hashtrie.synchronize_with_server(self.request)
        context.logger.info("Got %d hashes from %s" % (len(add_hashes), str(partner)))

        ### make file of socket
        socketfile = self.request.makefile()

        ### determine which hashes the partner must delete
        deleted_hashes = {}

        for binhash in add_hashes:
            retrieval_timestamp = context.entrydb.entry_deleted(binhash)

            if not retrieval_timestamp==None:
                deleted_hashes[binhash] = retrieval_timestamp

        add_hashes -= set(deleted_hashes)

        ### send these hashes to the partner
        for binhash,retrieval_timestamp in deleted_hashes.iteritems():
            hexhash = binascii.hexlify(binhash)
            socketfile.write(hexhash+" "+str(retrieval_timestamp)+"\n")
        socketfile.write("\n")
        socketfile.flush()

        ### receive the hashes we should delete
        delete_hashes = {}

        while True:
            try:
                hexhash, retrieval_timestamp = socketfile.readline().split()
            except ValueError:
                # got just a newline, so transmission is finished
                break

            binhash = binascii.unhexlify(hexhash)
            delete_hashes[binhash] = int(retrieval_timestamp)

        ### log the conversation
        partner.log_conversation(len(add_hashes), len(delete_hashes))

        ### call process_hashes_from_partner
        try:
            context.process_hashes_from_partner(partner, add_hashes, delete_hashes)
        except partners.PartnerKickedError:
            context.logger.debug("%s got kicked." % str(partner))

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

        self.logger.info("Start processing hashes - add_hashes: %d, delete_hashes: %d" % (len(add_hashes), len(delete_hashes)))

        # get database entries
        try:
            entrylist = entries.EntryList.from_server(add_hashes, partner.address)
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
                continue

                # TODO: remove entry from own database

            if not entry_fetched.hash==entry.hash:
                offense = partners.NonConcurrenceOffense(entry_fetched, entry, guilty=responsible)
                partner.add_offense(offense)
                entrylist.remove(entry)

                new_entries.append(entry_fetched)

        # add newly fetched entries to list
        entrylist.extend(new_entries)

        # add valid entries to database
        added_hashes, deleted_hashes, ignored_hashes = entrylist.save(self.entrydb)
        self.hashtrie.add(added_hashes)
        self.hashtrie.delete(deleted_hashes)

        for binhash in deleted_hashes:
            if binhash in delete_hashes:
                del delete_hashes[binhash]

        for binhash in ignored_hashes:
            if binhash in delete_hashes:
                del delete_hashes[binhash]

        # remove the entries for delete_hashes, taking control samples
        # TODO

    def process_submission(self, webfinger_address, submission_timestamp=None):
        if submission_timestamp==None:
            submission_timestamp = int(time.time())

        try:
            # retrieve entry
            entry = entries.Entry.from_webfinger_address(webfinger_address, submission_timestamp)
        except Exception, e: # TODO: better error handling
            self.logger.debug("error retrieving %s: %s" % (webfinger_address, str(e)))

            # if online profile invalid/non-existant, delete the database entry
            retrieval_timestamp = int(time.time())

            binhash = self.entrydb.delete_entry(retrieval_timestamp, webfinger_address=webfinger_address)
            if not binhash==None:
                self.hashtrie.delete([binhash])

            return None

        # check captcha signature
        if not entry.captcha_signature_valid():
            raise InvalidCaptchaSignatureError, "%s is not a valid signature for %s" % (binascii.hexlify(entry.captcha_signature), webfinger_address)

        # prevent too frequent resubmission
        # TODO: move to Entrylist.save
        old_entry = entries.Entry.from_database(self.entrydb, webfinger_address=webfinger_address)
        if old_entry:
            if submission_timestamp < old_entry.submission_timestamp + RESUBMISSION_INTERVAL:
                raise TooFrequentResubmissionError, "Resubmission of %s failed because it was submitted too recently (timestamps: %d/%d)" % (webfinger_address, submission_timestamp, old_entry.submission_timestamp)

        # add new entry
        entrylist = entries.EntryList([entry])

        add_hashes, delete_hashes, ignore_hashes = entrylist.save(self.entrydb)
        self.hashtrie.add(add_hashes)

        assert len(add_hashes)==1
        binhash, = add_hashes

        return binhash

class SDUDS:
    def __init__(self, webserver_address, suffix="", erase=False):
        self.context = Context(suffix, erase=erase)

        interface, port = webserver_address
        self.webserver = WebServer(self.context, interface, port)
        self.webserver.start()

        self.synchronization_server = None

    def run_synchronization_server(self, domain, interface="", synchronization_port=20001):
        # set up servers
        self.synchronization_server = SynchronizationServer((interface, synchronization_port), self.context)

        # publish address so that partners can synchronize with us
        self.webserver.set_synchronization_address(domain, synchronization_port)

        # set up the server threads
        self.synchronization_thread = threading.Thread(target=self.synchronization_server.serve_forever)

        # run the servers
        self.synchronization_thread.start()

    def synchronize_with_partner(self, partner):
        """ the client side for SynchronizationServer """

        # get the synchronization address
        host, synchronization_port = partner.synchronization_address()
        address = (host, synchronization_port)

        # connect
        self.context.logger.info("Connecting to %s for synchronization with %s" % (str(address), str(partner)))
        partnersocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        partnersocket.connect(address)

        # authenticate
        self.context.logger.debug("Authenticating synchronization socket %s to partner %s" % (str(address), str(partner)))
        success = authenticate_socket_to_partner(partnersocket, partner)
        assert success

        # get the set of hashes the partner has but we haven't
        self.context.logger.debug("Getting hashes from %s" % str(partner))
        add_hashes = self.context.hashtrie.synchronize_with_client(partnersocket)
        self.context.logger.info("Got %d hashes from %s" % (len(add_hashes), str(partner)))

        # make file of socket
        partnerfile = partnersocket.makefile()

        # get the hashes that we should delete
        delete_hashes = {}

        while True:
            try:
                hexhash, retrieval_timestamp = partnerfile.readline().split()
            except ValueError:
                # got just a newline, so transmission is finished
                break

            binhash = binascii.unhexlify(hexhash)
            delete_hashes[binhash] = int(retrieval_timestamp)

        # determine which hashes the partner must delete
        deleted_hashes = {}

        for binhash in add_hashes:
            retrieval_timestamp = self.context.entrydb.entry_deleted(binhash)
            
            if not retrieval_timestamp==None:
                deleted_hashes[binhash] = retrieval_timestamp

        add_hashes -= set(deleted_hashes)

        # send these hashes to the partner
        for binhash,retrieval_timestamp in deleted_hashes.iteritems():
            hexhash = binascii.hexlify(binhash)
            partnerfile.write(hexhash+" "+str(retrieval_timestamp)+"\n")
        partnerfile.write("\n")
        partnerfile.flush()

        ### log the conversation
        partner.log_conversation(len(add_hashes), len(delete_hashes))

        ### call process_hashes_from_partner
        try:
            self.context.process_hashes_from_partner(partner, add_hashes, delete_hashes)
        except partners.PartnerKickedError:
            self.context.logger.debug("%s got kicked." % str(partner))

    def submit_address(self, webfinger_address):
        return self.context.process_submission(webfinger_address)

    def terminate(self, erase=False):
        self.webserver.terminate()

        if self.synchronization_server:
            self.synchronization_server.terminate()
            self.synchronization_server = None

        self.context.close(erase=erase)

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

        sduds.run_synchronization_server("localhost", interface, synchronization_port)

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
        sduds.run_synchronization_server("localhost", interface, synchronization_port)

        # wait until program is interrupted
        while True: time.sleep(100)
