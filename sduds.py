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

CLEANUP_INTERVAL = 3600*24
EXPIRY_GRACE_PERIOD = 3600*24*3

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

#class SynchronizationServer(lib.BaseServer):
#    """ The server partners can connect to for synchronizing. The hashes that must be added
#        and the hashes that must be deleted are computed and context.process_hashes_from_partner
#        is called. The counterpart for this is SDUDS.synchronize_with_partner. """
#
#    def __init__(self, address, context):
#        lib.BaseServer.__init__(self, address, SynchronizationRequestHandler)
#        self.context = context

class SynchronizationRequestHandler(AuthenticatingRequestHandler): # old version
    def handle_partner(self, partner):
        context = self.server.context

        context.logger.info("%s connected from %s to %s" % (str(partner), str(self.client_address), str(self.server)))

        ### get the set of hashes the partner has but we haven't (conduct actual synchronization)
        context.logger.debug("conducting synchronization with %s" % str(partner))
        add_hashes = context.hashtrie.synchronize_as_client(self.request)
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

        ### call process_hashes_from_partner
        context.process_hashes_from_partner(partner, add_hashes, delete_hashes)

class SynchronizationServer(lib.BaseServer):
    def __init__(self, context, fqdn, interface, port):
        # initialize server
        address = (interface, port)
        lib.BaseServer.__init__(self, address, SynchronizationRequestHandler)

        # expose context so that the RequestHandler can access it
        self.context = context

        # expose public address
        self.public_address = (fqdn, port)

class SynchronizationRequestHandler(AuthenticatingRequestHandler):
    def handle_partner(self, partner):
        self.server.context.synchronize_as_server(self.request)
        # TODO: This function will get the entries from the partner.
        # So it needs to know which partner it talks to for the WrongEntriesViolation.

###

class Context:
    """ A context is a collection of all necessary databases. It does not contain any trie synchronization
        code, that's the job of the SDUDS class. """

    def __init__(self, suffix="", erase=False):
        self.partnerdb = partners.Database(suffix, erase=erase)
        self.entrydb = entries.Database(suffix, erase=erase)
        self.hashtrie = HashTrie(suffix, erase=erase)
        self.queue = lib.TwoPriorityQueue(500)
        self.clean = threading.Event()  # signalizes if cleanup was called recently enough

        self.logger = logging.getLogger("sduds"+suffix)

    def close(self, erase=False):
        self.partnerdb.close(erase=erase)
        self.entrydb.close(erase=erase)
        self.hashtrie.close(erase=erase)

    def process(self):
        while True:
            webfinger_address, claim = self.queue.get()
            if not webfinger_address: break

            self.logger.debug("processing %s from queue" % webfinger_address)

            if not claim:
                self.logger.debug("%s is a simple submission" % webfinger_address)
                retrieve_profile = True
            else:
                claimed_state, retrieval_timestamp, claiming_partner_name = claim
                claiming_partner = partners.Partner.from_database(self.partnerdb, partner_name=claiming_partner_name)

                # reject kicked partners
                if claiming_partner.kicked():
                    self.logger.warning("%s is kicked, so don't process %s." % (claiming_partner, webfinger_address))
                    self.queue.task_done()
                    continue

                # check whether the profile should be retrieved
                if retrieval_timestamp < time.time()-RESPONSIBILITY_TIME_SPAN:
                    # TODO: notify administrator
                    self.logger.warning("%s, gotten from %s was retrieved a too long time ago" % (webfinger_address, claiming_partner))
                    retrieve_profile = True
                    responsibility = False
                elif random.random() < claiming_partner.control_probability:
                    self.logger.debug("decided to take a control sample for %s, gotten from %s" % (webfinger_address, claiming_partner))
                    retrieve_profile = True
                    responsibility = True
                    claiming_partner.register_control_sample()
                else:
                    self.logger.debug("decided not to take a control sample for %s, gotten from %s" % (webfinger_address, claiming_partner))
                    retrieve_profile = False

            if retrieve_profile:
                self.logger.debug("retrieving the profile for %s" % webfinger_address)

                try:
                    state = entries.Entry.from_webfinger_address(webfinger_address)
                    retrieval_timestamp = state.retrieval_timestamp
                except Exception, error:
                    state = None
                    retrieval_timestamp = int(time.time())

                # check whether claim was right
                if not claim: pass
                elif state==None and claimed_state==None: pass
                elif state and claimed_state and state.hash==claimed_state.hash: pass
                else:
                    self.logger.warning("state of %s is not as %s claimed" % (webfinger_address, claiming_partner))
                    offense = partners.NonConcurrenceOffense(state, claimed_state, guilty=responsibility)
                    claiming_partner.add_offense(offense)
            else:
                state = claimed_state

                # verify that entry was retrieved after it was submitted
                if claim and state and state.retrieval_timestamp<state.submission_timestamp:
                    self.logger.warning("%s transmitted state with invalid timestamps" % claiming_partner)
                    violation = partners.InvalidTimestampsViolation(state)
                    claiming_partner.add_violation(violation)
                    self.queue.task_done()
                    continue

            # check if entry is expired
            if state and state.expired():
                self.logger.warning("got expired entry for %s" % state.webfinger_address)

                if not claim: pass # partner not responsible, address was submitted directly
                elif retrieve_profile: pass # partner not responsible, retrieved profile ourselves
                elif time.time()-state.submission_timestamp < entries.ENTRY_LIFETIME+EXPIRY_GRACE_PERIOD: pass # grace period
                else:
                    violation = partners.ExpiredEntryViolation(claimed_state)
                    claiming_partner.add_violation(violation)

                self.queue.task_done()
                continue

            # check captcha signature
            if state and not state.captcha_signature_valid():
                if claim and not retrieve_profile:
                    self.logger.warning("invalid captcha for %s from %s" % (webfinger_address, claiming_partner))
                    violation = partners.InvalidCaptchaViolation(state)
                    claiming_partner.add_violation(violation)
                else:
                    self.logger.warning("invalid captcha for %s" % webfinger_address)

                self.queue.task_done()
                continue

            added_hashes,deleted_hashes,ignored_hashes = self.entrydb.save_state(webfinger_address, state, retrieval_timestamp)
            self.hashtrie.add(added_hashes)
            self.hashtrie.delete(deleted_hashes)
            self.logger.info("added %d, deleted %d hashes" % (len(added_hashes), len(deleted_hashes)))

            self.queue.task_done()

    def process_hashes_from_partner(self, partner, add_hashes, delete_hashes):
        self.logger.debug("process_hashes_from_partner: add %d/delete %d from %s" % (len(add_hashes), len(delete_hashes), partner))

        claimed_states = {}

        # process delete_hashes first as new entries overwrite old entries
        for binhash,retrieval_timestamp in delete_hashes.iteritems():
            entry = entries.Entry.from_database(self.entrydb, hash=binhash)
            address = entry.webfinger_address

            claimed_states[address] = (None, retrieval_timestamp)

        # process add_hashes
        try:
            self.logger.info("Requesting %d hashes from %s" % (len(add_hashes), partner))
            entrylist = entries.EntryList.from_server(add_hashes, partner.address)
        except IOError, error:
            self.logger.warning("IOError with %s" % partner)
            offense = partners.ConnectionFailedOffense(error)
            partner.add_offense(offense)
            return
        except entries.InvalidHashError, error:
            self.logger.warning("invalid hash from %s" % partner)
            violation = partners.InvalidHashViolation(error)
            partner.add_violation(violation)
            return
        except entries.InvalidListError, error:
            self.logger.warning("invalid list from %s" % partner)
            violation = partners.InvalidListViolation(error)
            partner.add_violation(violation)
            return
        except entries.WrongEntriesError, error:
            self.logger.warning("wrong entries from %s" % partner)
            violation = partners.WrongEntriesViolation(error)
            partner.add_violation(violation)
            return

        for entry in entrylist:
            claimed_states[entry.webfinger_address] = (entry, entry.retrieval_timestamp)

        # create jobs
        for address, claimed_state in claimed_states.iteritems():
            self.queue.put_high((address, claimed_state + (partner.partner_name,)))

    def process_submission(self, webfinger_address):
        try:
            self.queue.put_low((webfinger_address, None))
            return True
        except lib.Full:
            self.logger.error("Submission queue full, rejected %s!" % webfinger_address)
            return False

    def cleanup(self):
        now, delete_hashes = self.entrydb.cleanup()
        self.hashtrie.delete(delete_hashes)

        self.entrydb.set_variable("last_cleanup", str(now))
        self.clean.set()

        self.logger.debug("cleanup executed, deleted %d entries." % len(delete_hashes))

        return now

class SDUDS:
#    def __init__(self, webserver_address, suffix="", erase=False):
#        self.context = Context(suffix, erase=erase)
#
#        interface, port = webserver_address
#        self.webserver = WebServer(self.context, interface, port)
#        self.webserver.start()
#
#        self.synchronization_server = None
#
#        self.worker = threading.Thread(target=self.context.process)
#        self.worker.start()
#
#        self.jobs = []
#
#        # go through servers, add jobs
#        servers = partners.Server.list_from_database(self.context.partnerdb)
#
#        for server in servers:
#            minute,hour,dom,month,dow = server.connection_schedule.split()
#            pattern = lib.CronPattern(minute,hour,dom,month,dow)
#            job = lib.Job(pattern, self.try_synchronization_with_server, (server.partner_name,), server.last_connection)
#            job.start()
#
#            self.jobs.append(job)
#
#        # add cleanup job
#        cleanup_schedule = self.context.entrydb.get_variable("cleanup_schedule")
#        minute,hour,dom,month,dow = cleanup_schedule.split()
#
#        last_cleanup = self.context.entrydb.get_variable("last_cleanup")
#        if not last_cleanup==None: last_cleanup = float(last_cleanup)
#
#        pattern = lib.CronPattern(minute,hour,dom,month,dow)
#        job = lib.Job(pattern, self.context.cleanup, (), last_cleanup)
#        if not job.overdue(): self.context.clean.set()
#        job.start()
#
#        self.jobs.append(job)
#
#    def run_synchronization_server(self, domain, interface="", synchronization_port=20001):
#        self.context.clean.wait()
#
#        # set up server
#        self.synchronization_server = SynchronizationServer((interface, synchronization_port), self.context)
#
#        # publish address so that partners can synchronize with us
#        self.webserver.set_synchronization_address(domain, synchronization_port)
#
#        # set up the server thread
#        self.synchronization_thread = threading.Thread(target=self.synchronization_server.serve_forever)
#
#        # run the server
#        self.synchronization_thread.start()

#    def try_synchronization_with_server(self, server_name):
#        # load partner
#        server = partners.Server.from_database(self.context.partnerdb, partner_name=server_name)
#
#        try:
#            self.synchronize_with_partner(server)
#        except Exception,error:
#            self.context.logger.warning("Error synchronizing with %s" % server)
#            offense = partners.ConnectionFailedOffense(error)
#            server.add_offense(offense)
#
#        # save synchronization attempt, returning timestamp
#        return server.register_connection()

    def synchronize_with_partner(self, partner):
        """ the client side for SynchronizationServer """

        self.context.clean.wait()

#        # get the synchronization address
#        host, synchronization_port = partner.synchronization_address()
#        address = (host, synchronization_port)
#
#        # connect
#        self.context.logger.info("Connecting to %s for synchronization with %s" % (str(address), str(partner)))
#        partnersocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#        partnersocket.connect(address)
#
#        # authenticate
#        self.context.logger.debug("Authenticating synchronization socket %s to partner %s" % (str(address), str(partner)))
#        success = authenticate_socket_to_partner(partnersocket, partner)
#        assert success

        # get the set of hashes the partner has but we haven't
        self.context.logger.debug("Getting hashes from %s" % str(partner))
        add_hashes = self.context.hashtrie.synchronize_as_server(partnersocket)
        self.context.logger.info("Got %d hashes from %s" % (len(add_hashes), str(partner)))

        # make file of socket
        partnerfile = partnersocket.makefile()

        # get the hashes that we should delete
        delete_hashes = {}

        while True:
            try:
                hexhash, retrieval_timestamp = partnerfile.readline().split()
                self.context.logger.debug("%s asks us to delete hash %s" % (str(partner), hexhash))
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

        ### call process_hashes_from_partner
        self.context.process_hashes_from_partner(partner, add_hashes, delete_hashes)

#    def submit_address(self, webfinger_address):
#        return self.context.process_submission(webfinger_address)
#
#    def terminate(self, erase=False):
#        for job in self.jobs: job.terminate()
#
#        self.webserver.terminate()
#
#        self.context.queue.put_high((None, None))
#
#        if self.synchronization_server:
#            self.synchronization_server.terminate()
#            self.synchronization_server = None
#
#        self.worker.join()
#        self.context.close(erase=erase)

class Application:
    def __init__(self, context):
        self.context = context

        # need this for waiting until expired states are deleted
        self.ready_for_synchronization = threading.Event()

        # set default values
        self.web_server = None
        self.synchronization_server = None
        self.jobs = []
        self.submission_workers = []
        self.validation_workers = []
        self.assimilation_worker = None

    def configure_web_server(self, interface="", port=20000):
        self.web_server = WebServer(self.context, interface, port, self.published_synchronization_address)

    def configure_synchronization_server(self, fqdn, interface="", port=20001):
        self.synchronization_server = SynchronizationServer(self.context, fqdn, interface, port)

    def configure_workers(self, submission_workers=5, validation_workers=5):
        # submission workers
        for i in xrange(submission_workers):
            worker = threading.Thread(target=self.submission_worker)
            self.submission_workers.append(worker)

        # validation workers
        for i in xrange(validation_workers):
            worker = threading.Thread(target=self.validation_worker)
            self.submission_workers.append(worker)

        # assimilation worker
        self.assimilation_worker = threading.Thread(target=self.assimilation_worker)

    def start_web_server(self, *args, **kwargs):
        # use arguments to configure web server
        if args or kwargs:
            self.configure_web_server(*args, **kwargs)

        # start web server
        self.web_server.start()

    def terminate_web_server(self):
        if not self.web_server: return

        self.web_server.terminate()
        self.web_server = None

    def start_synchronization_server(self, *args, **kwargs):
        # use arguments to configure synchronization server
        if args or kwargs:
            self.configure_synchronization_server(*args, **kwargs)

        # publish address so that partners can synchronize with us
        self.context.synchronization_address = self.synchronization_server.public_address

        # set up the server thread
        self.synchronization_thread = threading.Thread(target=self.synchronization_server.serve_forever)

        # run the server
        self.synchronization_thread.start()

    def terminate_synchronization_server(self):
        if not self.synchronization_server: return

        self.context.synchronization_address = None

        self.synchronization_server.terminate()
        self.synchronization_thread.join()
        self.synchronization_server = None

    def start_workers(self, *args, **kwargs):
        # use arguments to configure synchronization server
        if args or kwargs:
            self.configure_workers(*args, **kwargs)

        # start submission workers
        for worker in self.submission_workers:
            worker.start()
        
        # start validation workers
        for worker in self.validation_workers:
            worker.start()

        # start assimilation worker
        self.assimilation_worker.start()

    def terminate_workers(self):
        # terminate submission workers
        for worker in self.submission_workers:
            self.context.submission_queue.put(None)

        for worker in self.submission_workers:
            worker.join()

        self.submission_workers = []

        # terminate validation workers
        for worker in self.validation_workers:
            self.context.validation_queue.put(None)

        for worker in self.validation_workers:
            worker.join() 

        self.validation_workers = []

        # terminate assimilation worker
        if self.assimilation_worker:
            self.context.assimilation_queue.put(None)
            self.assimilation_worker.join()
            self.assimilation_worker = None

    def start_jobs(self):
        # go through servers, add jobs
        for server in self.context.partnerdb.get_servers():
            minute,hour,dom,month,dow = server.connection_schedule.split()
            pattern = lib.CronPattern(minute,hour,dom,month,dow)
            job = lib.Job(pattern, self.synchronize_with_partner, (server.partner_name,), server.last_connection)
            job.start()

            self.jobs.append(job)

        # add cleanup job
        last_cleanup = self.context.statedb.get_variable("last_cleanup")
        if not last_cleanup==None: last_cleanup = float(last_cleanup)

        pattern = lib.IntervalPattern(CLEANUP_INTERVAL)
        job = lib.Job(pattern, self.context.statedb.cleanup, (), last_cleanup)

        if not job.overdue(): self.ready_for_synchronization.set()
        job.start()

        self.jobs.append(job)

    def terminate_jobs(self):
        for job in self.jobs:
            job.terminate()

        self.jobs = []

    def start(self, web_server=True, synchronization_server=True, jobs=True, workers=True):
        if web_server:
            self.start_web_server()

        if jobs:
            self.start_jobs()

        if workers:
            sef.start_workers()

        if synchronization_server:
            # do not synchronize as long as we might have expired states
            self.ready_for_synchronization.wait()

            self.start_synchronization_server()

    def terminate(self, erase=False):
        self.termiante_web_server()
        self.terminate_synchronization_server()
        self.terminate_jobs()
        self.terminate_workers()

        self.context.close(erase)

    def submission_worker(self):
        while True:
            submission = self.context.submission_queue.get()
            if submission==None:
                self.context.submission_queue.task_done()
                return

            partner = partners.Me
            state = State.retrieve(submission.webfinger_address)

            claim = Claim(partner, state)

            self.context.validation_queue.put(claim, True)
            self.context.submission_queue.task_done()
        
    def validation_worker(self):
        while True:
            claim = self.context.validation_queue.get()
            if claim==None:
                self.context.validation_queue.task_done()
                return

            validated_claim = claim.validate()
            self.context.assimilation_queue.put(validated_claim, True)
            self.context.validation_queue.task_done()

    def assimilation_worker(self):
        while True:
            validated_claim = self.context.assimilation_queue.get()
            if validated_claim==None:
                self.context.assimilation_queue.task_done()
                return

            address = validated_claim.address
            state = validated_claim.state

            self.context.statedb.save(address, state)
            self.context.assimilation_queue.task_done()

    def submit_address(self, webfinger_address):
        try:
            submission = Submission(webfinger_address)
            self.context.submission_queue.put(submission)
            return True
        except Queue.Full:
            self.context.logger.warning("Submission queue full, rejected %s!" % webfinger_address)
            return False

    def synchronize_with_partner(self, partner):
        # do not synchronize as long as we might have expired states
        self.ready_for_synchronization.wait()

        # register synchronization attempt
        timestamp = self.context.partnerdb.register_connection(partner)
        
        # get the synchronization address
        try:
            host, synchronization_port = partner.get_synchronization_address()
            address = (host, synchronization_port)
        except Exception, e:
            self.context.logger.warning("Unable to get synchronization address of %s: %s" % (str(partner), str(e)))
            return timestamp
        
        # establish connection
        try:
            partnersocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            partnersocket.connect(address)
        except Exception, e:
            self.context.logger.warning("Unable to connect to partner %s for synchronization: %s" % (str(partner), str(e)))
            return timestamp

        # authentication
        try:
            success = authenticate_socket_to_partner(partnersocket, partner)
        except Exception, e:
            self.context.logger.warning("Unable to authenticate to partner %s for synchronization: %s" % (str(partner), str(e)))
            return timestamp

        if not success:
            self.context.logger.warning("Invalid credentials for partner %s!" % str(partner))
            return timestamp

        # conduct synchronization
        try:
            self.context.synchronize_as_client(partnersocket)
        except Exception, e:
            self.context.logger.warning("Unable to synchronize with partner %s: %s" % (str(partner), str(e)))

        # return synchronization time
        return timestamp

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
