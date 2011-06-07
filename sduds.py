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

class AuthenticatingRequestHandler(SocketServer.BaseRequestHandler): # use paramiko with certificates instead?
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
    def handle_partner(self, partner_name):
        context = self.server.context
        partersocket = self.request

        context.synchronize_as_server(partnersocket, partner_name)

class Context:
    statedb = None
    partnerdb = None

    submission_queue = None
    validation_queue = None
    assimilation_queue = None

    synchronization_address = None

    logger = None

    def __init__(self, statedb=None, partnerdb=None, submission_queue_size=500, validation_queue_size=500, assimilation_queue_size=500, **kwargs):
        if statedb:
            self.statedb = statedb
        else:
            if "erase_statedb" in kwargs:
                erase = kwargs["erase_statedb"]
            else:
                erase = False

            self.statedb = states.StateDatabase(kwargs["hashtrie_path"], kwargs["entrydb_path"], erase=erase)

        if partnerdb:
            self.partnerdb = partnerdb
        else:
            if "erase_partnerdb" in kwargs:
                erase = kwargs["erase_partnerdb"]
            else:
                erase = False

            self.partnerdb = partners.PartnerDatabase(kwargs["partnerdb_path"], erase=erase)

        self.submission_queue = Queue.Queue(submission_queue_size)
        self.validation_queue = Queue.PriorityQueue(validation_queue_size)
        self.assimilation_queue = Queue.Queue(assimilation_queue_size)

        logger_name = "context"
        if "log" in kwargs:
            logger_name += ".%s" % kwargs["log"]
        self.logger = logging.getLogger(logger_name)

    def close(self, erase=False):
        self.submission_queue.join()
        self.validation_queue.join()
        self.assimilation_queue.join()

        self.statedb.close()
        self.partnerdb.close()

    def process_state(state, partner_name):
        if state.retrieval_timestamp:
            # if partner does take over responsibility, submit claim to validation queue
            claim = Claim(state, partner_name)

            try: self.validation_queue.put(claim)
            except Queue.Full:
                self.logger.warning("validation queue full while synchronizing with %s!" % partner_name)
        else:
            # if partner does not take over responsibility, simply submit the address for retrieval
            submission = Submission(state.address)
            try: self.submission_queue.put(submission)
            except Queue.Full:
                self.logger.warning("submission queue full while synchronizing with %s!" % partner_name)

    def synchronize_as_server(partnersocket, partner_name):
        # get the hashes the partner has but we haven't
        missing_hashes = self.statedb.hashtrie.synchronize_as_server(partnersocket)

        # Receive delete requests and construct preliminary invalid states from them. These states
        # may be omitted later if there is a new valid state for the same webfinger address.
        preliminary_invalid_states = {}
        while True:
            delete_request = DeleteRequest.receive(partnersocket)
            if not delete_request: break

            invalid_state = self.statedb.get_invalid_state(delete_request.hash, delete_request.retrieval_timestamp)
            preliminary_invalid_states[invalid_state.address] = invalid_state

        # filter out ghost states the partner doesn't know yet from missing_hashes and tell him
        deleted_hashes = set()

        for ghost in self.statedb.get_ghosts(missing_hashes)
            deleted_hashes += ghost.hash

            if not ghost.retrieval_timestamp==None:
                delete_request = DeleteRequest(ghost)
                delete_request.transmit(partnersocket)

        terminator.transmit(partnersocket)

        request_hashes = missing_hashes - deleted_hashes

        # receive requests for valid profiles
        requests = []
        while True:
            request = StateRequest.receive(partnersocket)
            if not request: break

            requests.append(request)

        # answer requests
        for request in requests:
            state = self.statedb.get_valid_state(request.hash)
            message = StateMessage(state)
            message.transmit(partnersocket)
        terminator.transmit(partnersocket)

        # request valid states
        for binhash in request_hashes:
            request = lib.Request(binhash)
            request.transmit(partnersocket)
        terminator.transmit(partnersocket)

        # receive valid states, removing the preliminarily constructed invalid
        # states for the respective webfinger addresses
        while True:
            message = StateMessage.receive(partnersocket)
            if not message: break

            state = message.state

            # remove preliminarily constructed invalid state
            # for this webfinger address
            if state.address in preliminary_invalid_states:
                del preliminary_invalid_states[state.address]

            self.process_state(state, partner_name)

        # the remaining invalid states are kept
        invalid_states = preliminary_invalid_states

        # put invalid states into queues
        for invalid_state in invalid_states.itervalues():
            self.process_state(invalid_state, partner_name)

    def synchronize_as_client(partnersocket, partner_name):
        # get the hashes the partner has but we haven't
        missing_hashes = self.statedb.hashtrie.synchronize_as_client(partnersocket)

        # filter out ghost states the partner doesn't know yet and tell him
        deleted_hashes = set()

        for ghost in self.statedb.get_ghosts(missing_hashes)
            deleted_hashes += ghost.hash

            if not ghost.retrieval_timestamp==None:
                delete_request = DeleteRequest(ghost)
                delete_request.transmit(partnersocket)

        terminator.transmit(partnersocket)

        request_hashes = missing_hashes - deleted_hashes

        # Receive delete requests and construct preliminary invalid states from them. These states
        # may be omitted later if there is a new valid state for the same webfinger address.
        preliminary_invalid_states = {}
        while True:
            delete_request = DeleteRequest.receive(partnersocket)
            if not delete_request: break

            invalid_state = self.statedb.get_invalid_state(delete_request.hash, delete_request.retrieval_timestamp)
            preliminary_invalid_states[invalid_state.address] = invalid_state

        # request valid states
        for binhash in request_hashes:
            request = StateRequest(binhash)
            request.transmit(partnersocket)
        terminator.transmit(partnersocket)

        # receive valid states, removing the preliminarily constructed invalid
        # states for the respective webfinger addresses
        while True:
            message = StateMessage.receive(partnersocket)
            if not message: break

            state = message.state

            # remove preliminarily constructed invalid state
            # for this webfinger address
            if state.address in preliminary_invalid_states:
                del preliminary_invalid_states[state.address]

            self.process_state(state, partner_name)

        # the remaining invalid states are kept
        invalid_states = preliminary_invalid_states

        # put invalid states into queues
        for invalid_state in invalid_states.itervalues():
            self.process_state(invalid_state, partner_name)

        # receive requests for valid profiles
        requests = []
        while True:
            request = StateRequest.receive(partnersocket)
            if not request: break

            requests.append(request)

        # answer requests
        for request in requests:
            state = self.statedb.get_valid_state(request.hash)
            message = StateMessage(state)
            message.transmit(partnersocket)

        terminator.transmit(partnersocket)

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
