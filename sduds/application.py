#!/usr/bin/env python

import threading
import socket

from constants import *

from lib import scheduler, authentication
from lib.threadingserver import ThreadingServer

from webserver import WebServer
from context import Context

class SynchronizationRequestHandler(authentication.AuthenticatingRequestHandler):
    """ Authenticates partners and calls synchronize_as_server if successful. """

    def get_password(self, partner_name):
        context = self.server.context
        partner = context.partnerdb.get_partner(partner_name)

        if partner is None:
            context.logger.warning("Reject authentication: %s unknown." % partner_name)
            return None
        elif partner.kicked:
            context.logger.warning("Reject authentication: %s is kicked." % partner_name)
            return None
        else:
            return partner.accept_password

    def handle_user(self, partner_name):
        context = self.server.context
        partnersocket = self.request

        context.synchronize_as_server(partnersocket, partner_name)

class SynchronizationServer(threading.Thread):
    """ Waits for partners to synchronize. """

    server = None
    public_address = None

    def __init__(self, context, fqdn, interface, port):
        threading.Thread.__init__(self)

        # initialize server
        address = (interface, port)
        self.server = ThreadingServer(address, SynchronizationRequestHandler)

        # expose context so that the RequestHandler can access it
        self.server.context = context

        # save public address to be able to publish it when server starts
        self.public_address = (fqdn, port)

    def run(self):
        # publish address so that partners can synchronize with us
        context = self.server.context
        context.synchronization_address = self.public_address

        # start server
        self.server.serve_forever()

    def terminate(self):
        # depublish synchronization address
        context = self.server.context
        context.synchronization_address = None

        # terminate server
        self.server.terminate()

        # wait until server has terminated
        self.join()

class Application:
    context = None

    ready_for_synchronization = None

    web_server = None
    synchronization_server = None

    synchronization_jobs = None
    statedb_cleanup_job = None
    partnerdb_cleanup_job = None

    submission_workers = None
    validation_workers = None
    assimilation_worker = None

    def __init__(self, context):
        self.context = context

        # need this for waiting until expired states are deleted
        self.ready_for_synchronization = threading.Event()

        # set default values
        self.synchronization_jobs = []
        self.submission_workers = []
        self.validation_workers = []

    def configure_web_server(self, interface="", port=20000):
        self.web_server = WebServer(self.context, interface, port)

    def configure_synchronization_server(self, fqdn, interface="", port=20001):
        self.synchronization_server = SynchronizationServer(self.context, fqdn, interface, port)

    def configure_workers(self, submission_workers=5, validation_workers=5):
        # submission workers
        for i in xrange(submission_workers):
            worker = threading.Thread(target=self.context.submission_worker)
            self.submission_workers.append(worker)

        # validation workers
        for i in xrange(validation_workers):
            worker = threading.Thread(target=self.context.validation_worker)
            self.validation_workers.append(worker)

        # assimilation worker
        self.assimilation_worker = threading.Thread(target=self.context.assimilation_worker)

    def configure_jobs(self, synchronization=True, statedb_cleanup=True, partnerdb_cleanup=True):
        # go through servers, add jobs
        if synchronization:
            for partner in self.context.partnerdb.get_partners():
                if not partner.connection_schedule: continue

                minute,hour,dom,month,dow = partner.connection_schedule.split()
                pattern = scheduler.CronPattern(minute,hour,dom,month,dow)
                job = scheduler.Job(pattern, self.synchronize_with_partner, (partner.name,), partner.last_connection)
                self.synchronization_jobs.append(job)

        # add state database cleanup job
        if statedb_cleanup:
            last_cleanup = self.context.statedb.cleanup_timestamp

            pattern = scheduler.IntervalPattern(STATEDB_CLEANUP_INTERVAL)

            def callback(self):
                timestamp = self.context.statedb.cleanup()
                self.ready_for_synchronization.set()
                return timestamp

            self.statedb_cleanup_job = scheduler.Job(pattern, callback, (self,), last_cleanup)

        # add partner database cleanup job
        if partnerdb_cleanup:
            pattern = scheduler.IntervalPattern(PARTNERDB_CLEANUP_INTERVAL)
            self.partnerdb_cleanup_job = scheduler.Job(pattern, self.context.partnerdb.cleanup)

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

        self.synchronization_server.start()

    def terminate_synchronization_server(self):
        if not self.synchronization_server: return

        self.synchronization_server.terminate()
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
        for job in self.synchronization_jobs:
            job.start()

        job = self.statedb_cleanup_job
        if job:
            if not job.overdue(): self.ready_for_synchronization.set()
            job.start()

        job = self.partnerdb_cleanup_job
        if job: job.start()

    def terminate_jobs(self):
        for job in self.synchronization_jobs:
            job.terminate()

        self.synchronization_jobs = []

        if self.statedb_cleanup_job:
            self.statedb_cleanup_job.terminate()
        self.statedb_cleanup_job = None

        if self.partnerdb_cleanup_job:
            self.partnerdb_cleanup_job.terminate()
        self.partnerdb_cleanup_job = None

    def start(self, web_server=True, synchronization_server=True, jobs=True, workers=True):
        if web_server:
            self.context.logger.info("Starting web server...")
            self.start_web_server()
            self.context.logger.info("Web server started.")

        if jobs:
            self.context.logger.info("Starting jobs...")
            self.start_jobs()
            self.context.logger.info("Jobs started.")

        if workers:
            self.context.logger.info("Starting workers...")
            self.start_workers()
            self.context.logger.info("Workers started.")

        if synchronization_server:
            # do not synchronize as long as we might have expired states
            self.context.logger.info("Wait until statedb is clean...")
            self.ready_for_synchronization.wait()
            self.context.logger.info("statedb is clean. Starting synchronization server...")
            self.start_synchronization_server()
            self.context.logger.info("Synchronization server started.")

    def terminate(self, erase=False):
        self.context.logger.info("Terminating web server...")
        self.terminate_web_server()
        self.context.logger.info("Terminating synchronization server...")
        self.terminate_synchronization_server()
        self.context.logger.info("Terminating jobs...")
        self.terminate_jobs()
        self.context.logger.info("Terminating workers...")
        self.terminate_workers()
        self.context.logger.info("Closing context...")
        self.context.close(erase)
        self.context.logger.info("Context terminated.")

    def synchronize_with_partner(self, partner_name):
        # do not synchronize as long as we might have expired states
        self.context.logger.info("Wait until state database is clean before synchronizing with %s" % partner_name)
        self.ready_for_synchronization.wait()
        self.context.logger.info("Synchronizing with %s." % partner_name)

        # get partner from name
        partner = self.context.partnerdb.get_partner(partner_name)

        # register synchronization attempt
        timestamp = self.context.partnerdb.register_connection(partner_name)
        
        # no need to synchronize if partner is kicked: states will be rejected anyhow
        if partner.kicked:
            self.context.logger.warning("Will not synchronize with kicked partner %s" % partner_name)
            return timestamp

        # get the synchronization address
        try:
            host, synchronization_port = partner.get_synchronization_address()
            address = (host, synchronization_port)
        except Exception, e:
            self.context.logger.warning("Unable to get synchronization address of %s: %s" % (partner_name, str(e)))
            return timestamp

        # establish connection
        try:
            partnersocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            partnersocket.connect(address)
        except Exception, e:
            self.context.logger.warning("Unable to connect to partner %s for synchronization: %s" % (partner_name, str(e)))
            return timestamp

        # authentication
        try:
            success = authentication.authenticate_socket(partnersocket, partner.provide_username, partner.provide_password)
        except Exception, e:
            self.context.logger.warning("Unable to authenticate to partner %s for synchronization: %s" % (partner_name, str(e)))
            return timestamp

        if not success:
            self.context.logger.warning("Invalid credentials for partner %s!" % partner_name)
            return timestamp

        # conduct synchronization
        try:
            self.context.synchronize_as_client(partnersocket, partner_name)
        except Exception, e:
            self.context.logger.warning("Unable to synchronize with partner %s: %s" % (partner_name, str(e)))

        # return synchronization time
        return timestamp
