#!/usr/bin/env python

import logging, time, Queue

from states import State, StateDatabase
from partners import PartnerDatabase
from synchronization import Synchronization

import states # for the exceptions

class Claim:
    """ partner_name==None means that the claim is not by another server but
        'self-made'. """

    timestamp = None
    state = None
    partner_name = None

    def __init__(self, state, partner_name=None, timestamp=None):
        if timestamp is None:
            self.timestamp = time.time()
        else:
            self.timestamp = timestamp

        self.state = state
        self.partner_name = partner_name

    def __cmp__(self, other):
        """ Provides ordering of claims in the validation queue by their
            priority. """

        # Note: __cmp__() should return whether self>other. PriorityQueue takes
        #       the lowest value first, so the return value is negated.

        # entries retrieved by ourselves have higher priority
        if self.partner_name is None: return not True
        if other.partner_name is None: return not False

        # earlier claims have higher priority
        return not self.timestamp<other.timestamp

    def validate(self, partnerdb, logger):
        if self.partner_name is None:
            trusted_state = self.state
        else:
            partner = partnerdb.get_partner(self.partner_name)
            logger = logger.getChild(partner_name)

            if partner.kicked: return None

            if not partner.control_sample():
                trusted_state = self.state
            else:
                retrieved_state = State.retrieve(self.state.address)

                reference_timestamp = int(time.time())

                if self.state==retrieved_state:
                    failed_address = None
                else:
                    failed_address = self.state.address

                    log_message = "Claimed state:\n"+\
                                  "==============\n"+\
                                  str(self.state)+"%s\n"+\
                                  "\n"+\
                                  "Retrieved state:\n"+\
                                  "================\n"+\
                                  str(retrieved_state)
                    logger.warning(log_message)

                    trusted_state = retrieved_state
                    partner_name = None

                partnerdb.register_control_sample(partner_name, reference_timestamp, failed_address)

        try:
            trusted_state.assert_validity(self.timestamp)
        except states.ValidationFailed, e:
            if partner_name:
                partnerdb.register_malformed_state(partner_name)
                logger.warning(str(e))
            return None
        except states.RecentlyExpiredStateException:
            # TODO: log
            return None
        else:
            return trusted_state

class Submission:
    webfinger_address = None

    def __init__(self, webfinger_address):
        self.webfinger_address = webfinger_address

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

            self.statedb = StateDatabase(kwargs["hashtrie_path"], kwargs["statedb_path"], erase=erase)

        if partnerdb:
            self.partnerdb = partnerdb
        else:
            self.partnerdb = PartnerDatabase(kwargs["partnerdb_path"])

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

        self.statedb.close(erase=erase)
        self.partnerdb.close()

    def submit_address(self, webfinger_address):
        try:
            submission = Submission(webfinger_address)
            self.submission_queue.put(submission)
            return True
        except Queue.Full:
            self.logger.warning("Submission queue full, rejected %s!" % webfinger_address)
            return False

    def process_state(self, state, partner_name, reference_timestamp):
        if state.retrieval_timestamp:
            # if partner does take over responsibility, submit claim to validation queue
            claim = Claim(state, partner_name, reference_timestamp)

            try:
                self.validation_queue.put(claim)
                self.logger.debug("State received from %s was put in validation queue" % partner_name)
            except Queue.Full:
                self.logger.warning("Validation queue full while synchronizing with %s!" % partner_name)
        else:
            # if partner does not take over responsibility, simply submit the address for retrieval
            submission = Submission(state.address)
            try:
                self.submission_queue.put(submission)
                self.logger.debug("Address by %s was put in submission queue." % partner_name)
            except Queue.Full:
                self.logger.warning("Submission queue full while synchronizing with %s!" % partner_name)

    def synchronize_as_server(self, partnersocket, partner_name):
        missing_hashes = self.statedb.hashtrie.get_missing_hashes_as_server(partnersocket)

        synchronization = Synchronization(missing_hashes)

        f = partnersocket.makefile()

        self.logger.debug("receive deletion requests")
        synchronization.receive_deletion_requests(f, self.statedb)
        self.logger.debug("send deletion requests")
        synchronization.send_deletion_requests(f, self.statedb)

        self.logger.debug("receive state requests")
        synchronization.receive_state_requests(f)
        self.logger.debug("send states")
        synchronization.send_states(f, self.statedb)

        self.logger.debug("send state requests")
        reference_timestamp = time.time()
        synchronization.send_state_requests(f)
        self.logger.debug("receive states")
        for state in synchronization.receive_states(f):
            self.process_state(state, partner_name, reference_timestamp)

        f.close()

    def synchronize_as_client(self, partnersocket, partner_name):
        missing_hashes = self.statedb.hashtrie.get_missing_hashes_as_client(partnersocket)

        synchronization = Synchronization(missing_hashes)

        f = partnersocket.makefile()

        self.logger.debug("send deletion requests")
        synchronization.send_deletion_requests(f, self.statedb)
        self.logger.debug("receive deletion requests")
        synchronization.receive_deletion_requests(f, self.statedb)

        self.logger.debug("send state requests")
        reference_timestamp = time.time()
        synchronization.send_state_requests(f)
        self.logger.debug("receive states")
        for state in synchronization.receive_states(f):
            self.process_state(state, partner_name, reference_timestamp)

        self.logger.debug("receive state requests")
        synchronization.receive_state_requests(f)
        self.logger.debug("send states")
        synchronization.send_states(f, self.statedb)

        f.close()

    def submission_worker(self):
        while True:
            submission = self.submission_queue.get()
            if submission is None:
                self.submission_queue.task_done()
                self.logger.debug("Reached end of submission queue.")
                return

            self.logger.debug("Got address %s from submission queue." % submission.webfinger_address)

            try:
                state = State.retrieve(submission.webfinger_address)
            except Exception, e:
                self.logger.warning("Retrieval of address %s failed: %s" % (submission.webfinger_address, str(e)))
                self.submission_queue.task_done()
                continue

            self.logger.debug("Address %s successfully retrieved." % submission.webfinger_address)

            claim = Claim(state)

            self.validation_queue.put(claim, True)
            self.submission_queue.task_done()

            self.logger.debug("Claim for %s submitted to validation queue." % submission.webfinger_address)

    def validation_worker(self):
        while True:
            claim = self.validation_queue.get()
            if claim is None:
                self.validation_queue.task_done()
                self.logger.debug("Reached end of validation queue.")
                return

            self.logger.debug("Got claim(%s, %s) from validation queue." % (claim.state.address, claim.partner_name))

            validated_state = claim.validate(self.partnerdb, self.logger)
            if validated_state:
                self.assimilation_queue.put(validated_state, True)
                self.logger.debug("Validated claim(%s, %s) and submitted state to assimilation queue." % (claim.state.address, claim.partner_name))
            else:
                self.logger.warning("Validation of claim(%s, %s) failed." % (claim.state.address, claim.partner_name))

            self.validation_queue.task_done()

    def assimilation_worker(self):
        while True:
            state = self.assimilation_queue.get()
            if state is None:
                self.assimilation_queue.task_done()
                self.logger.debug("Reached end of assimilation queue.")
                return

            address = state.address # only for logging [ORM expires state object during save()]
            self.statedb.save(state)
            self.assimilation_queue.task_done()
            self.logger.debug("Saved state of %s to database." % address)
