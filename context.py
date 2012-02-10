#!/usr/bin/env python

import logging, time, Queue

from states import State, StateDatabase
from partners import PartnerDatabase
from synchronization import Synchronization

class Claim:
    """ partner_name==None means that the claim is not by another server but
        'self-made'. """

    timestamp = None
    state = None
    partner_name = None

    def __init__(self, state, partner_name=None, timestamp=None):
        if timestamp==None:
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
        if self.partner_name==None: return not True
        if other.partner_name==None: return not False

        # earlier claims have higher priority
        return not self.timestamp<other.timestamp

    def validate(self, partnerdb):
        if self.partner_name:
            partner = partnerdb.get_partner(self.partner_name)

            if partner.kicked: return None

            if partner.control_sample():
                retrieved_state = State.retrieve(self.state.address)

                offense = None
                if not self.state==retrieved_state:
                    offense = "Claimed state:\n"+\
                              "================\n"+\
                              str(self.state)+"\n"+\
                              "\n"+\
                              "Retrieved state:\n"+\
                              "================\n"+\
                              str(retrieved_state)

                    trusted_state = retrieved_state
                    partner_name = None

                partnerdb.register_control_sample(partner_name, self.state.address, offense)

            else:
                trusted_state = self.state

        try:
            trusted_state.assert_validity(self.timestamp)
        except Violation, violation:
            if partner_name:
                partnerdb.register_violation(partner_name, violation)
            return None
        except RecentlyExpiredException, e:
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
            if "erase_partnerdb" in kwargs:
                erase = kwargs["erase_partnerdb"]
            else:
                erase = False

            self.partnerdb = PartnerDatabase(kwargs["partnerdb_path"], erase=erase)

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
        self.partnerdb.close(erase=erase)

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

            try: self.validation_queue.put(claim)
            except Queue.Full:
                self.logger.warning("validation queue full while synchronizing with %s!" % partner_name)
        else:
            # if partner does not take over responsibility, simply submit the address for retrieval
            submission = Submission(state.address)
            try: self.submission_queue.put(submission)
            except Queue.Full:
                self.logger.warning("submission queue full while synchronizing with %s!" % partner_name)

    def synchronize_as_server(self, partnersocket, partner_name):
        missing_hashes = self.statedb.hashtrie.get_missing_hashes_as_server(partnersocket)

        synchronization = Synchronization(missing_hashes)

        f = partnersocket.makefile()

        synchronization.receive_deletion_requests(f, self.statedb)
        synchronization.send_deletion_requests(f, self.statedb)

        synchronization.receive_state_requests(f)
        synchronization.send_states(f, self.statedb)

        reference_timestamp = time.time()
        synchronization.send_state_requests(f)
        for state in synchronization.receive_states(f):
            self.process_state(state, partner_name, reference_timestamp)

        f.close()

    def synchronize_as_client(self, partnersocket, partner_name):
        missing_hashes = self.statedb.hashtrie.get_missing_hashes_as_client(partnersocket)

        synchronization = Synchronization(missing_hashes)

        f = partnersocket.makefile()

        synchronization.send_deletion_requests(f, self.statedb)
        synchronization.receive_deletion_requests(f, self.statedb)

        reference_timestamp = time.time()
        synchronization.send_state_requests(f)
        for state in synchronization.receive_states(f):
            self.process_state(state, partner_name, reference_timestamp)

        synchronization.receive_state_requests(f)
        synchronization.send_states(f, self.context.statedb)

        f.close()
