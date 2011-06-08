#!/usr/bin/env python

class Synchronization:
    missing_hashes = None
    preliminary_invalid_states = None
    request_hashes = None
    requests = None

    def __init__(self, missing_hashes)
        self.missing_hashes = missing_hashes

    def send_deletion_requests(self, socket, statedb):
        """ filter out ghost states the partner doesn't know yet and tell him """

        deleted_hashes = set()

        for ghost in statedb.get_ghosts(self.missing_hashes)
            deleted_hashes += ghost.hash

            if not ghost.retrieval_timestamp==None:
                deletion_request = DeletionRequest(ghost)
                deletion_request.transmit(partnersocket)

        terminator.transmit(partnersocket)

        self.request_hashes = self.missing_hashes - deleted_hashes
        self.missing_hashes = None

    def receive_deletion_requests(self, socket, statedb):
        """ Receive delete requests and construct preliminary invalid states from them. These states
            may be omitted later if there is a new valid state for the same webfinger address. """

        self.preliminary_invalid_states = {}

        while True:
            deletion_request = DeletionRequest.receive(partnersocket)
            if not deletion_request: break

            invalid_state = statedb.get_invalid_state(delete_request.hash, delete_request.retrieval_timestamp)
            self.preliminary_invalid_states[invalid_state.address] = invalid_state

    def send_state_requests(self, socket):
        """ request valid states """

        for binhash in self.request_hashes:
            request = StateRequest(binhash)
            request.transmit(partnersocket)

        terminator.transmit(partnersocket)

        self.request_hashes = None

    def receive_states(self, socket):
        """ Receive valid states, removing the preliminarily constructed invalid
            states for the respective webfinger addresses. Will yield the received
            states and the remaining invalid states. """

        while True:
            message = StateMessage.receive(partnersocket)
            if not message: break

            state = message.state

            # remove preliminarily constructed invalid state
            # for this webfinger address
            if state.address in self.preliminary_invalid_states:
                del self.preliminary_invalid_states[state.address]

            yield state

        # the remaining invalid states are kept
        invalid_states = self.preliminary_invalid_states
        self.preliminary_invalid_states = None
        
        # yield remaining invalid states
        for invalid_state in invalid_states.itervalues():
            yield invalid_state

    def receive_state_requests(self, socket):
        """ receive requests for valid states """

        self.requests = []

        while True:
            request = StateRequest.receive(partnersocket)
            if not request: break

            self.requests.append(request)

    def send_states(self, socket, statedb):
        """ answer state requests """

        for request in self.requests:
            state = statedb.get_valid_state(request.hash)
            message = StateMessage(state)
            message.transmit(partnersocket)

        terminator.transmit(partnersocket)

