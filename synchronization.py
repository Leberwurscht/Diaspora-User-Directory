#!/usr/bin/env python

import struct, time

""" functions to send and receive some basic types over the network,
    using the file descriptor obtained from socket.makefile() """

# for writing a one byte unsigned integer
def _write_char(f, integer):
    packed_integer = struct.pack("!B", integer)
    f.write(packed_integer)

def _read_char(f)
    packed_integer = f.read(4)
    integer = struct.unpack("!B", packed_integer)

    return integer

# for writing a 4 byte unsigned integer
def _write_integer(f, integer):
    packed_integer = struct.pack("!I", integer)
    f.write(packed_integer)

def _read_integer(f)
    packed_integer = f.read(4)
    integer = struct.unpack("!I", packed_integer)

    return integer

# for sending strings that are at most 255 bytes long
def _write_short_str(f, string):
    length = len(string)
    _write_char(f, length)
    f.write(string)

def _read_short_str(f):
    length = _read_char(f)
    webfinger_address = f.read(length)

# for sending strings that are at most 255**4 bytes long
def _write_str(f, string):
    length = len(string)
    _write_integer(f, length)
    f.write(string)

def _read_str(f):
    length = _read_integer(f)
    string = f.read(length)

    return string

# for sending unicode objects
def _write_unicode(f, u):
    string = u.encode("utf8")
    _write_str(f, string)

def _read_unicode(f):
    string = _read_string(f)
    u = unicode(string, "utf8")

    return u

###################

class Message:
    message_type = None

    def write(self, f):
        raise NotImplementedError, "override this function in subclasses!"

    @classmethod
    def read(cls, f):
        raise NotImplementedError, "override this function in subclasses!"

class Terminator(Message):
    message_type = 't'

    def write(self, f):
        f.write(self.message_type)

    @classmethod
    def read(cls, f):
        # read message type
        message_type = f.read(1)
        if not message_type==cls.message_type: return None

        return cls()

terminator = Terminator()

class DeletionRequest(Message):
    message_type = 'd'

    binhash = None
    retrieval_timestamp = None  # if None, partner does not take responsibility

    def __init__(self, ghost=None):
        if ghost:
            assert not ghost.retrieval_timestamp==None

            now = time.time()
            if now-ghost.retrieval_timestamp < RESPONSIBILITY_TIMESPAN:
                self.retrieval_timestamp = ghost.retrieval_timestamp

            self.binhash = ghost.binhash

    def write(self, f):
        # send message type
        f.write(self.message_type)

        # send binhash
        _write_short_str(f, binhash)

        # send retrieval_timestamp
        _write_integer(f, self.retrieval_timestamp)

    @classmethod
    def read(cls, f):
        # read message type
        message_type = f.read(1)
        if not message_type==cls.message_type: return None

        # read binhash
        binhash = _read_short_str(f)

        # read retrieval_timestamp
        retrieval_timestamp = _read_integer(f)

        # return the delete request
        deletion_request = cls()
        deletion_request.binhash = binhash
        deletion_request.retrieval_timestamp = retrieval_timestamp

        return deletion_request

class StateRequest(Message):
    message_type = 'r'

    binhash = None

    def __init__(self, binhash):
        self.binhash = binhash

    def write(self, f):
        # send message type
        f.write(self.message_type)

        # send binhash
        _write_short_str(f, binhash)

    @classmethod
    def read(cls, f):
        # read message type
        message_type = f.read(1)
        if not message_type==cls.message_type: return None

        # read binhash
        binhash = _read_short_str(f)

        # return the state request
        state_request = cls(binhash)
        return state_request

class Synchronization:
    missing_hashes = None
    preliminary_invalid_states = None
    request_hashes = None
    requests = None

    def __init__(self, missing_hashes)
        self.missing_hashes = missing_hashes

    def send_deletion_requests(self, f, statedb):
        """ filter out ghost states the partner doesn't know yet and tell him """

        deleted_hashes = set()

        for ghost in statedb.get_ghosts(self.missing_hashes)
            deleted_hashes += ghost.hash

            if not ghost.retrieval_timestamp==None:
                deletion_request = DeletionRequest(ghost)
                deletion_request.transmit(f)

        terminator.transmit(f)
        f.flush()

        self.request_hashes = self.missing_hashes - deleted_hashes
        self.missing_hashes = None

    def receive_deletion_requests(self, f, statedb):
        """ Receive delete requests and construct preliminary invalid states from them. These states
            may be omitted later if there is a new valid state for the same webfinger address. """

        self.preliminary_invalid_states = {}

        while True:
            deletion_request = DeletionRequest.receive(f)
            if not deletion_request: break

            invalid_state = statedb.get_invalid_state(delete_request.hash, delete_request.retrieval_timestamp)
            self.preliminary_invalid_states[invalid_state.address] = invalid_state

    def send_state_requests(self, f):
        """ request valid states """

        for binhash in self.request_hashes:
            request = StateRequest(binhash)
            request.transmit(f)

        terminator.transmit(f)
        f.flush()

        self.request_hashes = None

    def receive_states(self, f):
        """ Receive valid states, removing the preliminarily constructed invalid
            states for the respective webfinger addresses. Will yield the received
            states and the remaining invalid states. """

        while True:
            message = StateMessage.receive(f)
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

    def receive_state_requests(self, f):
        """ receive requests for valid states """

        self.requests = []

        while True:
            request = StateRequest.receive(f)
            if not request: break

            self.requests.append(request)

    def send_states(self, f, statedb):
        """ answer state requests """

        for request in self.requests:
            state = statedb.get_valid_state(request.hash)
            message = StateMessage(state)
            message.transmit(f)

        terminator.transmit(f)
        f.flush()
