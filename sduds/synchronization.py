#!/usr/bin/env python

import struct, time
from states import Profile, State # to be able to construct these objects from messages

from constants import *

""" functions to send and receive some basic types over the network,
    using the file descriptor obtained from socket.makefile() """

# for sending a one byte unsigned integer
def _write_char(f, integer):
    packed_integer = struct.pack("!B", integer)
    f.write(packed_integer)

def _read_char(f):
    packed_integer = f.read(1)
    integer, = struct.unpack("!B", packed_integer)

    return integer

# for sending a 4 byte unsigned integer
def _write_integer(f, integer):
    packed_integer = struct.pack("!I", integer)
    f.write(packed_integer)

def _read_integer(f):
    packed_integer = f.read(4)
    integer, = struct.unpack("!I", packed_integer)

    return integer

# for sending strings that are at most 255 bytes long
def _write_short_str(f, string):
    length = len(string)
    _write_char(f, length)
    f.write(string)

def _read_short_str(f):
    length = _read_char(f)
    string = f.read(length)

    return string

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
    string = _read_str(f)
    u = unicode(string, "utf8")

    return u

###################

class Message:
    """ Interface for message objects, which are objects that can be sent or
        received over the network. """

    message_type = None

    def write(self, f):
        """ The write method sends an serialized version of the object to the
            file-like object f. """

        raise NotImplementedError("override this function in subclasses!")

    @classmethod
    def read(cls, f):
        """ The read classmethod gets a serialized version of the message from
            the file-like object f and returns the corresponding python object.
            If the transmitted message type does not match, None is returned.
        """

        raise NotImplementedError("override this function in subclasses!")

class Terminator(Message):
    """ The terminator message is used to signalize the end of a stream of
        messages. """

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
    """ The DeletionRequest message is used to ask the partner to delete a
        specific hash from his database. We must tell the partner when we got
        the information that the corresponding state is invalid with the
        retrieval_timestamp variable. If the retrieval_timestamp is set to
        None, this means that the partner should not believe us but look at
        the profile himself. """

    message_type = 'd'

    binhash = None
    retrieval_timestamp = None  # if None, partner does not take responsibility

    def __init__(self, ghost=None):
        if ghost:
            assert ghost.retrieval_timestamp is not None

            now = time.time()
            if now < ghost.retrieval_timestamp + MAX_AGE:
                self.retrieval_timestamp = ghost.retrieval_timestamp

            self.binhash = ghost.binhash

    def write(self, f):
        # send message type
        f.write(self.message_type)

        # send binhash
        _write_short_str(f, self.binhash)

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
    """ The StateRequest message asks the partner to send the state
        corresponding to a certain hash to us. """

    message_type = 'r'

    binhash = None

    def __init__(self, binhash):
        self.binhash = binhash

    def write(self, f):
        # send message type
        f.write(self.message_type)

        # send binhash
        _write_short_str(f, self.binhash)

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

class StateMessage(Message):
    """ The StateMessage is used to transmit either a complete valid state, with
        both retrieval_timestamp and profile set, or just to ask the partner to
        look at the online profile by himself (retrieval_timestamp and profile
        both None). """

    message_type = 's'

    state = None

    def __init__(self, state):
        self.state = state

    def write(self, f):
        # send message type
        f.write(self.message_type)

        # send webfinger address
        _write_str(f, self.state.address)

        # send retrieval timestamp
        if self.state.retrieval_timestamp is None:
            f.write('\0')
            assert not self.state.profile
        else:
            f.write('T')
            _write_integer(f, self.state.retrieval_timestamp)
            assert self.state.profile

        # send profile
        if self.state.profile:
            _write_unicode(f, self.state.profile.full_name)
            _write_unicode(f, self.state.profile.hometown)
            _write_str(f, self.state.profile.country_code)
            _write_str(f, self.state.profile.services)
            _write_integer(f, self.state.profile.submission_timestamp)
            _write_str(f, self.state.profile.captcha_signature)

    @classmethod
    def read(cls, f):
        # read message type
        message_type = f.read(1)
        if not message_type==cls.message_type: return None

        # read webfinger address
        address = _read_str(f)

        announcement = f.read(1)
        if announcement=='\0':
            retrieval_timestamp = None
            profile = None
        else:
            # read retrieval timestamp
            retrieval_timestamp = _read_integer(f)

            # receive profile
            full_name = _read_unicode(f)
            hometown = _read_unicode(f)
            country_code = _read_str(f)
            services = _read_str(f)
            submission_timestamp = _read_integer(f)
            captcha_signature = _read_str(f)

            profile = Profile(full_name, hometown, country_code, services,
                              captcha_signature, submission_timestamp)

        state = State(address, retrieval_timestamp, profile)

        return cls(state)

class Synchronization:
    """ This is a helper class to avoid duplicated code blocks in the
        synchronization functions. """

    missing_hashes = None
    preliminary_invalid_states = None
    request_hashes = None
    requests = None

    def __init__(self, missing_hashes):
        self.missing_hashes = missing_hashes

    def send_deletion_requests(self, f, statedb):
        """ filter out ghost states the partner doesn't know yet and tell him
        """

        deleted_hashes = set()

        for ghost in statedb.get_ghosts(self.missing_hashes):
            deleted_hashes += ghost.hash

            if ghost.retrieval_timestamp is not None:
                deletion_request = DeletionRequest(ghost)
                deletion_request.write(f)

        terminator.write(f)
        f.flush()

        self.request_hashes = self.missing_hashes - deleted_hashes
        self.missing_hashes = None

    def receive_deletion_requests(self, f, statedb):
        """ Receive delete requests and construct preliminary invalid states
            from them. These states may be omitted later if there is a new valid
            state for the same webfinger address. """

        self.preliminary_invalid_states = {}

        while True:
            deletion_request = DeletionRequest.read(f)
            if not deletion_request: break

            binhash = deletion_request.binhash
            timestamp = deletion_request.retrieval_timestamp

            state = statedb.get_invalid_state(binhash, timestamp)
            self.preliminary_invalid_states[state.address] = state

    def send_state_requests(self, f):
        """ request valid states """

        for binhash in self.request_hashes:
            request = StateRequest(binhash)
            request.write(f)

        terminator.write(f)
        f.flush()

        self.request_hashes = None

    def receive_states(self, f):
        """ Receive valid states, removing the preliminarily constructed invalid
            states for the respective webfinger addresses. Will yield the
            received states and the remaining invalid states. """

        while True:
            message = StateMessage.read(f)
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
            request = StateRequest.read(f)
            if not request: break

            self.requests.append(request)

    def send_states(self, f, statedb):
        """ answer state requests """

        for request in self.requests:
            state = statedb.get_valid_state(request.binhash)
            message = StateMessage(state)
            message.write(f)

        terminator.write(f)
        f.flush()
