#!/usr/bin/env python

import urllib, json, pywebfinger, binascii, hashlib
import os, threading, time

from constants import *

import lib
from hashtrie import HashTrie

class ValidationFailed(AssertionError):
    info = None

    def __init__(self, info, message):
        AssertionError.__init__(self, message)
        self.info = info

    def __str__(self):
        r = self.message+"\n"
        r += "\n"
        r += "Info:\n"
        r += self.info
        return r

class InvalidProfileException(ValidationFailed):
    def __str__(self):
        r = ValidationFailed.__str__(self)
        r = "Profile invalid: "+r

class InvalidStateException(ValidationFailed):
    def __str__(self):
        r = ValidationFailed.__str__(self)
        r = "State invalid: "+r

class RecentlyExpiredStateException(Exception):
    info = None

    def __init__(self, info, reference_timestamp):
        message = "State recently expired (reference timestamp: %d" % reference_timestamp
        Exception.__init__(self, message)

        self.info = info

class Profile:
    full_name = None # unicode
    hometown = None # unicode
    country_code = None # str
    services = None # str
    captcha_signature = None # str
    submission_timestamp = None # int

    def __init__(self, full_name, hometown, country_code, services, captcha_signature, submission_timestamp):
        self.full_name = full_name
        self.hometown = hometown
        self.country_code = country_code
        self.services = services
        self.captcha_signature = captcha_signature
        self.submission_timestamp = submission_timestamp

    def __composite_values__(self):
        return self.full_name, self.hometown, self.country_code, self.services, self.captcha_signature, self.submission_timestamp

    def __str__(self):
        s = "Full name: "+self.full_name.encode("utf8")+"\n"+\
            "Hometown: "+self.hometown.encode("utf8")+"\n"+\
            "Country code: "+self.country_code+"\n"+\
            "Services: "+self.services+"\n"+\
            "Captcha signature: "+binascii.hexlify(self.captcha_signature[:8])+"...\n"+\
            "Submission time: "+time.ctime(self.submission_timestamp)

        return s

    def assert_validity(self, webfinger_address, reference_timestamp=None):
        """ Validates the profile against a certain webfinger address. Checks CAPTCHA signature,
            submission_timestamp, and field lengths. Also checks whether webfinger address is
            too long. """

        if reference_timestamp is None:
            reference_timestamp = time.time()

        # validate CAPTCHA signature for given webfinger address
        assert lib.signature_valid(CAPTCHA_PUBLIC_KEY, self.captcha_signature, webfinger_address),\
            InvalidProfileException(str(self), "Invalid captcha signature")

        # assert that submission_timestamp is not in future
        assert self.submission_timestamp <= reference_timestamp,\
            InvalidProfileException(str(self), "Submitted in future (reference timestamp: %d)" % reference_timestamp)

        # check lengths of webfinger address
        assert len(webfinger_address)<=MAX_ADDRESS_LENGTH,\
            InvalidProfileException(str(self), "Address too long")

        # check lengths of profile fields
        assert len(self.full_name.encode("utf8"))<=MAX_NAME_LENGTH,\
            InvalidProfileException(str(self), "Full name too long")

        assert len(self.hometown.encode("utf8"))<=MAX_HOMETOWN_LENTGTH,\
            InvalidProfileException(str(self), "Hometown too long")

        assert len(self.country_code)<=MAX_COUNTRY_CODE_LENGTH,\
            InvalidProfileException(str(self), "Country code too long")

        assert len(self.services)<=MAX_SERVICES_LENGTH,\
            InvalidProfileException(str(self), "Services list too long")

        for service in self.services.split(","):
            assert len(service)<=MAX_SERVICE_LENGTH,\
                InvalidProfileException(str(self), "Service %s too long" % service)

        return True

    @classmethod
    def retrieve(cls, address):
        wf = pywebfinger.finger(address)

        sduds_uri = wf.find_link("http://hoegners.de/sduds/spec", attr="href")

        f = urllib.urlopen(sduds_uri)
        json_string = f.read()
        f.close()

        json_dict = json.loads(json_string)

        full_name = json_dict["full_name"]
        hometown = json_dict["hometown"]
        country_code = json_dict["country_code"].encode("utf8")
        services = json_dict["services"].encode("utf8")
        captcha_signature = binascii.unhexlify(json_dict["captcha_signature"])

        submission_timestamp = int(json_dict["submission_timestamp"])

        profile = cls(
            full_name,
            hometown,
            country_code,
            services,
            captcha_signature,
            submission_timestamp
        )

        return profile

class State(object):
    address = None
    retrieval_timestamp = None
    profile = None

    def __init__(self, address, retrieval_timestamp, profile):
        self.address = address
        self.retrieval_timestamp = retrieval_timestamp
        self.profile = profile

    def __eq__(self, other):
        assert self.retrieval_timestamp is not None
        assert other.retrieval_timestamp is not None

        assert self.address==other.address

        if self.profile and other.profile:
            return self.hash==other.hash
        elif not self.profile and not other.profile:
            return True
        else:
            return False

    def __str__(self):
        s = "Webfinger address: "+self.address+"\n"
        s += "Retrieval time: "+time.ctime(self.retrieval_timestamp)+"\n"

        if self.profile is not None:
            s += "Hash: "+binascii.hexlify(self.hash)+"\n"

        s += "PROFILE:\n"
        s += str(self.profile)

        return s

    def assert_validity(self, reference_timestamp=None):
        """ Checks if a state was valid at a given time. Returns True if it was, raises
            an exception otherwise. """

        assert self.retrieval_timestamp is not None

        if reference_timestamp is None:
            reference_timestamp = time.time()

        assert self.retrieval_timestamp <= reference_timestamp,\
            InvalidStateException(str(self), "Retrieved in future (reference_timestamp: %d)" % reference_timestamp)

        assert self.retrieval_timestamp >= reference_timestamp - MAX_AGE,\
            InvalidStateException(str(self), "Not up to date (reference timestamp: %d)" % reference_timestamp)

        if self.profile:
            self.profile.assert_validity(self.address, reference_timestamp)

            assert self.retrieval_timestamp>=self.profile.submission_timestamp,\
                InvalidStateException(str(self), "Retrieval time lies before submission time")

            expiry_date = self.profile.submission_timestamp + STATE_LIFETIME

            if reference_timestamp>expiry_date:
                assert reference_timestamp < expiry_date + EXPIRY_GRACE_PERIOD,\
                    InvalidStateException(str(self), "State expired (reference timestamp: %d" % reference_timestamp)

                raise RecentlyExpiredStateException(str(self), reference_timestamp)

        return True

    @classmethod
    def retrieve(cls, address):
        try:
            profile = Profile.retrieve(address)
        except: # TODO: which exceptions?
            profile = None

        retrieval_timestamp = int(time.time())

        state = cls(address, retrieval_timestamp, profile)

        return state

    @property
    def hash(self):
        combinedhash = hashlib.sha1()

        relevant_data = [self.address, self.profile.full_name,
            self.profile.hometown, self.profile.country_code,
            self.profile.services, int(self.profile.submission_timestamp)]

        for data in relevant_data:
            # convert data to string
            if type(data)==unicode:
                data_str = data.encode("utf8")
            else:
                data_str = str(data)

            # TODO: take better hash function? (also for combinedhash)
            subhash = hashlib.sha1(data_str).digest()
            combinedhash.update(subhash)

        # TODO: is it unsecure to take only 16 bytes of the hash?
        binhash = combinedhash.digest()[:16]
        return binhash

class Ghost(object):
    hash = None
    retrieval_timestamp = None

    def __init__(self, binhash, retrieval_timestamp):
        self.hash = binhash
        self.retrieval_timestamp = retrieval_timestamp

# sqlalchemy mapping for State and Ghost classes
import sqlalchemy, sqlalchemy.orm

metadata = sqlalchemy.MetaData()

state_table = sqlalchemy.Table('states', metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("hash", lib.Binary, index=True, unique=True),
    sqlalchemy.Column("webfinger_address", lib.Text(MAX_ADDRESS_LENGTH), index=True, unique=True),
    sqlalchemy.Column("full_name", sqlalchemy.UnicodeText),
    sqlalchemy.Column("hometown", sqlalchemy.UnicodeText),
    sqlalchemy.Column("country_code", lib.Text(MAX_COUNTRY_CODE_LENGTH)),
    sqlalchemy.Column("services", lib.Text(MAX_SERVICES_LENGTH)),
    sqlalchemy.Column("captcha_signature", lib.Binary),
    sqlalchemy.Column("submission_timestamp", sqlalchemy.Integer),
    sqlalchemy.Column("retrieval_timestamp", sqlalchemy.Integer)
)

ghost_table = sqlalchemy.Table('ghosts', metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("hash", lib.Binary, index=True, unique=True),
    sqlalchemy.Column("retrieval_timestamp", sqlalchemy.Integer)
)

variables_table = sqlalchemy.Table('variables', metadata,
    sqlalchemy.Column("cleanup_timestamp", sqlalchemy.Integer)
)

sqlalchemy.orm.mapper(State, state_table, extension=lib.CalculatedPropertyExtension({"hash":"_hash"}),
    properties={
        "id": state_table.c.id,
        "hash": sqlalchemy.orm.synonym('_hash', map_column=True),
        "address": state_table.c.webfinger_address,
        "retrieval_timestamp": state_table.c.retrieval_timestamp,
        "profile": sqlalchemy.orm.composite(Profile, state_table.c.full_name, state_table.c.hometown, state_table.c.country_code, state_table.c.services, state_table.c.captcha_signature, state_table.c.submission_timestamp)
    }
)

sqlalchemy.orm.mapper(Ghost, ghost_table,
    properties={
        "id": ghost_table.c.id,
        "hash": ghost_table.c.hash,
        "retrieval_timestamp": ghost_table.c.retrieval_timestamp,
    }
)

class StateDatabase:
    database_path = None # for erasing when closing
    hashtrie = None
    Session = None
    lock = None

    cleanup_timestamp = None

    def __init__(self, hashtrie_path, statedb_path, erase=False):
        self.database_path = statedb_path
        self.hashtrie = HashTrie(hashtrie_path, erase=erase)
        self.lock = threading.Lock()

        if erase and os.path.exists(statedb_path):
            os.remove(statedb_path)

        engine = sqlalchemy.create_engine("sqlite:///"+statedb_path)
        metadata.create_all(engine)

        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)

        # initialize cleanup_timestamp so that the cleanup thread can
        # be initialized
        session = self.Session()

        select = variables_table.select(variables_table.c.cleanup_timestamp)
        result = session.execute(select)

        try:
            self.cleanup_timestamp, = result.fetchone()
        except TypeError:
            # insert one empty row; None is kept for self.cleanup_timestamp
            session.execute(variables_table.insert().values())

        session.close()

    def cleanup(self):
        with self.lock:
            session = self.Session()

            now = time.time()
            age = now - state_table.c.submission_timestamp
            query = session.query(State).filter(age > STATE_LIFETIME)

            delete_hashes = []
            for state in query:
                binhash = state.hash
                session.delete(state)
                delete_hashes.append(binhash)

            # save cleanup timestamp to be able to start over next time in case
            # application is closed
            session.execute(variables_table.update().values(cleanup_timestamp=int(now)))

            session.commit()
            session.close()

            self.hashtrie.delete(delete_hashes)

        return now

    def search(self, words=None, services=None, limit=50):
        """ Searches the database for certain words, and yields only profiles
            of users who use certain services.
            'words' must be a list of unicode objects and 'services' must be
            a list of str objects.
            Warning: Probably very slow! """

        if words is None: words = []
        if services is None: services = []

        with self.lock:
            session = self.Session()

            query = session.query(State)

            for word in words:
                like_str = "%" + word.encode("utf8") + "%"
                like_unicode = u"%" + word + u"%"

                condition = state_table.c.webfinger_address.like(like_str)
                condition |= state_table.c.full_name.like(like_unicode)
                condition |= state_table.c.hometown.like(like_unicode)
                condition |= state_table.c.country_code.like(like_str)

                query = query.filter(condition)

            for service in services:
                query = query.filter(
                      state_table.c.services.like(service)
                    | state_table.c.services.like(service+",%")
                    | state_table.c.services.like("%,"+service+",%")
                    | state_table.c.services.like("%,"+service)
                )

            if limit is not None:
                query = query.limit(limit)

            for state in query:
                session.expunge(state)
                yield state

            session.close()

    def save(self, state):
        """ returns False if state is discarded, True otherwise """
        with self.lock:
            session = self.Session()

            query = session.query(State).filter(State.address==state.address)
            existing_state = query.scalar()

            if existing_state:
                # discard states we have more recent information about
                if state.retrieval_timestamp<existing_state.retrieval_timestamp:
                    # TODO: logging
                    session.close()
                    return False

            if state.profile and existing_state:
                # make sure that RESUBMISSION_INTERVAL is respected
                current_submission = state.profile.submission_timestamp
                last_submission = existing_state.profile.submission_timestamp
                interval = current_submission - last_submission

                if interval<MIN_RESUBMISSION_INTERVAL:
                    # TODO: logging
                    session.close()
                    return False

            if existing_state:
                # delete existing state
                binhash = existing_state.hash
                retrieval_timestamp = existing_state.retrieval_timestamp

                session.delete(existing_state)

                self.hashtrie.delete([binhash])
                ghost = Ghost(binhash, retrieval_timestamp)
                session.add(ghost)

            if state.profile:
                # add new state
                session.add(state)
                self.hashtrie.add([state.hash])

            session.commit()
            session.close()
            return True

    def get_ghosts(self, binhashes):
        with self.lock:
            session = self.Session()

            for binhash in binhashes:
                query = session.query(Ghost).filter(Ghost.hash==binhash)
                ghost = query.scalar()

                if ghost:
                    session.expunge(ghost)
                    yield ghost

            session.close()

    def get_invalid_state(self, binhash, timestamp):
        with self.lock:
            session = self.Session()
            query = session.query(State.address).filter(State.hash==binhash)
            address, = query.one()
            session.close()

        invalid_state = State(address, timestamp, None)
        return invalid_state

    def get_valid_state(self, binhash):
        with self.lock:
            session = self.Session()
            query = session.query(State).filter(State.hash==binhash)
            state = query.one()
            session.close()

        now = time.time()
        age = now - state.retrieval_timestamp

        if age > MAX_AGE:
            state.profile = None
            state.retrieval_timestamp = None
            return state
        else:
            return state

    def close(self, erase=False):
        with self.lock:
            if self.Session is not None:
                self.Session.close_all()
                self.Session = None

            if erase and os.path.exists(self.database_path):
                os.remove(self.database_path)

            self.hashtrie.close(erase=erase)
