#!/usr/bin/env python

import os, threading, time

from sduds.constants import *
from sduds.states import *
from sduds.hashtrie import HashTrie

# sqlalchemy mapping for State and Ghost classes
import sqlalchemy, sqlalchemy.orm
import sduds.lib.sqlalchemyExtensions as sqlalchemyExt

metadata = sqlalchemy.MetaData()

state_table = sqlalchemy.Table('states', metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("hash", sqlalchemyExt.Binary, index=True, unique=True),
    sqlalchemy.Column("webfinger_address", sqlalchemyExt.String(MAX_ADDRESS_LENGTH), index=True, unique=True),
    sqlalchemy.Column("full_name", sqlalchemy.UnicodeText),
    sqlalchemy.Column("hometown", sqlalchemy.UnicodeText),
    sqlalchemy.Column("country_code", sqlalchemyExt.String(MAX_COUNTRY_CODE_LENGTH)),
    sqlalchemy.Column("services", sqlalchemyExt.String(MAX_SERVICES_LENGTH)),
    sqlalchemy.Column("captcha_signature", sqlalchemyExt.Binary),
    sqlalchemy.Column("submission_timestamp", sqlalchemy.Integer),
    sqlalchemy.Column("retrieval_timestamp", sqlalchemy.Integer)
)

ghost_table = sqlalchemy.Table('ghosts', metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("hash", sqlalchemyExt.Binary, index=True, unique=True),
    sqlalchemy.Column("retrieval_timestamp", sqlalchemy.Integer)
)

variables_table = sqlalchemy.Table('variables', metadata,
    sqlalchemy.Column("cleanup_timestamp", sqlalchemy.Integer)
)

sqlalchemy.orm.mapper(State, state_table, extension=sqlalchemyExt.CalculatedPropertyExtension({"hash":"_hash"}),
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
        self.hashtrie = HashTrie(hashtrie_path)
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
            query = session.query(State).filter(age > PROFILE_LIFETIME)

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
        # TODO: for performance: state -> states, then in assimilation_worker: wait some seconds to acquire states, then save all of them.
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

            self.hashtrie.close()
