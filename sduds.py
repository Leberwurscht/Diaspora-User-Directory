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

from synchronization import Synchronization
###

MAX_AGE = 3600*24*3 # specifies how long ago a state transmitted by a partner
                    # may have been retrieved so that we still don't have to
                    # retrieve it ourselves

CLEANUP_INTERVAL = 3600*24
EXPIRY_GRACE_PERIOD = 3600*24*3 # if states transmitted by a partner are
                                # expired, only reduce trust in him if grace
                                # period is over.

MIN_RESUBMISSION_INTERVAL = 3600*24*3
STATE_LIFETIME = 3600*24*365

MAX_ADDRESS_LENGTH = 1024
MAX_NAME_LENGTH = 1024
MAX_HOMETOWN_LENTGTH = 1024
MAX_COUNTRY_CODE_LENGTH = 2
MAX_SERVICES_LENGTH = 1024
MAX_SERVICE_LENGTH = 16

###
# Authentication functionality

def authenticate_socket(sock, username, password):
    """ Authenticates a socket using the HMAC-SHA512 algorithm. This is the
        counterpart of AuthenticatingRequestHandler. """
    f = sock.makefile()

    # make sure method is HMAC with SHA512
    method = f.readline().strip()
    if not method=="HMAC-SHA512":
        # TODO: logging
        f.close()
        sock.close()
        return False

    # send username
    f.write(username+"\n")
    f.flush()

    # receive challenge
    challenge = f.readline().strip()

    # send response
    response = hmac.new(password, challenge, hashlib.sha512).hexdigest()
    f.write(response+"\n")
    f.flush()

    # check answer
    answer = f.readline().strip()
    f.close()

    if answer=="ACCEPTED":
        return True
    else:
        sock.close()
        return False

class AuthenticatingRequestHandler(SocketServer.BaseRequestHandler):
    """ RequestHandler which checks the credentials transmitted by the other side
        using the HMAC-SHA512 algorithm.
        The get_password method must be overridden and return the password for a
        certain user. If the other side is authenticated, handle_user is called
        with the username as argument. """

    def handle(self):
        f = self.request.makefile()

        # send expected authentication method
        method = "HMAC-SHA512"
        f.write(method+"\n")
        f.flush()

        # receive username
        username = f.readline().strip()

        # send challenge
        challenge = uuid.uuid4().hex
        f.write(challenge+"\n")
        f.flush()

        # receive response
        response = f.readline().strip()

        # compute response
        password = self.get_password(username)
        if password==None:
            f.write("DENIED\n")
            f.close()
            return

        computed_response = hmac.new(password, challenge, hashlib.sha512).hexdigest()

        # check response
        if not response==computed_response:
            f.write("INVALID PASSWORD\n")
            f.close()
            return

        f.write("ACCEPTED\n")
        f.close()

        self.handle_partner(username)

    def get_password(self, username):
        raise NotImplementedError, "Override this function in subclasses!"

    def handle_user(self, username):
        raise NotImplementedError, "Override this function in subclasses!"

class SynchronizationRequestHandler(AuthenticatingRequestHandler):
    """ Authenticates partners and calls synchronize_as_server if successful. """

    def get_password(self, partner_name):
        context = self.server.context
        partner = context.statedb.get_partner(partner_name)

        if partner==None:
            return None
        elif partner.kicked:
            return None
        else:
            return partner.password

    def handle_user(self, partner_name):
        context = self.server.context
        partersocket = self.request

        context.synchronize_as_server(partnersocket, partner_name)

class SynchronizationServer(lib.BaseServer):
    """ Waits for partners to synchronize. """

    context = None
    public_address = None

    def __init__(self, context, fqdn, interface, port):
        # initialize server
        address = (interface, port)
        lib.BaseServer.__init__(self, address, SynchronizationRequestHandler)

        # expose context so that the RequestHandler can access it
        self.context = context

        # expose public address
        self.public_address = (fqdn, port)

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

    def assert_validity(self, webfinger_address, reference_timestamp=None):
        """ Validates the profile against a certain webfinger address. Checks CAPTCHA signature,
            submission_timestamp, and field lengths. Also checks whether webfinger address is
            too long. """

        if reference_timestamp==None:
            reference_timestamp = time.time()

        # validate CAPTCHA signature for given webfinger address
        if not signature_valid(self.captcha_signature, webfinger_address, CAPTCHA_PUBLIC_KEY):
            raise InvalidCaptchaSignature(self.captcha_signature, webfinger_address)

        # assert that submission_timestamp is not in future
        if not self.submission_timestamp <= reference_timestamp:
            raise SubmittedInFutureException(self.submission_timestamp,\
                                             reference_timestamp)

        # check lengths of webfinger address 
        if len(webfinger_address)>MAX_ADDRESS_LENGTH:
            raise InvalidAddressException(webfinger_address)

        # check lengths of profile fields
        if len(self.full_name.encode("utf8"))>MAX_NAME_LENGTH:
            raise InvalidFullNameException(self.full_name)

        if len(self.hometown.encode("utf8"))>MAX_HOMETOWN_LENTGTH:
            raise InvalidHometownException(self.hometown)

        if len(self.country_code)>MAX_COUNTRY_CODE_LENGTH:
            raise InvalidCountryCodeException(self.country_code)

        if len(self.services)>MAX_SERVICES_LENGTH:
            raise InvalidServicesException(self.services)

        for service in services.split(","):
            if len(service)>MAX_SERVICE_LENGTH:
                raise InvalidServicesException(self.services)

        return True

    @classmethod
    def retrieve(cls, address):
        wf = pywebfinger.finger(webfinger_address)

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
        assert not self.retrieval_timestamp==None
        assert not other.retrieval_timestamp==None

        assert self.address==other.address

        if self.profile and other.profile:
            return self.hash==other.hash
        elif not self.profile and not other.profile:
            return True
        else:
            return False

    def assert_validity(reference_timestamp=None):
        """ Checks if a state was valid at a given time. Returns True if it was, raises
            an exception otherwise. """

        assert not self.retrieval_timestamp==None

        if reference_timestamp==None:
            reference_timestamp = time.time()

        if not self.retrieval_timestamp <= reference_timestamp:
            raise RetrievedInFutureException(retrieval_timestamp,\
                                             reference_timestamp)

        if not self.retrieval_timestamp >= reference_timestamp - MAX_AGE:
            raise NotUpToDateException(retrieval_timestamp, reference_timestamp)

        if self.profile:
            self.profile.assert_validity(self.address, self.reference_timestamp)

            if not self.retrieval_timestamp>=self.profile.submission_timestamp:
                raise InvalidRetrievalTimestampException(retrieval_timestamp,\
                        self.profile.submission_timestamp)

            expiry_date = self.profile.submission_timestamp + STATE_LIFETIME

            if reference_timestamp>expiry_date:
                if reference_timestamp > expiry_date + EXPIRY_GRACE_PERIOD:
                    raise ExpiredException(reference_timestamp, self.profile.submission_timestamp)
                else:
                    raise RecentlyExpiredException(reference_timestamp, self.profile.submission_timestamp) # does not inherit from violation

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
            self.profile.services, self.profile.submission_timestamp]

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

    def __init__(self, hashtrie_path, statedb_path, erase=False):
        self.database_path = statedb_path
        self.hashtrie = HashTrie(hashtrie_path, erase=erase)
        self.lock = threading.Lock()

        if erase and os.path.exists(statedb_path):
            os.remove(self.statedb_path)

        engine = sqlalchemy.create_engine("sqlite:///"+statedb_path)
        metadata.create_all(engine)

        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)

    def cleanup(self):
        with self.lock:
            session = self.Session()

            now = time.time()
            age = now - state_table.c.submission_timestamp
            query = session.query(State).filter(age > STATE_LIFETIME)

            delete_hashes = []
            for state in query:
                binhash = state.hash
                session.delete(entry)
                delete_hashes.add(binhash)

            session.commit()
            session.close()

            self.hashtrie.delete(delete_hashes)

        return now

    def search(self, words=[], services=[], limit=50):
        """ Searches the database for certain words, and yields only profiles
            of users who use certain services.
            'words' must be a list of unicode objects and 'services' must be
            a list of str objects.
            Warning: Probably very slow! """

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

            if not limit==None:
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

                self.hashtrie.delete(binhash)
                ghost = Ghost(binhash, retrieval_timestamp)
                session.add(ghost)

            if state.profile:
                # add new state
                session.add(state)
                self.hashtrie.add(state.hash)

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
            if not self.Session==None:
                self.Session.close_all()
                self.Session = None

            if erase and os.path.exists(self.database_path):
                os.remove(self.database_path)

            self.hashtrie.close(erase=erase)

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
                partnerdb.register_control_sample(partner_name)

                retrieved_state = State.retrieve(self.state.address)

                if not self.state==retrieved_state:
                    partnerdb.register_offense(partner_name, self.state, retrieved_state)

                    trusted_state = retrieved_state
                    partner_name = None
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

            self.statedb = StateDatabase(kwargs["hashtrie_path"], kwargs["entrydb_path"], erase=erase)

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

        self.statedb.close(erase=erase)
        self.partnerdb.close(erase=erase)

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

class Application:
    self.context = None

    self.ready_for_synchronization = None

    self.web_server = None
    self.synchronization_server = None
    self.jobs = None
    self.submission_workers = None
    self.validation_workers = None
    self.assimilation_worker = None

    def __init__(self, context):
        self.context = context

        # need this for waiting until expired states are deleted
        self.ready_for_synchronization = threading.Event()

        # set default values
        self.jobs = []
        self.submission_workers = []
        self.validation_workers = []

    def configure_web_server(self, interface="", port=20000):
        self.web_server = WebServer(self.context, interface, port, self.published_synchronization_address)

    def configure_synchronization_server(self, fqdn, interface="", port=20001):
        self.synchronization_server = SynchronizationServer(self.context, fqdn, interface, port)

    def configure_workers(self, submission_workers=5, validation_workers=5):
        # submission workers
        for i in xrange(submission_workers):
            worker = threading.Thread(target=self.submission_worker_function)
            self.submission_workers.append(worker)

        # validation workers
        for i in xrange(validation_workers):
            worker = threading.Thread(target=self.validation_worker_function)
            self.submission_workers.append(worker)

        # assimilation worker
        self.assimilation_worker = threading.Thread(target=self.assimilation_worker_function)

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

    def submission_worker_function(self):
        while True:
            submission = self.context.submission_queue.get()
            if submission==None:
                self.context.submission_queue.task_done()
                return

            try:
                state = State.retrieve(submission.webfinger_address)
            except Exception, e:
                # TODO: logging
                self.context.submission_queue.task_done()
                continue

            claim = Claim(state)

            self.context.validation_queue.put(claim, True)
            self.context.submission_queue.task_done()
        
    def validation_worker_function(self):
        while True:
            claim = self.context.validation_queue.get()
            if claim==None:
                self.context.validation_queue.task_done()
                return

            validated_state = claim.validate(self.context.partnerdb)
            if validated_state:
                self.context.assimilation_queue.put(validated_state, True)

            self.context.validation_queue.task_done()

    def assimilation_worker_function(self):
        while True:
            state = self.context.assimilation_queue.get()
            if state==None:
                self.context.assimilation_queue.task_done()
                return

            self.context.statedb.save(state)
            self.context.assimilation_queue.task_done()

    def submit_address(self, webfinger_address):
        try:
            submission = Submission(webfinger_address)
            self.context.submission_queue.put(submission)
            return True
        except Queue.Full:
            self.context.logger.warning("Submission queue full, rejected %s!" % webfinger_address)
            return False

    def synchronize_with_partner(self, partner_name):
        # do not synchronize as long as we might have expired states
        self.ready_for_synchronization.wait()

        # register synchronization attempt
        timestamp = self.context.partnerdb.register_connection(partner_name)
        
        # get the synchronization address
        try:
            host, synchronization_port = partnerdb.get_synchronization_address(partner_name)
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
            success = authenticate_socket(partnersocket, partner)
        except Exception, e:
            self.context.logger.warning("Unable to authenticate to partner %s for synchronization: %s" % (partner_name, str(e)))
            return timestamp

        if not success:
            self.context.logger.warning("Invalid credentials for partner %s!" % partner_name)
            return timestamp

        # conduct synchronization
        try:
            self.context.synchronize_as_client(partnersocket)
        except Exception, e:
            self.context.logger.warning("Unable to synchronize with partner %s: %s" % (partner_name, str(e)))

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
