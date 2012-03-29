#!/usr/bin/env python

import random
import urllib, json

from constants import *

class Partner(object):
    name = None             # may not contain newline
    accept_password = None  # may not contain newline

    base_url = None
    control_probability = None
    last_connection = None
    kicked = None

    connection_schedule = None
    provide_username = None # may not contain newline
    provide_password = None # may not contain newline

    def __init__(self, name, accept_password, base_url, control_probability, connection_schedule=None, provide_username=None, provide_password=None):
        self.name = name
        self.accept_password = accept_password
        self.base_url = base_url
        self.control_probability = control_probability
        self.connection_schedule = connection_schedule
        self.provide_username = provide_username
        self.provide_password = provide_password
        self.kicked = False

    def get_synchronization_address(self):
        assert self.base_url.endswith("/")
        address_url = self.base_url+"synchronization_address"

        data = urllib.urlopen(address_url).read()
        host, control_port = json.loads(data)
        host = host.encode("utf8")

        assert type(host)==str
        assert type(control_port)==int

        return (host, control_port)

    def control_sample(self):
        return random.random()<self.control_probability

    def __str__(self):
        r = "Partner(name=%s, base_url=%s, control_probability=%f"
        r %= (self.name, self.base_url, self.control_probability)

        if self.connection_schedule:
            a = ", schedule=%s, provide_username=%s"
            a %= (self.connection_schedule, self.provide_username)
            r += a

        if self.kicked:
            r += ", kicked)"
        else:
            r += ")"

        return r

# sqlalchemy mapping for Partner class
import lib.sqlalchemyExtensions as sqlalchemyExt
import sqlalchemy, sqlalchemy.orm, sqlalchemy.ext.declarative

metadata = sqlalchemy.MetaData()

partner_table = sqlalchemy.Table('partners', metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemyExt.String, unique=True),
    sqlalchemy.Column("accept_password", sqlalchemyExt.String),
    sqlalchemy.Column("base_url", sqlalchemyExt.String),
    sqlalchemy.Column("control_probability", sqlalchemy.Float),
    sqlalchemy.Column("last_connection", sqlalchemy.Integer),
    sqlalchemy.Column("kicked", sqlalchemy.Boolean),
    sqlalchemy.Column("connection_schedule", sqlalchemyExt.String),
    sqlalchemy.Column("provide_username", sqlalchemyExt.String),
    sqlalchemy.Column("provide_password", sqlalchemyExt.String)
)

sqlalchemy.orm.mapper(Partner, partner_table,
    properties={
        "id": partner_table.c.id,
        "name": partner_table.c.name,
        "accept_password": partner_table.c.accept_password,
        "base_url": partner_table.c.base_url,
        "control_probability": partner_table.c.control_probability,
        "last_connection": partner_table.c.last_connection,
        "kicked": partner_table.c.kicked,
        "connection_schedule": partner_table.c.connection_schedule,
        "provide_username": partner_table.c.provide_username,
        "provide_password": partner_table.c.provide_password
    }
)

# control sample and violation classes with mapping
DatabaseObject = sqlalchemy.ext.declarative.declarative_base()

class SuccessfulSamplesSummary(DatabaseObject):
    """ Successful control samples must be stored in a way that we can determine the total
        number of them in a certain time range. This is achieved by summarizing all control
        samples in a shorter time intervals for each partner.
    """

    __tablename__ = "successful_samples"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    #: The ID (integer) of the :class:`Partner` for which the control samples were taken (each saved
    #: :class:`Partner` instance gets an ID from the sqlalchemy mapping).
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey(partner_table.c.id))

    #: The number of the time interval. Time intervals are enumerated the following way:
    #: Timestamp ``n*SAMPLE_SUMMARY_INTERVAL`` is the start of interval ``n``.
    interval = sqlalchemy.Column(sqlalchemy.Integer)

    #: The number of successful control samples by the specified partner in the specified time interval.
    samples = sqlalchemy.Column(sqlalchemy.Integer)

    # TODO: index for better performance

    def __init__(self, partner_id, interval):
        DatabaseObject.__init__(self)

        self.partner_id = partner_id
        self.interval = interval
        self.samples = 0

class FailedSample(DatabaseObject):
    """ Represents a failed control sample.
    """

    __tablename__ = "failed_samples"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    #: The ID (integer) of the :class:`Partner` for which the control sample was taken (each saved
    #: :class:`Partner` instance gets an ID from the sqlalchemy mapping).
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey(partner_table.c.id))

    #: The number of the time interval. Time intervals are enumerated the following way:
    #: Timestamp ``n*SAMPLE_SUMMARY_INTERVAL`` is the start of interval ``n``.
    interval = sqlalchemy.Column(sqlalchemy.Integer)

    #: The webfinger address of the :class:`~sduds.states.Profile` for which the
    #: :class:`~sduds.context.Claim` was made. This is needed because only one failed control sample
    #: per address may be counted, otherwise a single profile owner could get a server kicked by his
    #: partners by changing his profile frequently.
    webfinger_address = sqlalchemy.Column(sqlalchemyExt.String, unique=True)

    # TODO: index for better performance

    def __init__(self, partner_id, interval, webfinger_address):
        DatabaseObject.__init__(self)

        self.partner_id = partner_id
        self.interval = interval
        self.webfinger_address = webfinger_address

class ControlSamplesCache:
    Session = None

    window_end = None
    successful_cache = None
    successful_stored_count = None
    failed_cache = None
    failed_update_count = None
    failed_stored_count = None

    max_cache_size_failed = None

    def __init__(self, Session, window_end, max_cache_size_failed=500):
        self.Session = Session
        self.max_cache_size_failed = max_cache_size_failed

        self._reinitialize_cache(window_end)

    def _commit_successful_cache(self):
        session = self.Session()

        for partner_id in self.successful_cache:
            # get stored summary, is present
            query = session.query(SuccessfulSamplesSummary)
            query = query.filter_by(partner_id=partner_id)
            query = query.filter_by(interval=self.window_end)
            summary = query.scalar()

            # if not present, create new summary
            if summary is None:
                summary = SuccessfulSamplesSummary(partner_id, self.window_end)

            # update summary
            summary.samples += self.successful_cache[partner_id]

            # save summary to database
            session.add(summary)

        session.commit()
        session.close()

    def _commit_failed_cache(self):
        session = self.Session()

        for partner_id in self.failed_cache:
            samples_dict = self.failed_cache[partner_id]
            for failedsample in samples_dict.itervalues():
                session.add(failedsample)

        session.commit()
        session.close()

    def _reinitialize_cache(self, window_end):
        self.window_end = window_end

        self.successful_cache = {}
        self.failed_cache = {}

        self.successful_stored_count = {}
        self.failed_update_count = {}
        self.failed_stored_count = {}

    def _move_forward_to(self, interval):
        self._commit_successful_cache()
        self._commit_failed_cache()
        self._reinitialize_cache(interval)

    def add_successful_sample(self, partner_id, interval):
        assert interval>=self.window_end, "interval must be monotonously increasing"

        # move window forward if necessary
        if not interval==self.window_end:
            self._move_forward_to(interval)

        # add sample to cache
        self.successful_cache.setdefault(partner_id, 0)
        self.successful_cache[partner_id] += 1

    def count_successful_samples(self, partner_id, interval):
        assert interval>=self.window_end, "interval must be monotonously increasing"

        # move window forward if necessary
        if not interval==self.window_end:
            self._move_forward_to(interval)

        # get number of failed samples that are stored in the database
        stored_samples = self.successful_stored_count.get(partner_id, None)
        if stored_samples is None:
            window_start = interval - CONTROL_SAMPLE_WINDOW + 1

            session = self.Session()
            aggregator = sqlalchemy.sql.functions.sum(SuccessfulSamplesSummary.samples)
            query = session.query(aggregator)
            query = query.filter_by(partner_id=partner_id)
            query = query.filter(SuccessfulSamplesSummary.interval>=window_start)
            stored_samples = query.scalar()
            session.close()

            if stored_samples is None:
                # no successful samples found
                stored_samples = 0

            self.successful_stored_count[partner_id] = stored_samples

        # get number of cached samples
        self.successful_cache.setdefault(partner_id, 0)
        cached_samples = self.successful_cache[partner_id]

        # calculate total number of failed samples within window
        successful_samples = stored_samples + cached_samples

        return successful_samples

    def add_failed_sample(self, partner_id, interval, webfinger_address):
        assert interval>=self.window_end, "interval must be monotonously increasing"

        # move window forward if necessary
        if not interval==self.window_end:
            self._move_forward_to(interval)

        # get stored FailedSample for this address, if present
        window_start = interval - CONTROL_SAMPLE_WINDOW + 1

        session = self.Session()
        query = session.query(FailedSample)
        query = query.filter_by(partner_id=partner_id)
        query = query.filter_by(webfinger_address=webfinger_address)
        query = query.filter(FailedSample.interval>=window_start)
        failedsample = query.scalar()
        session.close()

        if failedsample is None:
            # if not present, create new FailedSample
            failedsample = FailedSample(partner_id, interval, webfinger_address)
        else:
            # if present, increase update counter to be able to subtract it in count_failed_samples()
            self.failed_update_count.setdefault(partner_id, 0)
            self.failed_update_count[partner_id] += 1

            failedsample.expunge()

        # write failed sample to cache
        self.failed_cache.setdefault(partner_id, {})
        self.failed_cache[partner_id][webfinger_address] = failedsample

        # check if cache for failed samples has grown too big
        cache_size = 0
        for cache_dict in self.failed_cache.itervalues():
            cache_size += len(cache_dict)

        if cache_size>self.max_cache_size_failed:
            # if this is the case, write cache to database
            self._move_forward_to(interval)

    def count_failed_samples(self, partner_id, interval):
        assert interval>=self.window_end, "interval must be monotonously increasing"

        # move window forward if necessary
        if not interval==self.window_end:
            self._move_forward_to(interval)

        # get number of failed samples that are stored in the database
        stored_samples = self.failed_stored_count.get(partner_id, None)
        if stored_samples is None:
            window_start = interval - CONTROL_SAMPLE_WINDOW + 1

            session = self.Session()
            query = session.query(FailedSample)
            query = query.filter_by(partner_id=partner_id)
            query = query.filter(FailedSample.interval>=window_start)
            stored_samples = query.count()
            session.close()

            self.failed_stored_count[partner_id] = stored_samples

        # get number of cached samples
        self.failed_cache.setdefault(partner_id, {})
        cached_samples = len(self.failed_cache[partner_id])

        # get number of updated samples
        self.failed_update_count.setdefault(partner_id, 0)
        updated_samples = self.failed_update_count[partner_id]

        # calculate total number of failed samples within window
        failed_samples = stored_samples + cached_samples - updated_samples

        return failed_samples

    def cleanup(self, interval):
        assert interval>=self.window_end, "interval must be monotonously increasing"

        # empty caches
        self._commit_successful_cache()
        self._commit_failed_cache()

        # move window forward if necessary
        if not interval==self.window_end:
            self._move_forward_to(interval)

        # clean up database
        window_start = interval - CONTROL_SAMPLE_WINDOW + 1

        session = self.Session()

        query = session.query(SuccessfulSamplesSummary)
        query = query.filter(SuccessfulSamplesSummary.interval<window_start)
        query.delete()

        query = session.query(FailedSample)
        query = query.filter(FailedSample.interval<window_start)
        query.delete()

        session.commit()

    def clear(self, partner_id):
        # clear cache
        if partner_id in self.successful_cache:
            del self.successful_cache[partner_id]

        if partner_id in self.failed_cache:
            del self.failed_cache[partner_id]

        # clear database
        session = self.Session()

        query = session.query(SuccessfulSamplesSummary)
        query = query.filter_by(partner_id=partner_id)
        query.delete()

        query = session.query(FailedSample)
        query = query.filter_by(partner_id=partner_id)
        query.delete()

        session.commit()

    def close(self):
        self._commit_successful_cache()
        self._commit_failed_cache()

class Violation(DatabaseObject):
    __tablename__ = "violations"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey(partner_table.c.id))
    description = sqlalchemy.Column(sqlalchemyExt.String)
    # TODO: index for better performance?

import threading, os, time

class PartnerDatabase:
    Session = None
    lock = None
    samples_cache = None

    def __init__(self, database_path):
        self.lock = threading.Lock()

        engine = sqlalchemy.create_engine("sqlite:///"+database_path)

        # create partners table if it doesn't exist
        metadata.create_all(engine)

        # create tables for control samples and violations if they don't exist
        DatabaseObject.metadata.create_all(engine)

        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)

        # initialize cache for successful control samples
        timestamp = time.time()
        window_end = int(timestamp/SAMPLE_SUMMARY_INTERVAL)
        self.samples_cache = ControlSamplesCache(self.Session, window_end)

    def cleanup(self, reference_timestamp=None):
        with self.lock:
            if reference_timestamp is None:
                reference_timestamp = time.time()

            # remove expired control samples
            interval = int(reference_timestamp/SAMPLE_SUMMARY_INTERVAL)
            self.samples_cache.cleanup(interval)

        return reference_timestamp

    def get_partners(self):
        with self.lock:
            session = self.Session()
            query = session.query(Partner)
            for partner in query:
                session.expunge(partner)
                yield partner

            session.close()

    def get_partner(self, partner_name):
        with self.lock:
            session = self.Session()
            query = session.query(Partner).filter(Partner.name==partner_name)
            partner = query.scalar()
            session.close()

            return partner

    def save_partner(self, partner):
        with self.lock:
            session = self.Session()
            session.merge(partner)
            session.commit()
            session.close()

    def delete_partner(self, partner_name):
        with self.lock:
            session = self.Session()
            partner = session.query(Partner).filter_by(name=partner_name)
            partner.delete()
            session.commit()
            session.close()
            # TODO: delete control samples and violations for this partner


    def register_control_sample(self, partner_name, reference_timestamp, failed_address=None):
        with self.lock:
            # get partner id
            session = self.Session()
            query = session.query(Partner.id).filter_by(name=partner_name)
            partner_id = query.scalar()
            session.close()

            # calculate current interval
            interval = int(reference_timestamp/SAMPLE_SUMMARY_INTERVAL)

            if failed_address:
                # write the failed sample to the cache
                self.samples_cache.add_failed_sample(partner_id, interval, failed_address)

                # count failed samples
                failed_samples = self.samples_cache.count_failed_samples(partner_id, interval)

                # count successful samples
                successful_samples = self.samples_cache.count_successful_samples(partner_id, interval)

                # kick partner if necessary
                sample_count = failed_samples + successful_samples
                failed_percentage = 100.*failed_samples

                if sample_count>=SIGNIFICANCE_THRESHOLD and failed_percentage>MAX_FAILED_PERCENTAGE:
                    session = self.Session()
                    query = session.query(Partner)
                    query = query.filter_by(name=partner_name)
                    query.update({Partner.kicked: True})
                    session.commit()
                    session.close()

            else:
                # write the successful sample to the cache
                self.samples_cache.add_successful_sample(partner_id, interval)

            return True

    def register_connection(self, partner_name):
        with self.lock:
            session = self.Session()

            timestamp = int(time.time())

            query = session.query(Partner).filter_by(name=partner_name)
            query.update({Partner.last_connection: timestamp})

            session.commit()
            session.close()

            return timestamp

    def register_violation(self, partner_name, description):
        with self.lock:
            session = self.Session()

            query = session.query(Partner).filter_by(name=partner_name)
            partner = query.scalar()

            if partner is None:
                # TODO: logging
                return False

            timestamp = time.time()

            violation = Violation(partner_id=partner.id, timestamp=timestamp,
                                  description=description)
            session.add(violation)

            partner.kicked = True
            session.add(partner)

            session.commit()
            session.close()

            return True

    def close(self):
        with self.lock:
            if not self.Session: return
            self.samples_cache.close()
            self.Session.close_all()
            self.Session = None
