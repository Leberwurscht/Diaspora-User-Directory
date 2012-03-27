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
    time_interval = sqlalchemy.Column(sqlalchemy.Integer)

    #: The number of successful control samples by the specified partner in the specified time interval.
    samples = sqlalchemy.Column(sqlalchemy.Integer)

    # TODO: index for better performance

    def __init__(self, partner_id, interval):
        DatabaseObject.__init__(self)

        self.partner_id = partner_id
        self.time_interval = interval
        self.samples = 0

class FailedSample(DatabaseObject):
    """ Represents a failed control sample.
    """

    __tablename__ = "failed_samples"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    #: The ID (integer) of the :class:`Partner` for which the control sample was taken (each saved
    #: :class:`Partner` instance gets an ID from the sqlalchemy mapping).
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey(partner_table.c.id))

    #: Too old control samples must expire, therefore we need to save the timestamp (integer) of the
    #: time the control sample was taken.
    timestamp = sqlalchemy.Column(sqlalchemy.Integer)

    #: The webfinger address of the :class:`~sduds.states.Profile` for which the
    #: :class:`~sduds.context.Claim` was made. This is needed because only one failed control sample
    #: per address may be counted, otherwise a single profile owner could get a server kicked by his
    #: partners by changing his profile frequently.
    webfinger_address = sqlalchemy.Column(sqlalchemyExt.String, unique=True)

    # TODO: index for better performance

    def __init__(self, partner_id, webfinger_address, timestamp):
        DatabaseObject.__init__(self)

        self.partner_id = partner_id
        self.webfinger_address = webfinger_address
        self.timestamp = timestamp

class SuccessfulSamplesCache:
    """ If for each successful control sample the counter in the database would be updated
        immediately, there would be a lot of writes to the database which would be very
        slow. Therefore this class maintains a cache which collects control samples and
        commits them only before closing the database or if the time interval of the current
        :class:`SuccessfulSamplesSummary` lies in the past.
    """

    Session = None
    cache_dict = None

    def __init__(self, Session):
        self.Session = Session
        self.cache_dict = {}

    def add(self, partner_id, interval):
        # read cached summary or create new one
        default = interval, 0
        cached_interval, cached_samples = self.cache_dict.get(partner_id, default)

        if interval==cached_interval:
            # update cached summary if its interval is the current interval
            cached_samples += 1
        else:
            # commit cached summary and create new one if not
            self.commit(partner_id)

            cached_interval = interval
            cached_samples = 1

        # update cache
        self.cache_dict[partner_id] = cached_interval, cached_samples

    def count(self, partner_id, start_interval):
        # calculate number of non-expired successful samples from database
        session = self.Session()
        aggregator = sqlalchemy.sql.functions.sum(SuccessfulSamplesSummary.samples)
        query = session.query(aggregator)
        query = query.filter_by(partner_id=partner_id)
        query = query.filter(SuccessfulSamplesSummary.time_interval>=start_interval)
        database_samples = query.scalar()
        session.close()

        if database_samples==None:
            # no successful samples for partner_id in database
            database_samples=0

        # read number of non-expired successful samples from cache
        default = start_interval, 0
        cached_interval, cached_samples = self.cache_dict.get(partner_id, default)
        if cached_interval>=start_interval:
            cache_samples = cached_samples
        else:
            cache_samples = 0

        # calculate total number of non-expired successful samples
        total_samples = database_samples + cache_samples

        return total_samples

    def commit(self, partner_id):
        if not partner_id in self.cache_dict: return
        cached_interval, cached_samples = self.cache_dict[partner_id]

        session = self.Session()

        try:
            # get summary from database
            query = session.query(SuccessfulSamplesSummary)
            query = query.filter_by(partner_id=partner_id)
            query = query.filter_by(time_interval=cached_interval)
            summary = query.one()

        except (KeyError, sqlalchemy.orm.exc.NoResultFound):
            # if not found in database, create new summary
            summary = SuccessfulSamplesSummary(partner_id, cached_interval)

        # save summary in database
        summary.samples += cached_samples
        cached_samples = 0

        session.add(summary)
        session.commit()
        session.close()

        # update cache
        self.cache_dict[partner_id] = cached_interval, cached_samples

    def cleanup(self, start_interval):
        # clean up cache
        for partner_id in self.cache_dict.keys():
            cached_interval, cached_samples = self.cache_dict[partner_id]
            if cached_interval<start_interval:
                del self.cache_dict[partner_id]

        # clean up database
        session = self.Session()
        query = session.query(SuccessfulSamplesSummary)
        query = query.filter(SuccessfulSamplesSummary.time_interval<start_interval)
        query.delete()
        session.commit()

    def clear(self, partner_id):
        # clear cache
        if partner_id in self.cache_dict:
            del self.cache_dict[partner_id]

        # clear database
        session = self.Session()
        query = session.query(SuccessfulSamplesSummary)
        query = query.filter_by(partner_id=partner_id)
        query.delete()
        session.commit()

    def close(self):
        for partner_id in self.cache_dict.iterkeys():
            self.commit(partner_id)

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
    successful_samples_cache = None

    def __init__(self, database_path):
        self.lock = threading.Lock()

        engine = sqlalchemy.create_engine("sqlite:///"+database_path)

        # create partners table if it doesn't exist
        metadata.create_all(engine)

        # create tables for control samples and violations if they don't exist
        DatabaseObject.metadata.create_all(engine)

        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)

        # initialize cache for successful control samples
        self.successful_samples_cache = SuccessfulSamplesCache(self.Session)

    def cleanup(self, reference_timestamp=None):
        with self.lock:
            if reference_timestamp is None:
                reference_timestamp = time.time()

            start_timestamp = reference_timestamp - CONTROL_SAMPLE_LIFETIME
            start_interval = int(start_timestamp/SAMPLE_SUMMARY_INTERVAL)

            # remove expired successful control sample summaries
            self.successful_samples_cache.cleanup(start_interval)

            # remove expired failed control samples
            session = self.Session()

            reference_time = int(time.time())
            query = session.query(FailedSample)
            query = query.filter(FailedSample.timestamp<start_timestamp)
            query.delete()

            session.commit()
            session.close()

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
            session.add(partner)
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

            if failed_address:
                session = self.Session()

                # delete old failed sample with same address if necessary
                query = session.query(FailedSample)
                query = query.filter_by(partner_id=partner_id)
                query = query.filter_by(webfinger_address=failed_address)
                query.delete()
                session.flush()

                # write failed sample to database
                sample = FailedSample(partner_id, failed_address, reference_timestamp)
                session.add(sample)
                session.commit()

                # calculate start timestamp and interval
                start_timestamp = reference_timestamp - CONTROL_SAMPLE_LIFETIME
                start_interval = int(start_timestamp/SAMPLE_SUMMARY_INTERVAL)

                # count failed samples
                query = session.query(FailedSample)
                query = query.filter_by(partner_id=partner_id)
                query = query.filter(FailedSample.timestamp>=start_timestamp)
                failed_samples = query.count()

                # count successful samples
                successful_samples = self.successful_samples_cache.count(partner_id, start_interval)

                # kick partner if necessary
                sample_count = failed_samples + successful_samples
                failed_percentage = 100.*failed_samples

                if sample_count>=SIGNIFICANCE_THRESHOLD and failed_percentage>MAX_FAILED_PERCENTAGE:
                    query = session.query(Partner)
                    query = query.filter_by(name=partner_name)
                    query.update({Partner.kicked: True})
                    session.commit()

                session.close()

            else:
                # calculate current interval
                interval = int(reference_timestamp/SAMPLE_SUMMARY_INTERVAL)

                # write the successful sample to the cache
                self.successful_samples_cache.add(partner_id, interval)


            session.close()
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
            self.successful_samples_cache.close()
            self.Session.close_all()
            self.Session = None
