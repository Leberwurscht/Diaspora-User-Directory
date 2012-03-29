#!/usr/bin/env python

"""
This module provides the functionality to manage the synchronization partners.

It implements a :class:`Partner` class representing a synchronization partner,
and a :class:`PartnerDatabase` class which saves Partner instances in a sqlite
database using a sqlalchemy mapping.

To be able to kick unreliable synchronization partners, PartnerDatabase provides
methods to register :meth:`control samples <PartnerDatabase.register_control_sample>`
and :meth:`malformed states <PartnerDatabase.register_malformed_state>`.
"""

import random
import urllib, json

from constants import *

class Partner(object):
    """ This class represents a synchronization partner. Instances can be stored in the :class:`PartnerDatabase`. """

    #: The name this partner uses to authenticate to us (string).
    #: Must be shorter than 256 bytes.
    name = None
    #: The password this partner uses to authenticate to us (string).
    accept_password = None

    #: The URL at which the partner provides its web interface (string). Must end with '``/``'.
    base_url = None
    #: The fraction of :class:`Claims <sduds.context.Claim>` from this partner that should be checked (float in the range 0.0--1.0).
    control_probability = None
    #: The timestamp of the last attempt to connect to this partner (integer). May be ``None`` if no connection was made yet.
    last_connection = None
    #: Whether :class:`Claims <sduds.context.Claim>` of this partner should be rejected (boolean).
    #: This is set to ``True`` if too many control samples fail or the partner transmits malformed :class:`States <sduds.states.State>`.
    kicked = None

    #: The schedule to connect to the partner, in cron-like syntax (the five arguments of the
    #: :meth:`CronPattern constructor <sduds.lib.scheduler.CronPattern.__init__>` constructor separated by whitespace).
    #: May be ``None`` if no connections should be initiated.
    connection_schedule = None
    #: The name used to authenticate to the partner (string, shorter than 256 bytes).
    #: May be ``None`` if no connections should be initiated.
    provide_username = None
    #: The password used to authenticate to the partner (string).
    #: May be ``None`` if no connections should be initiated.
    provide_password = None

    def __init__(self, name, accept_password, base_url, control_probability, connection_schedule=None, provide_username=None, provide_password=None):
        """ For a description of the arguments see the documentation of the attributes of this class. """

        self.name = name
        self.accept_password = accept_password
        self.base_url = base_url
        self.control_probability = control_probability
        self.connection_schedule = connection_schedule
        self.provide_username = provide_username
        self.provide_password = provide_password
        self.kicked = False

    def get_synchronization_address(self):
        """ Gets the host and port on which the partner can be contacted for synchronization by
            retrieving ``http://partner_base_url/synchronization_address``.
            This site should return a json document of the following form::

                ["www.example.org", 20000]


            :rtype: (string, integer)-tuple
        """

        assert self.base_url.endswith("/")
        address_url = self.base_url+"synchronization_address"

        data = urllib.urlopen(address_url).read()
        host, control_port = json.loads(data)
        host = host.encode("utf8")

        assert type(host)==str
        assert type(control_port)==int

        return host, control_port

    def control_sample(self):
        """ Used to determine whether a control sample should be taken or not.
            Returns ``True`` with probability :attr:`control_probability` using a random number generator.

            :rtype: boolean
        """

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
    sqlalchemy.Column("name", sqlalchemyExt.String, index=True, unique=True),
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
    """ Represents a summary of all successful control samples by a certain partner in a certain
        interval.
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
        """ For a description of the arguments see the documentation of the attributes of this class. """

        DatabaseObject.__init__(self)

        self.partner_id = partner_id
        self.interval = interval
        self.samples = 0

class FailedSample(DatabaseObject):
    """ Represents one failed control sample. """

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
    #: per address may be counted, otherwise a single profile owner could get a server kicked from his
    #: partners by changing his profile frequently.
    webfinger_address = sqlalchemy.Column(sqlalchemyExt.String, unique=True)

    # TODO: index for better performance

    def __init__(self, partner_id, interval, webfinger_address):
        """ For a description of the arguments see the documentation of the attributes of this class. """

        DatabaseObject.__init__(self)

        self.partner_id = partner_id
        self.interval = interval
        self.webfinger_address = webfinger_address

class ControlSampleCache:
    """ This class is used by the :meth:`PartnerDatabase.register_control_sample` method to save the
        control samples in an efficient manner. As control samples are registered very often, it would
        slow down the application if they would be written to the database immediately each time.
        Therefore the newly added control samples are cached.

        For caching, the time scale is divided into equally large intervals of SAMPLE_SUMMARY_SIZE seconds,
        starting at unix timestamp ``0``.
        Control samples expire after a certain time, so only samples from a certain time window must be
        considered. This time window ends at the present interval and has a length of CONTROL_SAMPLE_WINDOW
        intervals. As long as the current time does not exceed the border of an interval, this window stays
        at the same position, and no database write is performed except the cache grows too big.
        If current time does exceed a border, the window moves forward, and all control samples are commited
        to the database.
    """

    Session = None

    window_end = None
    successful_cache = None
    successful_stored_count = None
    failed_cache = None
    failed_update_count = None
    failed_stored_count = None

    max_cache_size_failed = None

    def __init__(self, Session, window_end, max_cache_size_failed=500):
        """ :param Session: the sqlalchemy :class:`Session`
            :param window_end: the initial end of the window (interval number)
            :type window_end: integer
            :param max_cache_size_failed: the maximal number of failed control samples that should be cached
                                          before the cache is commited to the database (optional)
            :type max_cache_size_failed: integer
        """

        self.Session = Session
        self.max_cache_size_failed = max_cache_size_failed

        self._reinitialize_cache(window_end)

    def _commit_successful_cache(self):
        session = self.Session()

        for partner_id in self.successful_cache:
            # get stored summary, if present
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
        """ Saves a successful control sample by increasing an interval counter. If the window
            is moved, this counter is used to update the corresponding :class:`SuccessfulSamplesSummary`
            in the database. This method moves the window automatically if necessary.

            :param partner_id: the id of the :class:`Partner` from the database mapping
            :type partner_id: integer
            :param interval: the interval number corresponding to the time the control sample was taken
            :type interval: integer
        """

        assert interval>=self.window_end, "interval must be monotonously increasing"

        # move window forward if necessary
        if not interval==self.window_end:
            self._move_forward_to(interval)

        # add sample to cache
        self.successful_cache.setdefault(partner_id, 0)
        self.successful_cache[partner_id] += 1

    def count_successful_samples(self, partner_id, interval):
        """ Returns the number of successful control samples in the window ending
            at the specified interval.

            :param partner_id: the id of the :class:`Partner` from the database mapping
            :type partner_id: integer
            :param interval: the interval number
            :type interval: integer
            :rtype: integer
        """

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
        """ Saves a failed control sample to an internal cache using the :class:`FailedSample` class.
            If the window is moved, this cache is commited to the database. This method moves the window
            automatically if necessary.

            :param partner_id: the id of the :class:`Partner` from the database mapping
            :type partner_id: integer
            :param interval: the interval number corresponding to the time the control sample was taken
            :type interval: integer
            :param webfinger_address: the address of profile for which the control sample failed
            :type webfinger_address: string
        """

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
        """ Returns the number of failed control samples in the window ending
            at the specified interval.

            :param partner_id: the id of the :class:`Partner` from the database mapping
            :type partner_id: integer
            :param interval: the interval number
            :type interval: integer
            :rtype: integer
        """

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
        """ Deletes all control samples which are too old too reside in the window ending
            at the specified interval.

            :param interval: the interval number
            :type interval: integer
        """

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
        """ Remove all cached and stored control samples for a given :class:`Partner`.
            Used if a partner is deleted.

            :param partner_id: the id of the :class:`Partner` from the database mapping
            :type partner_id: integer
        """

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
        """ Commits the cache to the database. May not be called more than one time.
        """

        self._commit_successful_cache()
        self._commit_failed_cache()

        self.window_end = None

import threading, os, time

class PartnerDatabase:
    """ This class is used to save :class:`Partner`, :class:`ControlSample` and :class:`Violation` instances
        to a sqlite database.
    """

    Session = None
    lock = None
    samples_cache = None

    def __init__(self, database_path):
        """ :param database_path: The path to the sqlite database file. Will be
                                  created automatically if it doesn't exist.
            :type database_path: string
        """

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
        self.samples_cache = ControlSampleCache(self.Session, window_end)

    def cleanup(self, reference_timestamp=None):
        """ Deletes expired control samples. Returns the current timestamp,
            so that this method can be used directly as a callback for the
            :class:`~sduds.lib.scheduler.Job` scheduler.

            :rtype: integer
        """

        with self.lock:
            if reference_timestamp is None:
                reference_timestamp = time.time()

            # remove expired control samples
            interval = int(reference_timestamp/SAMPLE_SUMMARY_INTERVAL)
            self.samples_cache.cleanup(interval)

        return reference_timestamp

    def get_partners(self):
        """ This method lists all synchronization partners.

            :rtype: python ``generator`` of :class:`Partner` instances
        """

        with self.lock:
            session = self.Session()
            query = session.query(Partner)
            for partner in query:
                session.expunge(partner)
                yield partner

            session.close()

    def get_partner(self, partner_name):
        """ Gets the corresponding :class:`Partner` instance to a given
            :attr:`Partner.name` from the database and returns it.
            Returns ``None`` if no such instance exists.

            :param partner_name: the name of the partner
            :type partner_name: string
            :rtype: :class:`Partner` or NoneType
        """

        with self.lock:
            session = self.Session()
            query = session.query(Partner).filter(Partner.name==partner_name)
            partner = query.scalar()
            session.close()

            return partner

    def save_partner(self, partner):
        """ Saves a :class:`Partner` instance to the database.

            :param partner: the :class:`Partner` instance
        """

        with self.lock:
            session = self.Session()
            session.merge(partner)
            session.commit()
            session.close()

    def delete_partner(self, partner_name):
        """ Deletes the :class:`Partner` with the given :attr:`~Partner.name` from
            the database. Does nothing if specified partner does not exist.

            :param partner_name: the name of the partner which should be deleted
            :type partner_name: string
        """

        with self.lock:
            session = self.Session()

            # fetch partner
            partner = session.query(Partner).filter_by(name=partner_name).scalar()
            if partner is None: return

            # delete successful control samples
            self.samples_cache.clear(partner.id)

            # delete partner
            session.delete(partner)

            session.commit()
            session.close()

    def register_control_sample(self, partner_name, reference_timestamp, failed_address=None):
        """ Saves a control sample using the :class:`ControlSampleCache` class. Returns ``False``
            if the specified partner was not found.

            :param partner_name: the name of the partner which should be deleted
            :type partner_name: string
            :param reference_timestamp: The timestamp for which the sample should be registered
            :type reference_timestamp: integer
            :param failed_address: if the control sample failed, the webfinger address of the profile, otherwise ``None``
            :type failed_address: string or NoneType

            :rtype: boolean
        """

        with self.lock:
            # get partner id
            session = self.Session()
            query = session.query(Partner.id).filter_by(name=partner_name)
            partner_id = query.scalar()
            session.close()

            # return False if partner name was not found
            if partner_id is None:
                return False

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
        """ When a connection to a synchronization partner is initiated, this
            method is called. It saves the current time to the database
            (:attr:`Partner.last_connection`), so that the :class:`~sduds.lib.scheduler.Job`
            scheduler knows when to synchronize next even when the application
            is restarted.  If no :class:`Partner` for the given name is in the
            database, this method won't throw an exception or indicate this in
            the return value.

            Returns the timestamp that was saved.

            :param partner_name: the :attr:`~Partner.name` of the partner
            :type partner_name: string
            :rtype: integer
        """

        with self.lock:
            session = self.Session()

            timestamp = int(time.time())

            query = session.query(Partner).filter_by(name=partner_name)
            query.update({Partner.last_connection: timestamp})

            session.commit()
            session.close()

            return timestamp

    def register_malformed_state(self, partner_name):
        """ This method is called when a synchronization partner transmits a malformed
            :class:`~sduds.states.State`. It kicks the partner by setting the
            :attr:`~Partner.kicked` attribute to ``True``. Does nothing if partner name
            is invalid.

            :param partner_name: the :attr:`~Partner.name` of the partner which transmitted the malformed state
            :type partner_name: string
        """

        with self.lock:
            session = self.Session()
            query = session.query(Partner)
            query = query.filter_by(name=partner_name)
            query.update({Partner.kicked: True})
            session.commit()
            session.close()

    def close(self):
        """ Closes the database. Must not be called more than one time. """

        with self.lock:
            self.samples_cache.close()
            self.Session.close_all()
            self.Session = None
