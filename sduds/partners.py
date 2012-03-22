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

# ControlSample and Violation classes with mapping
DatabaseObject = sqlalchemy.ext.declarative.declarative_base()

class ControlSample(DatabaseObject):
    __tablename__ = "control_samples"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey(partner_table.c.id))
    timestamp = sqlalchemy.Column(sqlalchemy.Integer)
    penalty = sqlalchemy.Column(sqlalchemy.Integer)
    offense = sqlalchemy.Column(sqlalchemyExt.String)
    webfinger_address = sqlalchemy.Column(sqlalchemyExt.String)
    # TODO: index for better performance

class Violation(DatabaseObject):
    __tablename__ = "violations"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey(partner_table.c.id))
    description = sqlalchemy.Column(sqlalchemyExt.String)
    # TODO: index for better performance?

import threading, os, time

class PartnerDatabase:
    database_path = None # for erasing when closing
    Session = None
    lock = None

    def __init__(self, database_path, erase=False):
        self.database_path = database_path
        self.lock = threading.Lock()

        if erase and os.path.exists(database_path):
            os.remove(database_path)

        engine = sqlalchemy.create_engine("sqlite:///"+database_path)

        # create partners table if it doesn't exist
        metadata.create_all(engine)

        # create tables for control samples and violations if they don't exist
        DatabaseObject.metadata.create_all(engine)

        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)

    def cleanup(self):
        with self.lock:
            session = self.Session()

            reference_time = time.time()
            age = reference_time - ControlSample.timestamp
            outdated = session.query(ControlSample).filter(age > CONTROL_SAMPLE_LIFETIME)
            outdated.delete()

            session.commit()
            session.close()

        return reference_time

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

    def register_control_sample(self, partner_name, webfinger_address=None, offense=None):
        with self.lock:
            session = self.Session()

            query = session.query(Partner.id).filter(Partner.name==partner_name)
            partner_id = query.scalar()

            if partner_id is None:
                # TODO: logging
                return False

            timestamp = time.time()

            if offense is None:
                penalty = 0

                # do not save address to save space
                webfinger_address = None
            else:
                penalty = 1

            control_sample = ControlSample(partner_id=partner_id,
                                           timestamp=timestamp,
                                           penalty=penalty,
                                           webfinger_address=webfinger_address,
                                           offense=offense)

            session.add(control_sample)
            session.commit()

            ## recalculate if partner must be kicked ##

            # create subquery which takes only worst penalty per webfinger address
            reference_timestamp = time.time()
            age = reference_timestamp-ControlSample.timestamp

            aggregator = sqlalchemy.sql.functions.max(ControlSample.penalty)
            subquery = session.query(aggregator.label("worst_penalty"))
            subquery = subquery.filter(not ControlSample.penalty==None)
            subquery = subquery.filter(age < CONTROL_SAMPLE_LIFETIME)
            subquery = subquery.group_by(ControlSample.webfinger_address)

            # calculate average worst penalty
            sum_aggregator = sqlalchemy.sql.functions.sum(subquery.c.worst_penalty)
            count_aggregator = sqlalchemy.sql.functions.count(subquery.c.worst_penalty)

            query = session.query(sum_aggregator, count_aggregator)
            penalty_sum, sample_count = query.one()
            offense_percentage = 100. * penalty_sum/sample_count

            # kick partner if necessary
            if sample_count>SIGNIFICANCE_THRESHOLD and offense_percentage>MAX_OFFENSE_PERCENTAGE:
                query = session.query(Partner)
                query = query.filter_by(partner_name=partner_name)
                query.update({Partner.kicked: True})

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

    def close(self, erase=False):
        with self.lock:
            if self.Session is not None:
                self.Session.close_all()
                self.Session = None

            if erase and os.path.exists(self.database_path):
                os.remove(self.database_path)
