#!/usr/bin/env python

## Reminder: Make clear in documentation that server/client is only about who
## initiates the connection; the synchronization is always happening in both
## directions.

import logging

import os, sys, time, urllib
import hashlib, json
import socket

# initialize database
import sqlalchemy, sqlalchemy.orm, lib, sqlalchemy.sql.functions

import sqlalchemy.ext.declarative
DatabaseObject = sqlalchemy.ext.declarative.declarative_base()

###

OFFENSE_LIFETIME = 3600*24*30
OFFENSES_THRESHOLD = 0.05

###

class Partner(DatabaseObject):
    __tablename__ = 'partners'

    partner_type = sqlalchemy.Column('partner_type', lib.Text)
    __mapper_args__ = {'polymorphic_on': partner_type}

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    address = sqlalchemy.Column(lib.Text)
    control_probability = sqlalchemy.Column(sqlalchemy.Float)

    # for authenticating to the partner
    identity = sqlalchemy.Column(lib.Text)
    password = sqlalchemy.Column(lib.Text)

    # for authenticating the partner
    partner_name = sqlalchemy.Column(lib.Text, unique=True)
    passwordhash = sqlalchemy.Column(lib.Binary)

    @classmethod
    def from_database(cls, database, **kwargs):
        Session = database.Session

        try:
            partner = Session.query(cls).filter_by(**kwargs).one()
            partner.database = database
        except sqlalchemy.orm.exc.NoResultFound:
            return None
        else:
            return partner

    @classmethod
    def list_from_database(cls, database, **kwargs):
        Session = database.Session

        if kwargs:
            partners = Session.query(cls).filter_by(**kwargs).all()
        else:
            partners = Session.query(cls).all()

        for partner in partners:
            partner.database = database

        return partners

    def __init__(self, database, *args, **kwargs):
        DatabaseObject.__init__(self, *args, **kwargs)
        self.database = database

    def kicked(self):
        Session = self.database.Session

        violations = Session.query(Violation).filter_by(partner=self, guilty=True).count()
        return (violations>0)

    def delete(self):
        Session = self.database.Session

        Session.delete(self)
        Session.commit()

    def add_offense(self, offense):
        Session = self.database.Session

        # save offense
        offense.partner = self

        Session.add(offense)
        Session.commit()

        # TODO:
        # notify partner

        # get current time
        current_timestamp = int(time.time())
        timestamp_limit = current_timestamp - OFFENSE_LIFETIME

        # calculate per-address severity sum (only most severe offense per webfinger_address)
        aggregator = sqlalchemy.sql.functions.max(Offense.severity)
        query = Session.query(aggregator.label("max_severity"))
        query = query.filter(Offense.partner == self)
        query = query.filter(Offense.timestamp >= timestamp_limit)
        query = query.filter(Offense.guilty == True)
        query = query.filter(Offense.webfinger_address != None)
        query = query.group_by(Offense.webfinger_address)
        subquery = query.subquery()

        aggregator = sqlalchemy.sql.functions.sum(subquery.c.max_severity)
        query = Session.query(aggregator.label("per_address_severity_sum"))
        per_address_severity_sum = query.one().per_address_severity_sum
        if per_address_severity_sum==None: per_address_severity_sum=0

        # calculate address-independent severity sum
        aggregator = sqlalchemy.sql.functions.sum(Offense.severity)
        query = Session.query(aggregator.label("address_independent_severity_sum"))
        query = query.filter(Offense.partner == self)
        query = query.filter(Offense.timestamp >= timestamp_limit)
        query = query.filter(Offense.guilty == True)
        query = query.filter(Offense.webfinger_address == None)
        address_independent_severity_sum = query.one().address_independent_severity_sum
        if address_independent_severity_sum==None: address_independent_severity_sum=0

        # calculate total severity sum
        severity_sum = per_address_severity_sum + address_independent_severity_sum

        # calculate total number of received entries
        aggregator = sqlalchemy.sql.functions.sum(Conversation.add)
        query = Session.query(aggregator.label("received_sum"))
        query = query.filter(Conversation.partner == self)
        query = query.filter(Conversation.timestamp >= timestamp_limit)
        received_sum = query.one().received_sum
        if received_sum==None: received_sum = 0

        # if there are enough entries to make a reliable calculation and if there were to
        # many offenses, add the violation.
        if received_sum>3/OFFENSES_THRESHOLD and severity_sum>OFFENSES_THRESHOLD*received_sum:
            # set guilty=False on offenses; guiltiness is absorbed into violation.
            query = Session.query(Offense)
            query = query.filter(Offense.partner == self)
            query = query.filter(Offense.timestamp >= timestamp_limit)
            query = query.filter(Offense.guilty == True)
            query.update({Offense.guilty:False})
            Session.commit()

            # add a violation
            violation = TooManyOffensesViolation(severity_sum, received_sum)
            self.add_violation(violation)
        
    def add_violation(self, violation):
        Session = self.database.Session

        # save violation
        violation.partner = self
        
        Session.add(violation)
        Session.commit()

        # TODO:
        # notify administrator and partner

        # this will interrupt the processing of data until the
        # exception is caught.
        raise PartnerKickedError

    def log_conversation(self, add, delete):
        Session = self.database.Session

        conversation = Conversation(partner=self, add=add, delete=delete, timestamp=int(time.time()))

        Session.add(conversation)
        Session.commit()

    def synchronization_address(self):
        data = urllib.urlopen(self.address+"synchronization_address").read()
        host, control_port = json.loads(data)
        host = host.encode("utf8")

        return (host, control_port)
    
    def password_valid(self, password):
        comparehash = hashlib.sha1(password).digest()
        
        return comparehash==self.passwordhash

    def __str__(self):
        r = "%s %s (%s)" % (self.partner_type, self.partner_name, self.address)

        if self.kicked():
            r += " [K]"

        return r

class Server(Partner):
    __mapper_args__ = {'polymorphic_identity': 'server'}

class Client(Partner):
    __mapper_args__ = {'polymorphic_identity': 'client'}

###

# Conversations

class Conversation(DatabaseObject):
    __tablename__ = 'conversations'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('partners.id'))
    partner = sqlalchemy.orm.relation(Partner, primaryjoin=(partner_id==Partner.id))
    add = sqlalchemy.Column(sqlalchemy.Integer)
    delete = sqlalchemy.Column(sqlalchemy.Integer)
    timestamp = sqlalchemy.Column(sqlalchemy.Integer)

# Violations

class Violation(DatabaseObject):
    """ A partner is kicked if and only if a violation exists """
    __tablename__ = 'violations'

    violation_type = sqlalchemy.Column(lib.Text)
    __mapper_args__ = {'polymorphic_on': violation_type}

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('partners.id'))
    partner = sqlalchemy.orm.relation(Partner, primaryjoin=(partner_id==Partner.id))
    description = sqlalchemy.Column(lib.Text)
    timestamp = sqlalchemy.Column(sqlalchemy.Integer)
    guilty = sqlalchemy.Column(sqlalchemy.Boolean, default=True)
        # The administrator must set this to false to unkick the partner

    def __init__(self, description, **kwargs):
        kwargs["description"] = str(description)

        if not "timestamp" in kwargs:
            kwargs["timestamp"] = int(time.time())

        DatabaseObject.__init__(self, **kwargs)

# TODO: override constructors
class InvalidHashViolation(Violation):
    """ The transmitted entry contained a hash but it was wrong """

    __mapper_args__ = {"polymorphic_identity": "InvalidHash"}

class InvalidListViolation(Violation):
    """ The partner responded with an improper format being asked for a set of entries. """

    __mapper_args__ = {"polymorphic_identity": "InvalidList"}

class WrongEntriesViolation(Violation):
    """ The partner did not send the requested set of entries but other ones. """

    __mapper_args__ = {"polymorphic_identity": "WrongEntries"}

class InvalidCaptchaViolation(Violation):
    """ The partner sent an entry with an invalid Captcha signature """

    __mapper_args__ = {"polymorphic_identity": "InvalidCaptcha"}

class InvalidTimestampsViolation(Violation):
    """ The partner sent an entry with retrieval_timestamp < submission_timestamp """

    __mapper_args__ = {"polymorphic_identity": "InvalidTimestamps"}

class TooManyOffensesViolation(Violation):
    """ Too many offenses """

    __mapper_args__ = {"polymorphic_identity": "TooManyOffenses"}

    def __init__(self, severity_sum, received_sum, **kwargs):
        description = "Too many offenses accumulated: A severity sum of %f was reached with a total of %d received entries." % (severity_sum, received_sum)

        Violation.__init__(self, description, **kwargs)

# Offences

class Offense(DatabaseObject):
    """ An offense is added each time a partner makes a fault that will not
        immediately get it kicked. If too many offenses exist, a violation
        will be created. """
    __tablename__ = 'offenses'

    offense_type = sqlalchemy.Column(lib.Text)
    __mapper_args__ = {'polymorphic_on': offense_type}

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    webfinger_address = sqlalchemy.Column(lib.Text)
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('partners.id'))
    partner = sqlalchemy.orm.relation(Partner, primaryjoin=(partner_id==Partner.id))
    description = sqlalchemy.Column(lib.Text)
    severity = sqlalchemy.Column(sqlalchemy.Float)
    timestamp = sqlalchemy.Column(sqlalchemy.Integer)
    guilty = sqlalchemy.Column(sqlalchemy.Boolean, default=True)
        # if a violation is created, this will be set to False.

    default_severity = 0

    def __init__(self, description, **kwargs):
        kwargs["description"] = str(description)

        if not "severity" in kwargs:
            kwargs["severity"] = self.default_severity

        if not "timestamp" in kwargs:
            kwargs["timestamp"] = int(time.time())

        DatabaseObject.__init__(self, **kwargs)

class ConnectionFailedOffense(Offense):
    """ partner is down """

    __mapper_args__ = {"polymorphic_identity": "ConnectionFailed"}

    default_severity = 1

class InvalidProfileOffense(Offense):
    """ webfinger profile is invalid but the partner transmitted information """

    __mapper_args__ = {"polymorphic_identity": "InvalidProfile"}

    default_severity = 1

    def __init__(self, webfinger_address, description, **kwargs):
        kwargs["webfinger_address"] = webfinger_address

        Offense.__init__(self, description, **kwargs)

class NonConcurrenceOffense(Offense):
    """ the webfinger profile differs from the one the partner transmitted """

    __mapper_args__ = {"polymorphic_identity": "NonConcurrence"}

    default_severity = 1

    def __init__(self, fetched_entry, transmitted_entry, **kwargs):
        kwargs["webfinger_address"] = transmitted_entry.webfinger_address

        description = "A control sample for an entry did not match the one on "+transmitted_entry.webfinger_address+"\n\n"
        description += "Fetched entry:\n"
        description += "==============\n"
        description += str(fetched_entry)+"\n"
        description += "Transmitted entry:\n"
        description += "==================\n"
        description += str(transmitted_entry)

        Offense.__init__(self, description, **kwargs)

####
# custom exceptions

class PartnerKickedError(Exception):
    """ The partner got a violation """
    pass

####
# database class

class Database:
    def __init__(self, suffix="", erase=False):
        global DatabaseObject

        self.logger = logging.getLogger("partnerdb"+suffix)

        self.dbfile = "partners"+suffix+".sqlite"

        if erase and os.path.exists(self.dbfile):
            os.remove(self.dbfile)

        engine = sqlalchemy.create_engine("sqlite:///"+self.dbfile)
        self.Session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=engine))

        # create tables if they don't exist
        DatabaseObject.metadata.create_all(engine)

    def close(self, erase=False):
        if hasattr(self, "Session"): self.Session.close_all()

        if erase and os.path.exists(self.dbfile):
            os.remove(self.dbfile)

### command line interface
if __name__=="__main__":
    import optparse
    import getpass

    database = Database()

    def read_password():
        while True:
            first = getpass.getpass("Password: ")
            second = getpass.getpass("Repeat password: ")

            if first==second:
                password = first
                return password
            else:
                print >>sys.stderr, "ERROR: Passwords do not match."

    parser = optparse.OptionParser(
        usage = "%prog -a (-s|-c) ADDRESS PARTNER_NAME IDENTITY [CONTROL_PROBABILITY]\nOr: %prog -d PARTNER_NAME\nOr: %prog -l [-s|-c]",
        description="manage the synchronization partners list"
    )
    
    parser.add_option( "-s", "--server", action="store_true", dest="server", help="action deals with server")
    parser.add_option( "-c", "--client", action="store_true", dest="client", help="action deals with client")
    
    parser.add_option( "-l", "--list", action="store_true", dest="list", help="list partners")
    parser.add_option( "-a", "--add", action="store_true", dest="add", help="add or change a partner")
    parser.add_option( "-d", "--delete", action="store_true", dest="delete", help="delete a partner")

    (options, args) = parser.parse_args()

    if options.list:
        # if neither -s nor -c given, display both servers and clients
        if not options.server and not options.client:
            options.server = True
            options.client = True

        if options.server:
            for server in Server.list_from_database(database):
                print str(server)

        if options.client:
            for client in Client.list_from_database(database):
                print str(client)

    elif options.add:
        if not options.server and not options.client:
            print >>sys.stderr, "ERROR: Need either -s or -c."
            sys.exit(1)

        try:
            address,partner_name,identity = args[:3]
        except ValueError:
            print >>sys.stderr, "ERROR: Need address, partner name and identity"
            sys.exit(1)

        control_probability = 0.1

        if len(args)>5:
            print >>sys.stderr, "ERROR: Too many arguments."
            sys.exit(1)
        elif len(args)==5:
            try:
                # might raise ValueError if string is invalid
                control_probability = float(args[4])

                # if number is not in range, raise ValueError ourselves
                if control_probability<0 or control_probability>1:
                    raise ValueError
            except ValueError:
                print >>sys.stderr, "ERROR: Invalid probability."
                sys.exit(1)


        print "Type the password the partner uses to authenticate to us (password for %s)" % partner_name
        password = read_password()
        passwordhash = hashlib.sha1(password).digest()

        print "Type the password we use to authenticate to the partner (password for %s)" % identity
        password = read_password()

        old_partner = Partner.from_database(database, partner_name=partner_name)
        if old_partner:
            # TODO: this will invalidate all the violations and offenses
            old_partner.delete()

        kwargs = {
            "address": address,
            "control_probability": control_probability,
            "identity": identity,
            "password": password,
            "partner_name": partner_name,
            "passwordhash": passwordhash
        }

        if options.server:
            print "Adding server \"%s\"." % address

            partner = Server(database, **kwargs)
        elif options.client:
            print "Adding client \"%s\"." % address

            partner = Client(database, **kwargs)

        database.Session.add(partner)
        database.Session.commit()

    elif options.delete:
        try:
            partner_name, = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need partner name."
            sys.exit(1)

        partner = Partner.from_database(database, partner_name=partner_name)

        if not partner:
            print >>sys.stderr, "ERROR: Partner \"%s\" does not exists." % partner_name
            sys.exit(1)

        print "Deleting partner \"%s\"." % partner_name

        partner.delete()

    else:
        parser.print_help()
