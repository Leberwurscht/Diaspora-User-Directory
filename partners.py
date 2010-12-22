#!/usr/bin/env python

## Reminder: Make clear in documentation that server/client is only about who
## initiates the connection; the synchronisation is always happening in both
## directions.

import logging

import sys, time
import hashlib
import socket

# initialize database
import sqlalchemy, sqlalchemy.orm, lib

engine = sqlalchemy.create_engine('sqlite:///partners.sqlite')
Session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=engine))

import sqlalchemy.ext.declarative
DatabaseObject = sqlalchemy.ext.declarative.declarative_base()

###

class Partner(DatabaseObject):
    __tablename__ = 'partners'

    partner_type = sqlalchemy.Column('partner_type', lib.String)
    __mapper_args__ = {'polymorphic_on': partner_type}

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    host = sqlalchemy.Column(lib.String)
    entryserver_port = sqlalchemy.Column(sqlalchemy.Integer)
    control_probability = sqlalchemy.Column(sqlalchemy.Float)

    @classmethod
    def from_database(cls, **kwargs):
        global Session

        try:
            partner = Session.query(cls).filter_by(**kwargs).one()
        except sqlalchemy.orm.exc.NoResultFound:
            return None
        else:
            return partner

    def kicked(self):
        global Session

        violations = Session.query(Violation).filter_by(partner=self, guilty=True).count()
        return (violations>0)

    def delete(self):
        global Session

        Session.delete(self)
        Session.commit()

    def kick(self, reason):
        raise NotImplementedError, "Not implemented yet."

    def log_conversation(self, received):
        global Session

        conversation = Conversation(partner=self, received=received, timestamp=int(time.time()))

        Session.add(conversation)
        Session.commit()

class Server(Partner):
    __tablename__ = 'servers'
    __mapper_args__ = {'polymorphic_identity': 'server'}
    
    id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('partners.id'), primary_key=True)
    synchronisation_port = sqlalchemy.Column(sqlalchemy.Integer)
    username = sqlalchemy.Column(lib.String)
    password = sqlalchemy.Column(lib.String)

    def authenticated_socket(self):
        address = (self.host, self.synchronisation_port)
        asocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        asocket.connect(address)

        asocket.sendall(self.username+"\n")
        asocket.sendall(self.password+"\n")

        f = asocket.makefile()
        answer = f.readline().strip()
        f.close()

        if answer=="OK":
            logging.info("Successfully authenticated to server %s." % str(address))
            return asocket
        else:
            logging.error("Authentication to server %s failed." % str(address))
            asocket.close()
            return False

    def __str__(self):
        r = self.username+"@"+self.host+":"+str(self.synchronisation_port)

        if self.kicked():
            r += " [K]"

        return r

class Client(Partner):
    __tablename__ = 'clients'
    __mapper_args__ = {'polymorphic_identity': 'client'}
    
    id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('partners.id'), primary_key=True)
    username = sqlalchemy.Column(lib.String)
    passwordhash = sqlalchemy.Column(lib.String)

    def password_valid(self, password):
        comparehash = hashlib.sha1(password).digest()
        
        return comparehash==self.passwordhash

    def __str__(self):
        r = self.host+" (using username "+self.username+")"

        if self.kicked():
            r += " [K]"

        return r

###

# Conversations

class Conversation(DatabaseObject):
    __tablename__ = 'conversations'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('partners.id'))
    partner = sqlalchemy.orm.relation(Partner, primaryjoin=(partner_id==Partner.id))
    received = sqlalchemy.Column(sqlalchemy.Integer)
    timestamp = sqlalchemy.Column(sqlalchemy.Integer)

# Violations

class Violation(DatabaseObject):
    """ A partner is kicked if and only if a violation exists """
    __tablename__ = 'violations'

    violation_type = sqlalchemy.Column(lib.String)
    __mapper_args__ = {'polymorphic_on': violation_type}

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('partners.id'))
    partner = sqlalchemy.orm.relation(Partner, primaryjoin=(partner_id==Partner.id))
    description = sqlalchemy.Column(lib.String)
    timestamp = sqlalchemy.Column(sqlalchemy.Integer)
    guilty = sqlalchemy.Column(sqlalchemy.Boolean, default=True)
        # The administrator must set this to false to unkick the partner

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
        kwargs["description"] = "Too many offenses accumulated: A severity sum of %f was reached with a total of %d received entries." % (severity_sum, received_sum)

        Violation.__init__(self, **kwargs)

# Offences

class Offense(DatabaseObject):
    """ An offense is added each time a partner makes a fault that will not
        immediately get it kicked. If too many offenses exist, a violation
        will be created. """
    __tablename__ = 'offenses'

    offense_type = sqlalchemy.Column(lib.String)
    __mapper_args__ = {'polymorphic_on': offense_type}

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    partner_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('partners.id'))
    partner = sqlalchemy.orm.relation(Partner, primaryjoin=(partner_id==Partner.id))
    description = sqlalchemy.Column(lib.String)
    severity = sqlalchemy.Column(sqlalchemy.Float)
    timestamp = sqlalchemy.Column(sqlalchemy.Integer)
    guilty = sqlalchemy.Column(sqlalchemy.Boolean, default=True)
        # if a violation is created, this will be set to False.

    default_severity = 0

    def __init__(self, description, **kwargs):
        kwargs["description"] = description

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

class NonConcurrenceOffense(Offense):
    """ the webfinger profile differs from the one the partner transmitted """

    __mapper_args__ = {"polymorphic_identity": "NonConcurrence"}

    default_severity = 1

    def __init__(self, fetched_entry, transmitted_entry, **kwargs):
        description = "A control sample for an entry did not match the one on "+transmitted_entry.webfinger_address+"\n\n"
        description += "Fetched entry:\n"
        description += "==============\n"
        description += str(fetched_entry)+"\n"
        description += "Transmitted entry:\n"
        description += "==================\n"
        description += str(transmitted_entry)

        Offense.__init__(self, description, **kwargs)

###
# create tables if they don't exist
DatabaseObject.metadata.create_all(engine)

### command line interface
if __name__=="__main__":
    import optparse
    import getpass

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
        usage = "%prog -a -s HOST PORT ENTRYSERVERPORT USERNAME [CONTROL_PROBABILITY]\nOr: %prog -d -s HOST PORT\nOr: %prog -a -c USERNAME HOST ENTRYSERVERPORT [CONTROL_PROBABILITY]\nOr: %prog -d -c USERNAME\nOr: %prog -l [-s|-c]",
        description="manage the synchronisation partners list"
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
            print "Servers:"
            for server in Session.query(Server).all():
                print str(server)
            print

        if options.client:
            print "Clients:"
            for client in Session.query(Client).all():
                print str(client)
            print

    elif options.add and options.server:
        try:
            host,synchronisation_port,entryserver_port,username = args[:4]
        except ValueError:
            print >>sys.stderr, "ERROR: Need host, port, EntryServer port and username."
            sys.exit(1)
            
        try:
            synchronisation_port = int(synchronisation_port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid port."
            sys.exit(1)
            
        try:
            entryserver_port = int(entryserver_port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid EntryServer port."
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

        print "Adding server \"%s\"." % host

        password = read_password()

        # delete old entry
        old_server = Server.from_database(host=host, synchronisation_port=synchronisation_port)
        if old_server:
            old_server.delete()

        server = Server(
            host=host,
            username=username,
            password=password,
            synchronisation_port=synchronisation_port,
            entryserver_port=entryserver_port,
            control_probability=control_probability
        )

        Session.add(server)
        Session.commit()

    elif options.add and options.client:
        try:
            username,host,entryserver_port = args[:3]
        except ValueError:
            print >>sys.stderr, "ERROR: Need username, host and EntryServer port."
            sys.exit(1)

        try:
            entryserver_port = int(entryserver_port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid EntryServer port."
            sys.exit(1)

        control_probability = 0.1

        if len(args)>4:
            print >>sys.stderr, "ERROR: Too many arguments."
            sys.exit(1)
        elif len(args)==4:
            try:
                # might raise ValueError if string is invalid
                control_probability = float(args[3])

                # if number is not in range, raise ValueError ourselves
                if control_probability<0 or control_probability>1:
                    raise ValueError
            except ValueError:
                print >>sys.stderr, "ERROR: Invalid probability."
                sys.exit(1)

        print "Adding client \"%s\"." % username

        password = read_password()
        passwordhash = hashlib.sha1(password).digest()

        # delete old entry
        old_client = Client.from_database(username=username)
        if old_client:
            old_client.delete()

        client = Client(
            host=host,
            entryserver_port=entryserver_port,
            username=username,
            passwordhash=passwordhash,
            control_probability=control_probability
        )

        Session.add(client)
        Session.commit()

    elif options.add:
        print >>sys.stderr, "ERROR: Need either -s or -c."
        sys.exit(1)

    elif options.delete and options.server:
        try:
            host,synchronisation_port = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need host and port."
            sys.exit(1)
            
        try:
            synchronisation_port = int(synchronisation_port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid port."
            sys.exit(1)

        server = Server.from_database(host=host, synchronisation_port=synchronisation_port)

        if not server:
            print >>sys.stderr, "ERROR: Server \"%s:%d\" is not in list." % (host, synchronisation_port)
            sys.exit(1)

        print "Deleting server \"%s:%d\"." % (host, synchronisation_port)

        server.delete()

    elif options.delete and options.client:
        try:
            username, = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need username."
            sys.exit(1)

        client = Client.from_database(username=username)

        if not client:
            print >>sys.stderr, "ERROR: Client username \"%s\" is not in list." % username
            sys.exit(1)

        print "Deleting client username \"%s\"." % username

        client.delete()

    elif options.delete:
        print >>sys.stderr, "ERROR: Need either -s or -c."
        sys.exit(1)

    else:
        parser.print_help()
