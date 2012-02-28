#!/usr/bin/env python

import logging

import socket

import time
import binascii, hashlib

import urllib
import json

import os

RESUBMISSION_INTERVAL = 3600*24*3
ENTRY_LIFETIME = 3600*24*365

# load public key for verifying captcha signatures

import base64, paramiko

f = open("captchakey.pub")
content = f.read()
f.close()

keytype, key = content.split()[:2]

if not keytype=="ssh-rsa":
    raise Exception, "Need keytype ssh-rsa."

data=base64.decodestring(key)
captcha_key = paramiko.RSAKey(data=data)

def signature_valid(public_key, signature, text):
    sig_message = paramiko.Message()
    sig_message.add_string("ssh-rsa")
    sig_message.add_string(signature)
    sig_message.rewind()

    return public_key.verify_ssh_sig(text, sig_message)

# initialize database
import sqlalchemy, sqlalchemy.orm, lib

import sqlalchemy.ext.declarative
DatabaseObject = sqlalchemy.ext.declarative.declarative_base()

######

class InvalidHashError(Exception): pass
""" The partner included a hash field into its JSON string but it was wrong """

class InvalidListError(Exception): pass
""" EntryList transmitted as JSON was invalid """

class WrongEntriesError(Exception): pass
""" Requested entries were missing or entries not requested were transmitted """

class EntryList(list):
    @classmethod
    def from_database(cls, database, binhashes):
        session = database.Session()

        entrylist = EntryList()

        for binhash in binhashes:
            try:
                entry = session.query(Entry).filter_by(hash=binhash).one()
            except sqlalchemy.orm.exc.NoResultFound:
                database.logger.warning("Requested hash \"%s\" does not exist in database." % binascii.hexlify(binhash))
            else:
                entrylist.append(entry)

        return entrylist

    @classmethod
    def from_server(cls, binhashes, address):
        data = []
        for binhash in binhashes:
            data.append(("hexhash", binascii.hexlify(binhash)))

        data = urllib.urlencode(data)

        json_string = urllib.urlopen(address+"entrylist", data).read()

        try:
            entrylist = cls.from_json(json_string)
        except Exception, error:
            raise InvalidListError(error)

        # check if the received entries are the requested ones
        if not set(entrylist.hashes())==set(binhashes):
            raise WrongEntriesError(entrylist.hashes(), binhashes)

        return entrylist

    @classmethod
    def from_json(cls, json_string):
        entrylist = cls()

        for json_entry in json.loads(json_string):
            entry = Entry(
                json_entry["webfinger_address"].encode("latin-1"),
                full_name=json_entry["full_name"],
                hometown=json_entry["hometown"],
                country_code=json_entry["country_code"].encode("latin-1"),
                services=json_entry["services"].encode("latin-1"),
                captcha_signature=binascii.unhexlify(json_entry["captcha_signature"]),
                submission_timestamp=json_entry["submission_timestamp"],
                retrieval_timestamp=json_entry["retrieval_timestamp"]
            )

            if "hash" in json_entry:
                if not binascii.unhexlify(json_entry["hash"])==entry.hash:
                    raise InvalidHashError(json_string, json_entry["hash"], entry.hash)

            entrylist.append(entry)

        return entrylist

    def hashes(self):
        binhashes = [entry.hash for entry in self]
        return binhashes

    def json(self):
        json_list = []

        for entry in self:
            json_list.append({
                "hash": unicode(binascii.hexlify(entry.hash), "latin-1"),
                "webfinger_address": unicode(entry.webfinger_address, "latin-1"),
                "full_name": entry.full_name,
                "hometown": entry.hometown,
                "country_code": unicode(entry.country_code, "latin-1"),
                "services": unicode(entry.services, "latin-1"),
                "captcha_signature": unicode(binascii.hexlify(entry.captcha_signature), "latin-1"),
                "submission_timestamp": entry.submission_timestamp,
                "retrieval_timestamp": entry.retrieval_timestamp
            })

        json_string = json.dumps(json_list)

        return json_string

    def save(self, database):
        session = database.Session()

        added_hashes = set()
        deleted_hashes = set()
        ignored_hashes = set()

        for entry in self:
            try:
                existing_entry = session.query(Entry).filter_by(webfinger_address=entry.webfinger_address).one()
                session.expunge(existing_entry)

                if existing_entry.submission_timestamp>entry.submission_timestamp:
                    # ignore entry if a more recent one exists already
                    ignored_hashes.add(existing_entry.hash)
                    continue
                else:
                    # delete older entries
                    deleted_hashes.add(existing_entry.hash)
                    database.delete_entry(entry.retrieval_timestamp, hash=existing_entry.hash)

            except sqlalchemy.orm.exc.NoResultFound:
                # entry is new
                pass

            # add entry to database
            session.add(entry)
            session.commit()

            added_hashes.add(entry.hash)

        return added_hashes, deleted_hashes, ignored_hashes

# https://github.com/jcarbaugh/python-webfinger
import pywebfinger

import urllib

class Entry(DatabaseObject):
    """ represents a database entry for a webfinger address """

    __tablename__ = 'entries'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    hash = sqlalchemy.Column(lib.Binary, index=True, unique=True)
    webfinger_address = sqlalchemy.Column(lib.Text, index=True, unique=True)
    full_name = sqlalchemy.Column(sqlalchemy.UnicodeText)
    hometown = sqlalchemy.Column(sqlalchemy.UnicodeText)
    country_code = sqlalchemy.Column(lib.Text(2))
    services = sqlalchemy.Column(lib.Text)
    captcha_signature = sqlalchemy.Column(lib.Binary)
    submission_timestamp = sqlalchemy.Column(sqlalchemy.Integer)
    retrieval_timestamp = sqlalchemy.Column(sqlalchemy.Integer)

    @classmethod
    def from_database(cls, database, **kwargs):
        session = database.Session()

        try:
            entry = session.query(Entry).filter_by(**kwargs).one()
        except sqlalchemy.orm.exc.NoResultFound:
            entry = None

        return entry

    @classmethod
    def from_webfinger_address(cls, webfinger_address):

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
        retrieval_timestamp = int(time.time())

        entry = cls(
            webfinger_address,
            full_name=full_name,
            hometown=hometown,
            country_code=country_code,
            services=services,
            captcha_signature=captcha_signature,
            submission_timestamp=submission_timestamp,
            retrieval_timestamp=retrieval_timestamp
        )

        return entry

    def __init__(self, webfinger_address, **kwargs):
        kwargs["webfinger_address"] = webfinger_address
        DatabaseObject.__init__(self, **kwargs)
        self.update_hash()

    def update_hash(self):
        combinedhash = hashlib.sha1()

        for data in [self.webfinger_address, self.full_name, self.hometown, self.country_code, self.services, self.submission_timestamp]:
            subhash = hashlib.sha1(str(data)).digest()
            combinedhash.update(subhash)

        # is it unsecure to take only 16 bytes of the hash?
        binhash = combinedhash.digest()[:16]

        self.hash = binhash

    def captcha_signature_valid(self):
        global captcha_key

        return signature_valid(captcha_key, self.captcha_signature, self.webfinger_address.encode("utf-8"))

    def expired(self, now=time.time()):
        return self.submission_timestamp < now - ENTRY_LIFETIME

    def __str__(self):
        """ for debbuging and log messages """
        r = "Hash: "+binascii.hexlify(self.hash)+"\n"
        r += "Address: "+self.webfinger_address+"\n"
        r += "Full name: "+self.full_name.encode("utf-8")+"\n"
        r += "Hometown: "+self.hometown.encode("utf-8")+"\n"
        r += "Country code: "+self.country_code+"\n"
        r += "Services: "+self.services+"\n"
        r += "Captcha signature: "+binascii.hexlify(self.captcha_signature)[:20]+"...\n"
        r += "Submission time: "+time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime(self.submission_timestamp))+"\n"
        r += "Retrieval time: "+time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime(self.retrieval_timestamp))+"\n"

        return r

class DeletedEntry(DatabaseObject):
    __tablename__ = 'deleted_entries'

    hash = sqlalchemy.Column(lib.Binary, primary_key=True)
    retrieval_timestamp = sqlalchemy.Column(sqlalchemy.Integer)

class Variable(DatabaseObject):
    __tablename__ = "variables"

    key = sqlalchemy.Column(lib.Text, primary_key=True)
    value = sqlalchemy.Column(lib.Text)

####
# database class

class Database:
    def __init__(self, suffix="", erase=False):
        global DatabaseObject

        self.suffix = suffix
        self.logger = logging.getLogger("entrydb"+suffix)

        self.dbfile = "entries"+suffix+".sqlite"

        if erase and os.path.exists(self.dbfile):
            os.remove(self.dbfile)

        engine = sqlalchemy.create_engine("sqlite:///"+self.dbfile)
        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)

        # create tables if they don't exist
        DatabaseObject.metadata.create_all(engine)

        # make sure cleanup_schedule is set
        if not self.get_variable("cleanup_schedule"):
            # NOTE: must be frequent enough for ENTRY_LIFETIME
            self.set_variable("cleanup_schedule", "0 0 * * *")

    def get_variable(self, key):
        session = self.Session()

        try:
            value, = session.query(Variable.value).filter_by(key=key).one()
            return value
        except sqlalchemy.orm.exc.NoResultFound:
            return None
        finally:
            session.close()
    
    def set_variable(self, key, value):
        session = self.Session()

        session.query(Variable).filter_by(key=key).delete()

        variable = Variable(key=key, value=value)
        session.add(variable)

        session.commit()

    def cleanup(self):
        session = self.Session()

        now = time.time()

        query = session.query(Entry).filter(Entry.submission_timestamp < now-ENTRY_LIFETIME)

        deleted = set()
        for entry in query:
            binhash = entry.hash
            session.delete(entry)
            deleted.add(binhash)

        session.close()

        return now, deleted

    def delete_entry(self, retrieval_timestamp, **kwargs):
        session = self.Session()

        # delete entry from database
        try:
            entry = session.query(Entry).filter_by(**kwargs).one()
        except sqlalchemy.orm.exc.NoResultFound:
            return None

        binhash = entry.hash
        session.delete(entry)

        # add entry to deleted entries list
        deleted_entry = DeletedEntry(hash=binhash, retrieval_timestamp=retrieval_timestamp)
        session.add(deleted_entry)

        session.commit()

        return binhash

    def search(self, words=[], services=[]):
        """ Searches the database for certain words, and yields only profiles
            of users who use certain services.
            'words' must be a list of unicode objects and 'services' must be
            a list of str objects.
            Warning: Probably very slow! """

        session = self.Session()

        query = session.query(Entry)

        for word in words:
            condition = Entry.webfinger_address

            scondition = "%" + word.encode("utf8") + "%"
            ucondition = u"%" + word + u"%"

            condition = Entry.webfinger_address.like(scondition)
            condition |= Entry.full_name.like(ucondition)
            condition |= Entry.hometown.like(ucondition)
            condition |= Entry.country_code.like(scondition)

            query = query.filter(condition)

        for service in services:
            query = query.filter(
                  Entry.services.like(service)
                | Entry.services.like(service+",%")
                | Entry.services.like("%,"+service+",%")
                | Entry.services.like("%,"+service)
            )

        query = query.limit(50)

        for entry in query:
            session.expunge(entry)
            yield entry

    def entry_deleted(self, binhash):
        session = self.Session()

        try:
            retrieval_timestamp, = session.query(DeletedEntry.retrieval_timestamp).filter_by(hash=binhash).one()
            return retrieval_timestamp
        except sqlalchemy.orm.exc.NoResultFound:
            return None
        finally:
            session.close()

    def save_state(self, webfinger_address, state, retrieval_timestamp):
        """ Updates the Entry and DeletedEntry tables to reflect the current state of the profile at
            'webfinger_address'. Takes care that only older entries are overwritten, that we don't
            accept submission_timestamps from the future, and that users don't update their profiles
            too often.
            'state' may be an Entry instance or None, which indicates that the profile is non-existant
            or invalid. """

        session = self.Session()

        added = set()
        deleted = set()
        ignored = set()

        # get old database entry
        try:
            old_entry = session.query(Entry).filter_by(webfinger_address=webfinger_address).one()
        except sqlalchemy.orm.exc.NoResultFound:
            old_entry = None

        if state and state.submission_timestamp>time.time():
            self.logger.warning("Entry for '%s' has a submission timestamp in the future, will not be saved." % (
                webfinger_address,
                old_entry.submission_timestamp,
                state.submission_timestamp
            ))
            ignored.add(state.hash)

        elif state==None and old_entry:
            # delete entry
            deleted_entry = DeletedEntry(hash=old_entry.hash, retrieval_timestamp=retrieval_timestamp)
            session.add(deleted_entry)

            deleted.add(old_entry.hash)
            session.delete(old_entry)

        elif old_entry: # and state
            if state.submission_timestamp<old_entry.submission_timestamp:
                # ignore if we have more recent information in the database
                ignored.add(state.hash)
            elif state.submission_timestamp>old_entry.submission_timestamp+RESUBMISSION_INTERVAL:
                # overwrite
                deleted_entry = DeletedEntry(hash=old_entry.hash, retrieval_timestamp=retrieval_timestamp)
                session.add(deleted_entry)

                deleted.add(old_entry.hash)
                session.delete(old_entry)
                session.commit()

                added.add(state.hash)
                session.add(state)
            else:
                self.logger.warning("Webfinger address '%s' was resubmitted too frequently (%d/%d)" % (
                    webfinger_address,
                    old_entry.submission_timestamp,
                    state.submission_timestamp
                ))
                ignored.add(state.hash)

        elif state:
            added.add(state.hash)
            session.add(state)

        session.commit()

        return added,deleted,ignored

    def close(self, erase=False):
        if hasattr(self, "Session"):
            self.Session.close_all()
            del self.Session

        if erase and os.path.exists(self.dbfile):
            os.remove(self.dbfile)