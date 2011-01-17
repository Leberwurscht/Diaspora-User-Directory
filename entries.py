#!/usr/bin/env python

import logging

import socket

import time
import binascii, hashlib

import urllib
import json

import os

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
                database.logger.warning("Requested hash \"%s\" does not exist." % binascii.hexlify(binhash))
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

        new_hashes = []

        for entry in self:
            hexhash = binascii.hexlify(entry.hash)

            # add entry to database
            session.add(entry)

            try:
                session.commit()
            except sqlalchemy.exc.IntegrityError:
                database.logger.warning("Attempted reinsertion of %s (%s) into the database" % (entry.webfinger_address, hexhash))
            else:
                new_hashes.append(entry.hash)

        return new_hashes

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
    def from_webfinger_address(cls, webfinger_address, submission_timestamp):

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

####
# database class

class Database:
    def __init__(self, suffix="", erase=False):
        global DatabaseObject

        self.suffix = suffix
        self.logger = logging.getLogger("entrydb"+suffix)

        self.dbfile = "entries"+suffix+".sqlite"

        if erase: self.erase()

        engine = sqlalchemy.create_engine("sqlite:///"+self.dbfile)
        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)

        # create tables if they don't exist
        DatabaseObject.metadata.create_all(engine)

    def delete_entry(self, **kwargs):
        session = self.Session()

        # delete entry from database
        try:
            entry = session.query(Entry).filter_by(**kwargs).one()
        except sqlalchemy.orm.exc.NoResultFound:
            return None

        binhash = entry.hash
        session.delete(entry)

        # add entry to deleted entries list
        deleted_entry = DeletedEntry(hash=binhash)
        session.add(deleted_entry)

        session.commit()

        return binhash

    def entry_deleted(self, binhash):
        session = self.Session()

        try:
            session.query(DeletedEntry).filter_by(hash=binhash).one()
            return True
        except sqlalchemy.orm.exc.NoResultFound:
            return False
        finally:
            session.close()

    def close(self, erase=False):
        if hasattr(self, "Session"): self.Session.close_all()

        if erase and os.path.exists(self.dbfile):
            os.remove(self.dbfile)
