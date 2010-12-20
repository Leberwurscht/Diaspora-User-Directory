#!/usr/bin/env python

import logging

import socket

import time
import binascii, hashlib

import json

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

engine = sqlalchemy.create_engine('sqlite:///entries.sqlite')
Session = sqlalchemy.orm.sessionmaker(bind=engine)

import sqlalchemy.ext.declarative
DatabaseObject = sqlalchemy.ext.declarative.declarative_base()

######

import threading

class EntryServer(threading.Thread):
    def __init__(self, interface="localhost", port=20001):
            threading.Thread.__init__(self)

            entrysocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            entrysocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            entrysocket.bind((interface,port))
            entrysocket.listen(5)

            self.entrysocket = entrysocket

            self.daemon = True  # terminate if main program exits
            self.start()

    def run(self):
        while True:
            (clientsocket, address) = self.entrysocket.accept()

            thread = threading.Thread(
                target=self.handle_connection,
                args=(clientsocket, address)
            )

            thread.start()

    def handle_connection(self, clientsocket, address):
        f = clientsocket.makefile()

        # get list of hashes to be transmitted
        binhashes = []

        while True:
            hexhash = f.readline().strip()
            if not hexhash: break

            try:
                binhash = binascii.unhexlify(hexhash)
            except Exception,e:
                logging.warning("Invalid request for hash \"%s\" by %s: %s" % (hexhash, str(address), str(e)))
                continue

            binhashes.append(binhash)

        entrylist = EntryList.from_database(binhashes)

        # serve requested hashes
        json_string = entrylist.json()
        f.write(json_string)
        f.close()
        clientsocket.close()

######

class InvalidHashError(Exception): pass
""" The partner included a hash field into its JSON string but it was wrong """

class InvalidListError(Exception): pass
""" EntryList transmitted as JSON was invalid """

class WrongEntriesError(Exception): pass
""" Requested entries were missing or entries not requested were transmitted """

class EntryList(list):
    @classmethod
    def from_database(cls, binhashes):
        global Session
        session = Session()

        entrylist = EntryList()

        for binhash in binhashes:
            try:
                entry = session.query(Entry).filter_by(hash=binhash).one()
            except sqlalchemy.orm.exc.NoResultFound:
                logging.warning("Requested hash \"%s\" does not exist." % binascii.hexlify(binhash))
            else:
                entrylist.append(entry)

        return entrylist

    @classmethod
    def from_server(cls, binhashes, address):
        asocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        asocket.connect(address)

        for binhash in binhashes:
            hexhash = binascii.hexlify(binhash)
            asocket.sendall(hexhash+"\n")
        asocket.sendall("\n")

        f = asocket.makefile()
        json_string = f.read()
        f.close()
        asocket.close()

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
                json_entry["webfinger_address"],
                full_name=json_entry["full_name"],
                hometown=json_entry["hometown"],
                country_code=json_entry["country_code"],
                services=json_entry["services"],
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
                "hash":binascii.hexlify(entry.hash),
                "webfinger_address":entry.webfinger_address,
                "full_name":entry.full_name,
                "hometown":entry.hometown,
                "country_code":entry.country_code,
                "services":entry.services,
                "captcha_signature":binascii.hexlify(entry.captcha_signature),
                "submission_timestamp":entry.submission_timestamp,
                "retrieval_timestamp":entry.retrieval_timestamp
            })

        json_string = json.dumps(json_list)

        return json_string

    def save(self):
        global Session
        session = Session()

        # open unix domain socket for adding hashes to the prefix tree
        logging.debug("Connect to unix socket add.ocaml2py.sock.")
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect("add.ocaml2py.sock")

        for entry in self:
            hexhash = binascii.hexlify(entry.hash)

            # add entry to database
            session.add(entry)

            try:
                session.commit()
            except sqlalchemy.exc.IntegrityError:
                logging.warning("Attempted reinsertion of %s (%s) into the database" % (entry.webfinger_address, hexhash))
            else:
                # add hash to prefix tree
                s.sendall(hexhash+"\n")
                logging.debug("Sent hash %s to unix socket add.ocaml2py.sock." % hexhash)

        s.close()
        logging.debug("Closed unix socket add.ocaml2py.sock.")

# https://github.com/jcarbaugh/python-webfinger
import pywebfinger

import urllib

class Entry(DatabaseObject):
    """ represents a database entry for a webfinger address """

    __tablename__ = 'entries'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    hash = sqlalchemy.Column(sqlalchemy.BLOB, index=True, unique=True)
    webfinger_address = sqlalchemy.Column(lib.String, index=True, unique=True)
    full_name = sqlalchemy.Column(sqlalchemy.UnicodeText)
    hometown = sqlalchemy.Column(sqlalchemy.UnicodeText)
    country_code = sqlalchemy.Column(lib.String(2))
    services = sqlalchemy.Column(lib.String)
    captcha_signature = sqlalchemy.Column(sqlalchemy.BLOB)
    submission_timestamp = sqlalchemy.Column(sqlalchemy.Integer)
    retrieval_timestamp = sqlalchemy.Column(sqlalchemy.Integer)

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
        country_code = json_dict["country_code"]
        services = json_dict["services"]
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
#        global Session
#        session = Session()
#
#        try:
#            old_entry = session.query(self.__class__).filter_by(webfinger_address=webfinger_address).one()
#        except sqlalchemy.orm.exc.NoResultFound: pass
#        else:
#            kwargs["id"] = old_entry.id

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
        r += "Address: "+self.webfinger_address.encode("utf8")+"\n"
        r += "Full name: "+self.full_name.encode("utf8")+"\n"
        r += "Hometown: "+self.hometown.encode("utf8")+"\n"
        r += "Country code: "+self.country_code.encode("utf8")+"\n"
        r += "Services: "+self.services.encode("utf8")+"\n"
        r += "Captcha signature: "+binascii.hexlify(self.captcha_signature)[:20]+"...\n"
        r += "Submission time: "+time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime(self.submission_timestamp))+"\n"
        r += "Retrieval time: "+time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime(self.retrieval_timestamp))+"\n"

        return r

# create tables if they don't exist
DatabaseObject.metadata.create_all(engine)

class DatabaseOperation:
    def verify(self):
        raise NotImplementedError, "Override this function in subclasses."

    def execute(self):
        raise NotImplementedError, "Override this function in subclasses."

class AddEntry(DatabaseOperation):
    def __init__(self):
        raise NotImplementedError, "Not implemented yet."

    def verify(self):
        raise NotImplementedError, "Not implemented yet."

class DeleteEntry(DatabaseOperation):
    def __init__(self):
        raise NotImplementedError, "Not implemented yet."

    def verify():
        raise NotImplementedError, "Not implemented yet."
