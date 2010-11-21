#!/usr/bin/env python

import logging

import socket

import time
import binascii, hashlib

import sqlite3 as db

def get_db_connection():
    return db.connect("entries.sqlite")

# check if table exists and create it if not

con = get_db_connection()
cur = con.cursor()
cur.execute("SELECT count(1) FROM sqlite_master WHERE type='table' AND name='entries'")
table_exists = cur.fetchone()[0]

if not table_exists:
    cur.execute("PRAGMA legacy_file_format=0")
    cur.execute("CREATE TABLE entries (hash BLOB UNIQUE, webfinger_address TEXT UNIQUE, full_name TEXT, hometown TEXT, "
               +"country_code CHARACTER(2), captcha_signature BLOB, timestamp INTEGER)")
    cur.execute("CREATE UNIQUE INDEX entries_hashes ON entries (hash)")
    cur.execute("CREATE UNIQUE INDEX entries_addresses ON entries (webfinger_address)")

cur.close()
con.close()

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

######

class Entry:
    """ represents a database entry for a webfinger address """

    @classmethod
    def from_hash(cls, entryhash):
        raise NotImplementedError, "Not implemented yet."

    @classmethod
    def from_webfinger_address(cls, webfinger_address):
        raise NotImplementedError, "Not implemented yet."

    @classmethod
    def fetch(cls, webfinger_address):
        # https://github.com/jcarbaugh/python-webfinger
        raise NotImplementedError, "Not implemented yet."

    def __init__(self, webfinger_address, full_name, hometown, country_code, captcha_signature, timestamp=None):
        """ arguments must be utf-8 encoded strings (except timestamp) """
        if not timestamp: timestamp = int(time.time())

        self.webfinger_address = webfinger_address
        self.full_name = full_name
        self.hometown = hometown
        self.country_code = country_code
        self.captcha_signature = captcha_signature
        self.timestamp = timestamp

        # compute hash
        combinedhash = hashlib.sha1()

        for data in [self.webfinger_address, self.full_name, self.hometown, self.country_code, self.timestamp]:
            subhash = hashlib.sha1(str(data)).hexdigest()
            combinedhash.update(subhash)

        # is it unsecure to take only 16 bytes of the hash?
        self.hash = combinedhash.digest()[:16]

    def captcha_signature_valid(self):
        return signature_valid(captcha_key, self.captcha_signature, self.webfinger_address)

    def save(self, cur, con=None):
        cur.execute("INSERT INTO entries (hash, webfinger_address, full_name, hometown, country_code, captcha_signature, timestamp) VALUES (?,?,?,?,?,?,?)",
            (buffer(self.hash),
            self.webfinger_address,
            self.full_name,
            self.hometown,
            self.country_code,
            buffer(self.captcha_signature),
            self.timestamp)
        )

        if con: con.commit()

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

#####################

def addhashes(l):
    logging.debug("Connect to unix socket add.ocaml2py.sock.")
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect("add.ocaml2py.sock")

    for h in l:
        hexhash = binascii.hexlify(h)
        s.sendall(hexhash+"\n")
        logging.debug("Sent hash %s to unix socket add.ocaml2py.sock." % hexhash)

    s.close()
    logging.debug("Closed unix socket add.ocaml2py.sock.")

if __name__=="__main__":
    import sys

    logging.basicConfig(level=logging.DEBUG)

    hashes = [binascii.unhexlify(i) for i in sys.argv[1:]]
    addhashes(hashes)
