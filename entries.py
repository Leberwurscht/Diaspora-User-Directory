#!/usr/bin/env python

import logging

import socket

import time
import binascii, hashlib

import json

import sqlite3 as db

def get_db_connection():
    return db.connect("entries.sqlite", check_same_thread=False)

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

import threading

class EntryServer(threading.Thread):
    def __init__(self, con, interface="localhost", port=20001):
            threading.Thread.__init__(self)

            self.con = con

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
        cur = self.con.cursor()
        f = clientsocket.makefile()

        # build list of entries to be transmitted
        entrylist = EntryList()

        while True:
            hexhash = f.readline().strip()
            if not hexhash: break

            binhash = binascii.unhexlify(hexhash)
            entry = Entry.from_database(cur, binhash)

            if entry: entrylist.append(entry)

        # serve requested hashes
        json_string = entrylist.json()
        f.write(json_string)
        f.close()
        clientsocket.close()

######

class InvalidHashError(Exception): pass

class EntryList(list):
    @classmethod
    def from_server(cls, binhashes, host, port=20001):
        asocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        asocket.connect((host, port))

        for binhash in binhashes:
            hexhash = binascii.hexlify(binhash)
            asocket.sendall(hexhash+"\n")
        asocket.sendall("\n")

        f = asocket.makefile()
        json_string = f.read()
        f.close()
        asocket.close()

        return cls.from_json(json_string)

    @classmethod
    def from_json(cls, json_string):
        entrylist = cls()

        for json_entry in json.loads(json_string):
            webfinger_address = json_entry["webfinger_address"]
            full_name = json_entry["full_name"]
            hometown = json_entry["hometown"]
            country_code = json_entry["country_code"]
            captcha_signature = binascii.unhexlify(json_entry["captcha_signature"])

            if "timestamp" in json_entry:
                timestamp = json_entry["timestamp"]
            else:
                timestamp = None

            entry = Entry(webfinger_address, full_name, hometown, country_code, captcha_signature, timestamp)

            if "hash" in json_entry:
                if not binascii.unhexlify(json_entry["hash"])==entry.hash:
                    raise InvalidHashError

            entrylist.append(entry)

        return entrylist

    def json(self):
        json_list = []

        for entry in self:
            json_list.append({
                "hash":binascii.hexlify(entry.hash),
                "webfinger_address":entry.webfinger_address,
                "full_name":entry.full_name,
                "hometown":entry.hometown,
                "country_code":entry.country_code,
                "captcha_signature":binascii.hexlify(entry.captcha_signature),
                "timestamp":entry.timestamp
            })

        json_string = json.dumps(json_list)

        return json_string

class Entry:
    """ represents a database entry for a webfinger address """

    @classmethod
    def from_database(cls, cur, binhash):
        cur.execute(
            "SELECT webfinger_address, full_name, hometown, country_code, captcha_signature, timestamp FROM entries WHERE hash=?",
            (buffer(binhash), )
        )
        args = cur.fetchone()
        if not args: return None

        entry = cls(*args)

        return entry

    @classmethod
    def from_webfinger_profile(cls, webfinger_address):
        # https://github.com/jcarbaugh/python-webfinger
        raise NotImplementedError, "Not implemented yet."

    def __init__(self, webfinger_address, full_name, hometown, country_code, captcha_signature, timestamp=None):
        """ arguments must be unicode strings, except captcha_signature (buffer) and timestamp (int) """
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
        return signature_valid(captcha_key, self.captcha_signature, self.webfinger_address.encode("utf-8"))

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
