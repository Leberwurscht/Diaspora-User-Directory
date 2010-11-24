#!/usr/bin/env python

"""
This module tests the functions provided by the entries module. It
will create a test database entry, run some checks and output some
information.

Two notes:
 - Do not run this while sduds.py is running, as both will try to
   run the trieserver executable.
 - This script does not only need the public key of the captcha
   provider, but also the private one. It must be placed at the path
   './captchakey'.
"""

# run the trieserver executable
import trieserver

# cryptography functions
import paramiko

def get_private_key(path="captchakey"):
    private_key = paramiko.RSAKey(filename=path)
    return private_key

def sign(private_key, text):
    sig_message = private_key.sign_ssh_data(paramiko.randpool, text)
    sig_message.rewind()

    keytype = sig_message.get_string()
    assert keytype=="ssh-rsa"

    signature = sig_message.get_string()

    return signature

private_key = get_private_key()

import entries, binascii, sqlite3

# create an example database entry
webfinger_address = u"test@example.com"
full_name = u"John Doe"
hometown = u"Los Angeles"
country_code = u"US"
services = "diaspora,email"
captcha_signature = sign(private_key, webfinger_address.encode("utf-8"))
timestamp = 1290117971

entry = entries.Entry(webfinger_address, full_name, hometown, country_code, services, captcha_signature, timestamp)
assert entry.captcha_signature_valid()

entrylist = entries.EntryList([entry])

# save it
try:
    entrylist.save()
except sqlite3.IntegrityError:
    print "already in db"

# print hash
binhash = entry.hash
hexhash = binascii.hexlify(binhash)
print hexhash

# test loading from database
entrylist2 = entries.EntryList.from_database([binhash])
entry2 = entrylist2[0]
hexhash2 = binascii.hexlify(entry2.hash)
assert hexhash2==hexhash

# test exporting a EntryList as JSON
json_string = entrylist2.json()
print json_string

# test loading a EntryList from JSON
entrylist3 = entries.EntryList.from_json(json_string)
entry3 = entrylist3[0]
hexhash3 = binascii.hexlify(entry3.hash)
assert hexhash3==hexhash

# run a EntryServer
entryserver_interface = "localhost"
entryserver_port = 20001
entryserver = entries.EntryServer(entryserver_interface, entryserver_port)

# get a EntryList from the EntryServer
entrylist4 = entries.EntryList.from_server([binhash], (entryserver_interface, entryserver_port))
entry4 = entrylist4[0]
hexhash4 = binascii.hexlify(entry4.hash)
assert hexhash4==hexhash
