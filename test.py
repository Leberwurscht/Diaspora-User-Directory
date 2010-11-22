#!/usr/bin/env python

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
captcha_signature = sign(private_key, webfinger_address.encode("utf-8"))
timestamp = 1290117971

entry = entries.Entry(webfinger_address, full_name, hometown, country_code, captcha_signature, timestamp)
assert entry.captcha_signature_valid()

# save it
try:
    entry.save()
except sqlite3.IntegrityError:
    print "already in db"

# print hash
binhash = entry.hash
hexhash = binascii.hexlify(binhash)
print hexhash

# test loading from database
entry2 = entries.Entry.from_database(binhash)
hexhash2 = binascii.hexlify(entry2.hash)
assert hexhash2==hexhash

# test exporting a EntryList as JSON
entrylist = entries.EntryList([entry])
json_string = entrylist.json()
print json_string

# test loading a EntryList from JSON
entrylist2 = entries.EntryList.from_json(json_string)
entry3 = entrylist2[0]
hexhash3 = binascii.hexlify(entry3.hash)
assert hexhash3==hexhash

# run a EntryServer
entryserver = entries.EntryServer()

# get a EntryList from the EntryServer
entrylist3 = entries.EntryList.from_server([binhash], ("localhost",20001))
entry4 = entrylist3[0]
hexhash4 = binascii.hexlify(entry4.hash)
assert hexhash4==hexhash
