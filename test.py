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

import entries, binascii

webfinger_address = "test@example.com"
full_name = "John Doe"
hometown = "Los Angeles"
country_code = "US"
captcha_signature = sign(private_key, webfinger_address)
timestamp = 1290117971

e = entries.Entry(webfinger_address, full_name, hometown, country_code, captcha_signature, timestamp)
assert e.captcha_signature_valid()
con = entries.get_db_connection()
e.save(con.cursor(), con)

print binascii.hexlify(e.hash)
