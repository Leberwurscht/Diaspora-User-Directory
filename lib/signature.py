#!/usr/bin/env python

import base64, paramiko

def signature_valid(public_key_base64, signature, text):
    data = base64.decodestring(public_key_base64)
    public_key = paramiko.RSAKey(data=data)

    sig_message = paramiko.Message()
    sig_message.add_string("ssh-rsa")
    sig_message.add_string(signature)
    sig_message.rewind()

    return public_key.verify_ssh_sig(text, sig_message)
