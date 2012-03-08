#!/usr/bin/env python

"""
This module defines functions to sign data and verify signatures using
the RSA PKCS#1 standard.

As PyCrypto versions older than 2.5 do not include an implementation of PKCS#1,
the paramiko module is used for that, which depends on PyCrypto.
"""

import paramiko
import base64, StringIO

def signature_valid(public_key_base64, signature, data):
    """ Checks a signature of given data using the public key.

        :param public_key_base64: base64-encoded public key
        :type public_key_base64: string
        :param signature: the raw signature
        :type signature: string
        :param data: the data for which the signature should be checked
        :type data: string
        :rtype: boolean
    """

    public_key_data = base64.decodestring(public_key_base64)
    public_key = paramiko.RSAKey(data=public_key_data)

    sig_message = paramiko.Message()
    sig_message.add_string("ssh-rsa")
    sig_message.add_string(signature)
    sig_message.rewind()

    return public_key.verify_ssh_sig(data, sig_message)

def sign(private_key_block, data):
    """ Calculates signature for given data using the private key.

        :param private_key_block: the private key block
        :type private_key_block: string
        :param data: the data which should be signed
        :type data: string
        :returns: the raw signature
        :rtype: string
    """
    f = StringIO.StringIO(private_key_block)
    private_key = paramiko.RSAKey(file_obj=f)

    randpool = None # RSA keys don't need a random number generator for signing
    sig_message = private_key.sign_ssh_data(randpool, data)
    sig_message.rewind()

    keytype = sig_message.get_string()
    assert keytype=="ssh-rsa"

    signature = sig_message.get_string()

    return signature
