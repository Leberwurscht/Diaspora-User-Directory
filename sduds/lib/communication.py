#!/usr/bin/env python

"""
This module implements some functions to facilitate working with sockets; first of all a :func:`recvall`
function to receive *exactly* the number of bytes one wants to have, and functions to send and receive
some built-in types of python.
"""

import socket, struct

def _recvall_chunks(sock, length):
    received = 0

    while received < length:
        chunk = sock.recv(length-received)
        if chunk=="":
            raise socket.error

        received += len(chunk)
        yield chunk

def recvall(sock, length):
    """ Receives *exactly* ``length`` bytes from a network socket, as opposed to
        :meth:`socket.socket.recv`, which returns *at most* the number of bytes
        specified. To circumvent this behaviour, one could also use :meth:`socket.makefile`,
        and then use :meth:`file.read`, but according to the python documentation
        this does not work for sockets with a timeout.

        This function raises a :class:`socket.error` if the socket is closed
        before as many bytes as specified are received.

        :param sock: the network socket
        :type sock: :class:`socket.socket`
        :param length: number of bytes to receive
        :type length: integer
        :rtype: string
    """

    # use method 6 from http://www.skymind.com/~ocrow/python_string/
    # for efficient string concatenation
    chunks = _recvall_chunks(sock, length)
    string = "".join(chunks)

    return string

# for sending a one byte unsigned integer
def send_char(sock, integer):
    """ Sends a one byte unsigned integer over a socket.

        :param integer: must be in the range 0--255
        :type integer: integer
    """
    packed_integer = struct.pack("!B", integer)
    sock.sendall(packed_integer)

def recv_char(sock):
    """ Receives a one byte unsigned integer from a socket.

        :rtype: integer
    """
    packed_integer = recvall(sock, 1)
    integer, = struct.unpack("!B", packed_integer)

    return integer

# for sending a 4 byte unsigned integer
def send_integer(sock, integer):
    """ Sends a 4 byte unsigned integer over a socket.

        :param integer: must be in the range 0--(256\ :sup:`4`-1)
        :type integer: integer
    """
    packed_integer = struct.pack("!I", integer)
    sock.sendall(packed_integer)

def recv_integer(sock):
    """ Receives a 4 byte unsigned integer from a socket.

        :rtype: integer
    """
    packed_integer = recvall(sock, 4)
    integer, = struct.unpack("!I", packed_integer)

    return integer

# for sending strings that are shorter than 256 bytes
def send_short_str(sock, string):
    """ Sends a short string over a socket.

        :param string: must be at most 255 bytes long
        :type string: string
    """
    length = len(string)
    send_char(sock, length)
    sock.sendall(string)

def recv_short_str(sock):
    """ Receives a short string from a socket.

        :rtype: string
    """
    length = recv_char(sock)
    string = recvall(sock, length)

    return string

# for sending strings that are shorter than 4 GiB
def send_str(sock, string):
    """ Sends a long string over a socket.

        :param string: must be shorter than 4 GiB
        :type string: string
    """
    length = len(string)
    send_integer(sock, length)
    sock.sendall(string)

def recv_str(sock):
    """ Receives a long string from a socket.

        :rtype: string
    """
    length = recv_integer(sock)
    string = recvall(sock, length)

    return string

# for sending unicode objects
def send_unicode(sock, u):
    """ Sends a unicode object over a socket.

        :param u: utf8-encoding of this must be shorter than 4 GiB
        :type u: unicode
    """
    string = u.encode("utf8")
    send_str(sock, string)

def recv_unicode(sock):
    """ Receives a unicode object from a socket.

        :rtype: unicode
    """
    string = recv_str(sock)
    u = unicode(string, "utf8")

    return u
