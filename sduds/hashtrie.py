#!/usr/bin/env python

"""
This module is the python interface to the ``trie_manager/manager`` executable, which manages a large
set of 16 byte hashes in a manner that efficient synchronization of these hashes between two servers
is possible.

The module offers a :class:`HashTrie` object which has methods for :meth:`adding <HashTrie.add>` and
:meth:`deleting <HashTrie.delete>` hashes from the set, a :meth:`~HashTrie.contains` method and synchronization methods for
the :meth:`server <HashTrie.get_missing_hashes_as_server>` and :meth:`client <HashTrie.get_missing_hashes_as_client>`
side.

Communication with the ``manager`` process works by sending commands over standard input and output, as
described in the documentation of the :ref:`trie manager<trie_manager>` program.
"""

import threading
import os, shutil, binascii

import subprocess

import select, socket, struct
from exceptions import IOError

from sduds.lib import communication

def _forward_packets(sock, cin, cout):
    """ Forwards packets from a socket to a Popened process. cin and cout are stdin and stdout for the process.
        A packet is built of an one byte announcement containing the packet length and then the payload. """

    channels = [sock, cout]

    # take socket timeout also for select
    timeout = sock.gettimeout()

    while channels:
        inputready,outputready,exceptready = select.select(channels,[],[], timeout)

        if not inputready:
            # select timed out due to socket, therefore don't read from sock anymore
            # TODO: logging
            cin.write("\0")
            cin.flush()

            sock.close()
            channels.remove(sock)

        for input_channel in inputready:
            if input_channel==sock:
                try:
                    announcement = communication.recvall(sock, 1)
                    packet_length, = struct.unpack("!B", announcement)
                    packet = communication.recvall(sock, packet_length)
                except (IOError, socket.timeout):
                    # reading from sock failed, therefore don't read from sock anymore
                    # TODO: logging
                    cin.write("\0")
                    cin.flush()

                    sock.close()
                    channels.remove(sock)
                else:
                    cin.write(announcement)
                    cin.write(packet)
                    cin.flush()

                    if packet_length==0:
                        channels.remove(sock)

            elif input_channel==cout:
                announcement = cout.read(1)
                assert len(announcement)==1

                packet_length, = struct.unpack("!B", announcement)

                packet = cout.read(packet_length)
                assert len(packet)==packet_length

                try:
                    sock.sendall(announcement)
                    sock.sendall(packet)
                except (IOError, socket.timeout):
                    # writing to sock failed, therefore don't read from sock anymore
                    if sock in channels:
                        cin.write("\0")
                        cin.flush()

                        sock.close()
                        channels.remove(sock)

                if packet_length==0:
                    channels.remove(cout)

class HashTrie:
    """ This class takes care of starting the ``manager`` executable as subprocess, and
        serves as a higher-level interface to the commands this executable makes accessible
        over standard input and output.
    """

    lock = None
    manager_process = None

    def __init__(self, database_path, manager_executable="trie_manager/manager"):
        """ :param database_path: path to database directory, without trailing slash
            :type database_path: string
            :param manager_executable: the path to the ``manager`` executable (optional)
            :type manager_executable: string
        """

        database_path = os.path.relpath(database_path)

        assert not database_path.endswith("/")
        logfile = database_path # .log is appended automatically by manager

        # run manager process
        self.manager_process = subprocess.Popen([manager_executable, database_path, logfile], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        self.lock = threading.Lock()

    def _synchronize_common(self, partnersocket, command):
        """ Both SYNCHRONIZATION commands obey the same protocol, so to avoid
            duplicated code, this function is called by the server and by the
            client synchronization method.
        """

        with self.lock:
            # send command
            self.manager_process.stdin.write(command+"\n")
            self.manager_process.stdin.flush()

            # read response
            response = self.manager_process.stdout.readline()
            assert response=="OK\n"

            # establish tunnel
            _forward_packets(partnersocket, self.manager_process.stdin, self.manager_process.stdout)

            # get the result of the synchronization
            assert self.manager_process.stdout.readline()=="NUMBERS\n"

            binhashes = set()

            while True:
                hexhash = self.manager_process.stdout.readline().strip()
                if not hexhash: break

                binhash = binascii.unhexlify(hexhash)
                binhashes.add(binhash)

            # read DONE
            response = self.manager_process.stdout.readline()
            assert response=="DONE\n"

            return binhashes

    def get_missing_hashes_as_server(self, partnersocket):
        """ This method compares the own set of hashes with the one
            of a remote machine and returns the hashes which are missing
            in the own database. The method does not alter the database.

            This is the counterpart of :meth:`get_missing_hashes_as_client`.

            :param partnersocket: the connection to the other machine
            :type partnersocket: :class:`socket.socket`
            :rtype: :class:`set` of raw 16-byte hashes
        """
        return self._synchronize_common(partnersocket, "SYNCHRONIZE_AS_SERVER")

    def get_missing_hashes_as_client(self, partnersocket):
        """ This method compares the own set of hashes with the one
            of a remote machine and returns the hashes which are missing
            in the own database. The method does not alter the database.

            This is the counterpart of :meth:`get_missing_hashes_as_server`.

            :param partnersocket: the connection to the other machine
            :type partnersocket: :class:`socket.socket`
            :rtype: :class:`set` of raw 16-byte hashes
        """
        return self._synchronize_common(partnersocket, "SYNCHRONIZE_AS_CLIENT")

    def _add_delete_common(self, binhashes, command):
        """ The ADD and DELETE commands obey the same protocol, so to avoid
            duplicated code, this function is called by the :meth:`add` and
            the :meth:`delete` method.
        """

        with self.lock:
            # send command
            self.manager_process.stdin.write(command+"\n")
            self.manager_process.stdin.flush()

            # read response
            response = self.manager_process.stdout.readline()
            assert response=="OK\n"

            # send list of hashes
            for binhash in binhashes:
                hexhash = binascii.hexlify(binhash)
                self.manager_process.stdin.write(hexhash+"\n")
            self.manager_process.stdin.write("\n")
            self.manager_process.stdin.flush()

            # read response
            response = self.manager_process.stdout.readline()
            assert response=="DONE\n"

    def add(self, binhashes):
        """ Adds a set of raw hashes to the database.

            .. warning::

                You may not try to add a hash that is already
                stored in the database.

            :param binhashes: raw 16-byte hashes
            :type binhashes: iterable
        """
        self._add_delete_common(binhashes, "ADD")

    def delete(self, binhashes):
        """ Deletes a set of raw hashes from the database.

            .. warning::

                You may not try to delete a hash that isn't
                stored in the database.

            :param binhashes: raw 16-byte hashes
            :type binhashes: iterable
        """
        self._add_delete_common(binhashes, "DELETE")

    def contains(self, binhash):
        """ Returns whether a hash is contained in the database.

            :param binhash: raw 16-byte hash
            :type binhash: string
            :rtype: boolean
        """

        # send command
        self.manager_process.stdin.write("EXISTS\n")
        self.manager_process.stdin.flush()

        # read response
        response = self.manager_process.stdout.readline()
        assert response=="OK\n"

        # send hash
        hexhash = binascii.hexlify(binhash)
        self.manager_process.stdin.write(hexhash+"\n")
        self.manager_process.stdin.flush()

        # read response
        response = self.manager_process.stdout.readline()
        if response=="TRUE\n":
            contained = True
        elif response=="FALSE\n":
            contained = False

        # read DONE
        response = self.manager_process.stdout.readline()
        assert response=="DONE\n"

        # return result
        return contained

    def close(self):
        """ Terminates the internal ``manager`` subprocess. Blocks until this is finished.

            It is safe to call this method multiple times.
        """

        if not self.manager_process: return

        with self.lock:
            self.manager_process.stdin.write("EXIT\n")
            self.manager_process.stdin.flush()

            self.manager_process.wait()
            self.manager_process = None
