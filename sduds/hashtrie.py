#!/usr/bin/env python

import threading
import os, binascii

import subprocess

import select, socket, struct
from exceptions import IOError

from sduds.lib import communication

import shutil

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
                except IOError, socket.timeout:
                    # TODO: logging
                    annoucement = "\0"
                    packet_length = 0
                    packet = ""

                cin.write(announcement)
                cin.write(packet)
                cin.flush()

                if packet_length==0:
                    channels.remove(sock)

            elif input_channel==cout:
                try:
                    announcement = cout.read(1)
                    if len(announcement)==0: raise IOError

                    packet_length, = struct.unpack("!B", announcement)

                    packet = cout.read(packet_length)
                    if not len(packet)==packet_length: raise IOError
                except IOError:
                    # TODO: logging
                    annoucement = "\0"
                    packet_length = 0
                    packet = ""

                sock.sendall(announcement)
                sock.sendall(packet)

                if packet_length==0:
                    channels.remove(cout)

class HashTrie:
    lock = None
    trieserver = None

    def __init__(self, database_path, manager_executable="trie_manager/manager"):
        # TODO: logging

        database_path = os.path.relpath(database_path)

        assert not database_path.endswith("/")
        logfile = database_path # .log is appended automatically by trieserver

        # run trieserver
        self.trieserver = subprocess.Popen([manager_executable, database_path, logfile], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        self.lock = threading.Lock()

    def _synchronize_common(self, partnersocket, command):
        with self.lock:
            # send command
            self.trieserver.stdin.write(command+"\n")
            self.trieserver.stdin.flush()

            response = self.trieserver.stdout.readline()
            assert response=="OK\n"

            # establish tunnel
            _forward_packets(partnersocket, self.trieserver.stdin, self.trieserver.stdout)

            # get the result of the synchronization
            assert self.trieserver.stdout.readline()=="NUMBERS\n"

            binhashes = set()

            while True:
                hexhash = self.trieserver.stdout.readline().strip()
                if not hexhash: break

                binhash = binascii.unhexlify(hexhash)
                binhashes.add(binhash)

            assert self.trieserver.stdout.readline()=="DONE\n"

            return binhashes

    def get_missing_hashes_as_server(self, partnersocket):
        return self._synchronize_common(partnersocket, "SYNCHRONIZE_AS_SERVER")

    def get_missing_hashes_as_client(self, partnersocket):
        return self._synchronize_common(partnersocket, "SYNCHRONIZE_AS_CLIENT")

    def _add_delete_common(self, binhashes, command):
        with self.lock:
            self.trieserver.stdin.write(command+"\n")
            self.trieserver.stdin.flush()
            assert self.trieserver.stdout.readline()=="OK\n"

            for binhash in binhashes:
                hexhash = binascii.hexlify(binhash)
                self.trieserver.stdin.write(hexhash+"\n")
            self.trieserver.stdin.write("\n")
            self.trieserver.stdin.flush()

            assert self.trieserver.stdout.readline()=="DONE\n"

    def add(self, binhashes):
        self._add_delete_common(binhashes, "ADD")

    def delete(self, binhashes):
        self._add_delete_common(binhashes, "DELETE")

    def close(self):
        if not self.trieserver: return

        with self.lock:
            self.trieserver.stdin.write("EXIT\n")
            self.trieserver.stdin.flush()

            self.trieserver.wait()
            self.trieserver = None
