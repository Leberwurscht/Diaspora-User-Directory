#!/usr/bin/env python

import logging

import threading, socket
import os, binascii

import subprocess
import select

import shutil

def _forward_messages(partnersocket, cin, cout):
    """ Forwards messages from a socket to a Popened process. cin and cout are stdin and stdout for the process.
        A message is built of an one byte announcement containing the message length and then the message itself. """
    socketfile = partnersocket.makefile()

    channels = [socketfile, cout]

    while channels:
        inputready,outputready,exceptready = select.select(channels,[],[])

        for inputfile in inputready:
            if inputfile==socketfile:
                outputfile=cin
            elif inputfile==cout:
                outputfile=socketfile

            announcement = inputfile.read(1)
            message_length = ord(announcement)

            message = ""
            while len(message)<message_length:
                message += inputfile.read(message_length - len(message))

            outputfile.write(announcement)
            outputfile.write(message)
            outputfile.flush()

            if message_length==0: channels.remove(inputfile)

    socketfile.close()

class HashTrie:
    def __init__(self, suffix="", erase=False):
        self.logger = logging.getLogger("HashTrie"+suffix)

        self.dbdir = "PTree"+suffix
        logfile = "trieserver"+suffix

        # erase database if requested
        if erase and os.path.exists(self.dbdir):
            shutil.rmtree(self.dbdir)

        # run trieserver
        self.trieserver = subprocess.Popen(["./trieserver", self.dbdir, logfile], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        self.lock = threading.Lock()

    def _synchronize_common(self, partnersocket, command):
        self.lock.acquire()

        # conduct the synchronization
        self.trieserver.stdin.write(command+"\n")
        self.trieserver.stdin.flush()

        assert self.trieserver.stdout.readline()=="OK\n"

        _forward_messages(partnersocket, self.trieserver.stdin, self.trieserver.stdout)

        # get the result of the synchronization
        assert self.trieserver.stdout.readline()=="NUMBERS\n"
       
        binhashes = set()

        while True:
            hexhash = self.trieserver.stdout.readline().strip()
            if not hexhash: break

            binhash = binascii.unhexlify(hexhash)
            binhashes.add(binhash)

        assert self.trieserver.stdout.readline()=="DONE\n"

        self.lock.release()

        return binhashes

    def synchronize_as_server(self, partnersocket):
        return self._synchronize_common(partnersocket, "SYNCHRONIZE_AS_SERVER")

    def synchronize_as_client(self, partnersocket):
        return self._synchronize_common(partnersocket, "SYNCHRONIZE_AS_CLIENT")

    def _add_delete_common(self, binhashes, command):
        self.lock.acquire()

        self.trieserver.stdin.write(command+"\n")
        assert self.trieserver.stdout.readline()=="OK\n"

        for binhash in binhashes:
            hexhash = binascii.hexlify(binhash)
            self.trieserver.stdin.write(hexhash+"\n")
        self.trieserver.stdin.write("\n")
        self.trieserver.stdin.flush()

        assert self.trieserver.stdout.readline()=="DONE\n"

        self.lock.release()

    def add(self, binhashes):
        self._add_delete_common(binhashes, "ADD")

    def delete(self, binhashes):
        self._add_delete_common(binhashes, "DELETE")

    def close(self, erase=False):
        if not self.trieserver: return

        self.lock.acquire()

        self.trieserver.stdin.write("EXIT\n")
        self.trieserver.stdin.flush()

        self.trieserver.wait()
        self.trieserver = None

        if erase and os.path.exists(self.dbdir):
            shutil.rmtree(self.dbdir)

        self.lock.release()
