trie_manager program
====================

The ``trie_manager/manager`` program manages a set of 16 byte hashes in a manner that
efficient synchronization of these hashes between two servers is possible. It is not meant
to be called manually; it requires some higher-level program which calls it and forwards
the synchronization traffic over the network to other machines.

This program is derived from the OpenPGP key server `SKS`_.

.. _SKS: http://code.google.com/p/sks-keyserver/

Command line arguments
----------------------

The program must be called with two arguments::

   ./manager DB_DIRECTORY LOG_FILE

Here, ``DB_DIRECTORY`` is the path of the directory in which the set is stored as a `BDB`_
database. If this directory does not exist yet, it is created automatically.

The ``LOG_FILE`` argument must specify the path to the log file without the ``.log``
extension, which is appended automatically. A good practice is to set it to ``DB_DIRECTORY``,
too.

.. _BDB: http://en.wikipedia.org/wiki/Berkeley_DB

Overview
--------

When the program is started, it waits for messages on standard input. A message
consists of a command followed by newline. Possible commands are:

* ADD
* DELETE
* SYNCHRONIZE_AS_SERVER
* SYNCHRONIZE_AS_CLIENT
* EXIT

The program responds with ``OK\n`` to a valid message, except to the EXIT command,
which terminates the program immediately.

ADD and DELETE commands
-----------------------

The ``ADD`` command adds a set of hashes to the database, and the ``DELETE`` command
deletes a set of hashes from the database.

After one of these commands, the program expects a list 16 byte hashes in
hexadecimal representation, each followed by newline. This list must be terminated
by an additional newline.

The program will respond with ``DONE\n`` when the specified hashes are added/deleted.

.. warning::

   You may not add hashes which are already stored, nor may you try to delete
   hashes which are not present in the database.

Synchronization commands
------------------------

The ``SYNCHRONIZE_AS_SERVER`` and ``SYNCHRONIZE_AS_CLIENT`` commands are used to get a list
of hashes which are present on a remote server but not in the own database. These commands
do not alter the database.

After one of these commands, standard input and output are used to tunnel synchronization traffic.
Therefore, the manager program must be called by some higher-level program which forwards this traffic
to another instance of the program. To be able to determine the end of this synchronization stream,
this higher-level program needs to know something about the structure of this traffic:

The tunnel works by splitting the raw traffic into packets of known length (maximal 255 bytes), and
sending each packet as a one-byte announcement containing the length and then the payload. If the
synchronization is finished, an announcement for a zero-length packet is sent.

After this, the tunnel can be regarded as closed. Then the program sends ``NUMBERS\n`` to standard output,
followed by the list of missing hashes. Each hash has 16 bytes and is sent in hexadecimal representation
and followed by a newline. The list is terminated by an additional newline.

Finally, the program transmits ``DONE\n``.

.. note::

   When connecting two instances of the manager program, one instance must use the ``SYNCHRONIZE_AS_SERVER``
   command and the other one the ``SYNCHRONIZE_AS_CLIENT`` command.

Usage
-----

The following example adds two hashes to a ``test.bdb`` database and deletes one of them afterwards.
In the end, only the hash ``00112233445566778899AABBCCDDEEFF`` is stored.
'``<``' indicates input and '``>``' indicates output of the program:

.. code-block:: text

   $ ./trie_manager/manager test.bdb test
   < ADD
   > OK
   < 00112233445566778899AABBCCDDEEFF
   < 00112233445566778899AABBCCDDEE00
   <
   > DONE
   < DELETE
   > OK
   < 00112233445566778899AABBCCDDEE00
   <
   > DONE
   < EXIT
   $

For synchronization, it must be taken care of establishing a tunnel to another instance of the program.
The following example python script runs two instances of the manager, with databases ``server.bdb``
and ``client.bdb`` respectively, and prints the missing hashes of each instance:

.. literalinclude:: trie_manager_example.py

In real world applications, the two instances of the manager program would run on different machines, and
the packets would be forwarded into network sockets connecting these machines.
