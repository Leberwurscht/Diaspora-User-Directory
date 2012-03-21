import unittest

import os, tempfile, shutil
import binascii
import threading, socket

from sduds.hashtrie import HashTrie

class AddDeleteContains(unittest.TestCase):
    def setUp(self):
        directory = tempfile.mkdtemp() # create temporary directory
        self.addCleanup(shutil.rmtree, directory)

        database_path = os.path.join(directory, "trie.bdb")
        self.hashtrie = HashTrie(database_path)
        self.addCleanup(self.hashtrie.close)

    def test_add(self):
        """ contains() must return True for a hash which is added to the database """

        binhash = binascii.unhexlify("00112233445566778899AABBCCDDEEFF")
        self.hashtrie.add([binhash])

        contained = self.hashtrie.contains(binhash)
        self.assertEqual(contained, True)

    def test_contains(self):
        """ contains() must return False for a non-existant hash """

        binhash = binascii.unhexlify("00112233445566778899AABBCCDDEEFF")
        contained = self.hashtrie.contains(binhash)
        self.assertFalse(contained)

    def test_delete(self):
        """ contains() must return False after a hash is deleted from the database """

        binhash = binascii.unhexlify("00112233445566778899AABBCCDDEEFF")
        self.hashtrie.add([binhash])
        self.hashtrie.delete([binhash])

        contained = self.hashtrie.contains(binhash)
        self.assertFalse(contained)

class Synchronization(unittest.TestCase):
    def setUp(self):
        directory = tempfile.mkdtemp() # create temporary directory
        self.addCleanup(shutil.rmtree, directory)

        # create server trie
        database_path = os.path.join(directory, "server.bdb")
        self.server_trie = HashTrie(database_path)
        self.addCleanup(self.server_trie.close)

        # create client trie
        database_path = os.path.join(directory, "client.bdb")
        self.client_trie = HashTrie(database_path)
        self.addCleanup(self.client_trie.close)

    def test(self):
        """ make sure that the get_missing_hashes methods work on both sides """

        # create socket pair
        server_socket, client_socket = socket.socketpair()

        # add one hash to server trie
        server_hash = binascii.unhexlify("00112233445566778899AABBCCDDEE00")
        self.server_trie.add([server_hash])

        # add one hash to client trie
        client_hash = binascii.unhexlify("00112233445566778899AABBCCDDEE11")
        self.client_trie.add([client_hash])

        # add one hash to both tries
        common_hash = binascii.unhexlify("00112233445566778899AABBCCDDEEFF")
        self.server_trie.add([common_hash])
        self.client_trie.add([common_hash])

        # start synchronization thread for server and client side
        class SynchronizationThread(threading.Thread):
            def __init__(self, sock, method):
                threading.Thread.__init__(self)

                self.sock = sock
                self.method = method

            def run(self):
                self.missing_hashes = self.method(self.sock)

        server_thread = SynchronizationThread(server_socket, self.server_trie.get_missing_hashes_as_server)
        client_thread = SynchronizationThread(client_socket, self.client_trie.get_missing_hashes_as_client)

        server_thread.start()
        client_thread.start()

        server_thread.join()
        client_thread.join()

        # make sure the synchronization methods returned the right set of hashes
        self.assertEqual(server_thread.missing_hashes, set([client_hash]))
        self.assertEqual(client_thread.missing_hashes, set([server_hash]))

    def test_timeout(self):
        """ make sure that synchronization methods terminate even when socket times out """

        # test get_missing_hashes_as_server
        server_socket, dummy_socket = socket.socketpair()
        server_socket.settimeout(0.01)

        missing_hashes = self.server_trie.get_missing_hashes_as_server(server_socket)
        self.assertEqual(missing_hashes, set())

        # test get_missing_hashes_as_client
        client_socket, dummy_socket = socket.socketpair()
        client_socket.settimeout(0.01)

        missing_hashes = self.client_trie.get_missing_hashes_as_client(client_socket)
        self.assertEqual(missing_hashes, set())

    def test_close(self):
        """ make sure that synchronization methods terminate even when socket is closed unexpectedly """

        # test get_missing_hashes_as_server
        server_socket, dummy_socket = socket.socketpair()
        dummy_socket.close()

        missing_hashes = self.server_trie.get_missing_hashes_as_server(server_socket)
        self.assertEqual(missing_hashes, set())

        # test get_missing_hashes_as_client
        client_socket, dummy_socket = socket.socketpair()
        dummy_socket.close()

        missing_hashes = self.client_trie.get_missing_hashes_as_client(client_socket)
        self.assertEqual(missing_hashes, set())

if __name__ == '__main__':
    unittest.main()
