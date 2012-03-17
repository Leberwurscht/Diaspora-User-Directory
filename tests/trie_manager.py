import unittest

import os, shutil, subprocess
import struct, threading

executable = "trie_manager/manager"

class BaseTestCase(unittest.TestCase):
    def set_up_manager(self, name):
        database = name+".bdb"
        logfile = name

        if os.path.exists(database):
            shutil.rmtree(database)

        manager = subprocess.Popen([executable, database, logfile], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        self.addCleanup(os.remove, logfile+".log")
        self.addCleanup(shutil.rmtree, database)
        self.addCleanup(manager.wait)
        self.addCleanup(manager.stdin.flush)
        self.addCleanup(manager.stdin.write, "EXIT\n")

        return manager

    def _add_delete_hashes_common(self, manager, hashes, command):
        manager.stdin.write(command+"\n")
        manager.stdin.flush()
        response = manager.stdout.readline()
        self.assertEqual(response, "OK\n")

        for hexhash in hashes:
            manager.stdin.write(hexhash+"\n")

        manager.stdin.write("\n")
        manager.stdin.flush()
        response = manager.stdout.readline()
        self.assertEqual(response, "DONE\n")

    def add_hashes(self, manager, hashes):
        self._add_delete_hashes_common(manager, hashes, "ADD")

    def delete_hashes(self, manager, hashes):
        self._add_delete_hashes_common(manager, hashes, "DELETE")

    def contains_hash(self, manager, hexhash):
        manager.stdin.write("EXISTS\n")
        manager.stdin.flush()
        response = manager.stdout.readline()
        self.assertEqual(response, "OK\n")

        manager.stdin.write(hexhash+"\n")
        manager.stdin.flush()
        response = manager.stdout.readline()
        self.assertIn(response, ("TRUE\n", "FALSE\n"))
        contained = response

        response = manager.stdout.readline()
        self.assertEqual(response, "DONE\n")

        return contained=="TRUE\n"

class AddDeleteExists(BaseTestCase):
    def setUp(self):
        self.manager = self.set_up_manager("test")

    def test_add(self):
        """ add two hashes to database and make sure they are stored """

        # define two hashes
        hashes = ["00112233445566778899AABBCCDDEEFF", "00112233445566778899AABBCCDDEE00"]

        # add hashes to database
        self.add_hashes(self.manager, hashes)

        # make sure database contains hashes
        for hexhash in hashes:
            contained = self.contains_hash(self.manager, hexhash)
            self.assertTrue(contained)

    def test_nonexistant(self):
        """ the EXIST command must respond with FALSE if hash does not exist """

        hexhash = "00112233445566778899AABBCCDDEEFF"
        contained = self.contains_hash(self.manager, hexhash)
        self.assertFalse(contained)

    def test_delete(self):
        """ test the DELETE command """

        hexhash = "00112233445566778899AABBCCDDEEFF"

        # add hash to database
        self.add_hashes(self.manager, [hexhash])

        # delete hash from database
        self.delete_hashes(self.manager, [hexhash])

        # make sure hash is not stored anymore
        contained = self.contains_hash(self.manager, hexhash)
        self.assertFalse(contained)

# packet forwarder function
def forward_packets(cout, cin):
    while True:
        # read packet from cout
        announcement = cout.read(1)
        packet_length, = struct.unpack("!B", announcement)
        packet = cout.read(packet_length)
        assert len(packet)==packet_length # i.e. no EOF

        # forward packet to cin
        cin.write(announcement)
        cin.write(packet)
        cin.flush()

        # terminate after packet of length 0
        if packet_length==0: return

class Synchronization(BaseTestCase):
    def setUp(self):
        # run two manager instances: server and client
        self.manager_server = self.set_up_manager("testserver")
        self.manager_client = self.set_up_manager("testclient")

    def test(self):
        """ test synchronization commands """

        serverhash = "00112233445566778899AABBCCDDEEFF"
        clienthash = "00112233445566778899AABBCCDDEE00"

        manager_server = self.manager_server
        manager_client = self.manager_client

        # add one hash to each manager instance
        self.add_hashes(manager_server, [serverhash])
        self.add_hashes(manager_client, [clienthash])

        # send synchronization commands
        manager_server.stdin.write("SYNCHRONIZE_AS_SERVER\n")
        manager_server.stdin.flush()
        response = manager_server.stdout.readline()
        assert response=="OK\n"

        manager_client.stdin.write("SYNCHRONIZE_AS_CLIENT\n")
        manager_client.stdin.flush()
        response = manager_client.stdout.readline()
        assert response=="OK\n"

        # establish tunnel between manager instances
        server2client = threading.Thread(target=forward_packets, args=(manager_server.stdout, manager_client.stdin))
        client2server = threading.Thread(target=forward_packets, args=(manager_client.stdout, manager_server.stdin))

        server2client.start()
        client2server.start()

        # wait until synchronization is completed and tunnel is closed
        server2client.join()
        client2server.join()

        # get missing hashes of the server
        missing_hashes_of_server = set()

        response = manager_server.stdout.readline()
        assert response=="NUMBERS\n"

        while True:
            hex_number = manager_server.stdout.readline().strip()
            if not hex_number: break

            hex_number = hex_number.upper()
            missing_hashes_of_server.add(hex_number)

        response = manager_server.stdout.readline()
        assert response=="DONE\n"

        # get missing hashes of the client
        missing_hashes_of_client = set()

        response = manager_client.stdout.readline()
        assert response=="NUMBERS\n"

        while True:
            hex_number = manager_client.stdout.readline().strip()
            if not hex_number: break

            hex_number = hex_number.upper()
            missing_hashes_of_client.add(hex_number)

        response = manager_client.stdout.readline()
        assert response=="DONE\n"

        # check missing hashes
        self.assertEqual(missing_hashes_of_server, set([clienthash]))
        self.assertEqual(missing_hashes_of_client, set([serverhash]))

if __name__ == '__main__':
    unittest.main()
