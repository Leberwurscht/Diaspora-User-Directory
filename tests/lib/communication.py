import unittest

from sduds.lib import communication

import socket, threading
from exceptions import IOError

class Test(unittest.TestCase):
    def setUp(self):
        # set up two real connected sockets
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("", 0))
        server.listen(1)
        self.addCleanup(server.close)

        self.receiver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        address = server.getsockname()
        self.receiver.connect(address)

        self.sender, recv_addr = server.accept()

        # set NODELAY flag so that we could more reliably detect when
        # communication.recvall does the same thing as socket.recv, i.e.
        # return too few bytes.
        self.sender.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.receiver.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)

        self.addCleanup(self.sender.close)
        self.addCleanup(self.receiver.close)

    def test_recvall(self):
        """ recvall must return as many bytes as specified """

        # Send two messages in the following way:
        # - send first message (short)
        # - read one character at other end; if it has arrived, this
        #   indicates that the rest of the message has also arrived
        # - start a thread reading len(first_message)-1 + len(second_message)
        #   characters
        # - send second message
        #
        # Using this procedure, socket.recv would read less characters than
        # specified, but sduds.lib.recvall reads exactly the specified number of
        # characters. This test also makes sure that recvall doesn't read too
        # much characters using a third message.

        first_message = "X"+"123"
        second_message = "ABC"
        third_message = "\0"

        self.sender.sendall(first_message)

        assert self.receiver.recv(1)=="X"

        class Receiver(threading.Thread):
            def __init__(self, socket):
                threading.Thread.__init__(self)
                self.socket = socket

            def run(self):
                length = len(first_message)-1 + len(second_message)
                self.received = communication.recvall(self.socket, length)

        thread = Receiver(self.receiver)
        thread.start()

        self.sender.sendall(second_message+third_message)

        thread.join()
        self.assertEqual(thread.received, "123ABC")

        terminator = self.receiver.recv(1)
        self.assertEqual(terminator, third_message)

    def test_recvall_exception(self):
        """ recvall must raise socket.error if socket is closed before specified
            number of bytes is read """

        message = "123"
        self.sender.sendall(message)
        self.sender.shutdown(socket.SHUT_WR)

        with self.assertRaises(IOError):
            communication.recvall(self.receiver, 10)

    def test_char(self):
        sent = 255
        communication.send_char(self.sender, sent)
        received = communication.recv_char(self.receiver)
        self.assertEqual(sent, received)

    def test_integer(self):
        sent = 256**4 - 1
        communication.send_integer(self.sender, sent)
        received = communication.recv_integer(self.receiver)
        self.assertEqual(sent, received)

    def test_short_str(self):
        sent = "X"*255
        communication.send_short_str(self.sender, sent)
        received = communication.recv_short_str(self.receiver)
        self.assertEqual(sent, received)

    def test_str(self):
        sent = "X"*1000
        assert len(sent)>255

        communication.send_str(self.sender, sent)
        received = communication.recv_str(self.receiver)
        self.assertEqual(sent, received)

    def test_unicode(self):
        sent = u"X"*1000
        assert len(sent)>255

        communication.send_unicode(self.sender, sent)
        received = communication.recv_unicode(self.receiver)
        self.assertEqual(type(received), unicode)
        self.assertEqual(sent, received)

if __name__ == '__main__':
    unittest.main()
