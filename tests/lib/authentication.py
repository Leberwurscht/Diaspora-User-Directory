import unittest

from sduds.lib import authentication
import SocketServer, socket, threading

class Server(SocketServer.TCPServer):
    allow_reuse_address = True

class RequestHandler(authentication.AuthenticatingRequestHandler):
    def get_password(self, username):
        """ accepts only one user named user1 with passwort 1234 """
        if username=="user1":
            return "1234"
        else:
            return None

    def handle_user(self, username):
        """ saves all accepted users """
        self.server.successfully_authenticated.add(username)
        self.request.close()

class Authentication(unittest.TestCase):
    def setUp(self):
        # set up server
        # http://stackoverflow.com/questions/1365265/on-localhost-how-to-pick-a-free-port-number
        self.server = Server(("", 0), RequestHandler)
        self.server.successfully_authenticated = set()

        server_thread = threading.Thread(target=self.server.serve_forever)
        server_thread.start()
        self.addCleanup(server_thread.join)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

        # set up client
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        address = self.server.socket.getsockname()
        self.sock.connect(address)
        self.addCleanup(self.sock.close)

    def test_valid(self):
        success = authentication.authenticate_socket(self.sock, "user1", "1234")
        self.assertTrue(success)

        # wait until socket is closed by server, which means it has registered the username
        assert self.sock.recv(1)==""

        # make sure server accepted the user
        expected = set(["user1"])
        self.assertEqual(expected, self.server.successfully_authenticated)

    def test_invalid_user(self):
        success = authentication.authenticate_socket(self.sock, "user2", "1234")
        self.assertFalse(success)

        # make sure authenticate_socket closed the socket
        with self.assertRaises(Exception):
            self.sock.recv(1)

        # make sure server rejected the user
        expected = set()
        self.assertEqual(expected, self.server.successfully_authenticated)

    def test_invalid_password(self):
        success = authentication.authenticate_socket(self.sock, "user1", "1235")
        self.assertFalse(success)

        # make sure authenticate_socket closed the socket
        with self.assertRaises(Exception):
            self.sock.recv(1)

        # make sure server rejected the user
        expected = set()
        self.assertEqual(expected, self.server.successfully_authenticated)

if __name__ == '__main__':
    unittest.main()
