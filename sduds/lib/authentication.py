#!/usr/bin/env python

"""
This implements server and client side of authentication with a HMAC-SHA512 challenge-response algorithm.

Example usage (server side)::

    import SocketServer

    class RequestHandler(AuthenticatingRequestHandler):
        def get_password(self, username):
            if username=="user1":
                return "1234"
            else:
                return None

        def handle_user(self, username):
            print username, "authenticated"
            # ... communicate with client using self.request ...

    server = SocketServer.TCPServer(("", 20000), RequestHandler)
    server.serve_forever()

Example usage (client side)::

    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("", 20000))

    if authenticate_socket(sock, "user1", "1234"):
        print "successfully authenticated"
        # ... communicate with server using sock ...

"""

import SocketServer, uuid, hmac, hashlib

def authenticate_socket(sock, username, password):
    """ Authenticates a socket using the HMAC-SHA512 algorithm. This is the
        counterpart of :class:`AuthenticatingRequestHandler`. Returns whether
        authentication was successful. If it wasn't, the socket is closed.

        :param sock: the network socket
        :type sock: `socket.socket`
        :param username: the username; may not contain newline
        :type username: string
        :param password: the password
        :type password: string
        :rtype: boolean
    """
    f = sock.makefile()

    # make sure method is HMAC with SHA512
    method = f.readline().strip()
    if not method=="HMAC-SHA512":
        # TODO: logging
        f.close()
        sock.close()
        return False

    # send username
    assert not "\n" in username
    f.write(username+"\n")
    f.flush()

    # receive challenge
    challenge = f.readline().strip()

    # send response
    response = hmac.new(password, challenge, hashlib.sha512).hexdigest()
    f.write(response+"\n")
    f.flush()

    # check answer
    answer = f.readline().strip()
    f.close()

    if answer=="ACCEPTED":
        return True
    else:
        sock.close()
        return False

class AuthenticatingRequestHandler(SocketServer.BaseRequestHandler):
    """ Abstract base class for a `RequestHandler` which requires authentication with
        the HMAC-SHA512 algorithm. To learn how a `RequestHandler` is used, see the
        section about `SocketServer.BaseRequestHandler` in the Python documentation.

        To verify the credentials of the client, the method :meth:`get_password` is used,
        which must be overridden in subclasses. When authentication succeeds, the
        :meth:`handle_user` method is called, which also has to be overridden in
        subclasses. This class is the counterpart of the :func:`authenticate_socket`
        function.
    """

    def handle(self):
        f = self.request.makefile()

        # send expected authentication method
        method = "HMAC-SHA512"
        f.write(method+"\n")
        f.flush()

        # receive username
        username = f.readline().strip()

        # send challenge
        challenge = uuid.uuid4().hex
        f.write(challenge+"\n")
        f.flush()

        # receive response
        response = f.readline().strip()

        # compute response
        password = self.get_password(username)
        if password is None:
            f.write("DENIED\n")
            f.close()
            return

        computed_response = hmac.new(password, challenge, hashlib.sha512).hexdigest()

        # check response
        if not response==computed_response:
            f.write("INVALID PASSWORD\n")
            f.close()
            return

        f.write("ACCEPTED\n")
        f.close()

        self.handle_user(username)

    def get_password(self, username):
        """ Must be overridden in subclasses.

            This method is used to verify the credentials of the client.
            It must return the correct password for the given username,
            or None if the user should be rejected.

            :param username: the username the client transmitted
            :type username: string
            :rtype: string or NoneType
        """
        raise NotImplementedError, "Override this function in subclasses!"

    def handle_user(self, username):
        """ Must be overridden in subclasses.

            This method is called if authentication succeeded, with the username
            the client uses as an argument.

            :param username: username the client uses
            :type username: string
        """
        raise NotImplementedError, "Override this function in subclasses!"
