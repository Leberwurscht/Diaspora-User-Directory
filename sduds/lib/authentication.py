#!/usr/bin/env python

import SocketServer, uuid, hmac, hashlib

def authenticate_socket(sock, username, password):
    """ Authenticates a socket using the HMAC-SHA512 algorithm. This is the
        counterpart of AuthenticatingRequestHandler. """
    f = sock.makefile()

    # make sure method is HMAC with SHA512
    method = f.readline().strip()
    if not method=="HMAC-SHA512":
        # TODO: logging
        f.close()
        sock.close()
        return False

    # send username
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
    """ RequestHandler which checks the credentials transmitted by the other side
        using the HMAC-SHA512 algorithm.
        The get_password method must be overridden and return the password for a
        certain user. If the other side is authenticated, handle_user is called
        with the username as argument. """

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
        raise NotImplementedError, "Override this function in subclasses!"

    def handle_user(self, username):
        raise NotImplementedError, "Override this function in subclasses!"
