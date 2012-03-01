#!/usr/bin/env python

import SocketServer

class BaseServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    """ a threading TCPServer with allow_reuse_address """

    allow_reuse_address = True

    def __init__(self, address, handler_class):
        SocketServer.TCPServer.__init__(self, address, handler_class)

    def terminate(self):
        self.shutdown()
        self.socket.close()
