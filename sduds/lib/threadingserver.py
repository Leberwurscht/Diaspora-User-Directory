#!/usr/bin/env python

import SocketServer

class ThreadingServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    """ A threading :class:`SocketServer.TCPServer` with ``allow_reuse_address``
        set to ``True`` and an additional :meth:`terminate` method to facilitate
        shutting down properly.
    """

    allow_reuse_address = True

    def __init__(self, address, handler_class):
        SocketServer.TCPServer.__init__(self, address, handler_class)

    def terminate(self):
        """ Shuts down server by calling :func:`SocketServer.shutdown` and
            :func:`SocketServer.socket.close` """

        self.shutdown()
        self.socket.close()
