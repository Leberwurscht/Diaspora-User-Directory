#!/usr/bin/env python

import sqlalchemy.types

class Binary(sqlalchemy.types.TypeDecorator):
    """ This type allows saving str objects in Binary columns. It converts between
        str and buffer automatically. """

    impl = sqlalchemy.types.Binary

    def process_bind_param(self, value, dialect):
        if value==None: return None

        return buffer(value)

    def process_result_value(self, value, dialect):
        if value==None: return None

        return str(value)

    def copy(self):
        return Binary(self.impl.length)


class Text(sqlalchemy.types.TypeDecorator):
    """ This type is a workaround for the fact that sqlite returns only unicode but not
        str objects. We need str objects to save URLs for example. """

    impl = sqlalchemy.types.UnicodeText

    def process_bind_param(self, value, dialect):
        if value==None: return None

        return unicode(value, "latin-1")

    def process_result_value(self, value, dialect):
        if value==None: return None

        return value.encode("latin-1")

    def copy(self):
        return Text(self.impl.length)

import SocketServer

class BaseServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    allow_reuse_address = True

    def __init__(self, address, handler_class):
        SocketServer.TCPServer.__init__(self, address, handler_class)

    def terminate(self):
        self.shutdown()
        self.socket.close()
