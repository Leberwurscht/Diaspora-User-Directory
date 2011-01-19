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

import SocketServer, threading

class BaseServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    allow_reuse_address = True

    def __init__(self, address, handler_class):
        SocketServer.TCPServer.__init__(self, address, handler_class)

    def terminate(self):
        self.shutdown()
        self.socket.close()

class NotifyingServer(BaseServer):
    """ A class for servers that provides a method that allows us to wait until data for a specific
        identifier arrived. """

    def __init__(self, address, handler_class):
        BaseServer.__init__(self, address, handler_class)

        self.lock = threading.Lock() # lock for events and data dictionaries
        self.events = {}
        self.data = {}

    def _get_event(self, identifier):
        """ Create event for an identifier if it does not exist and return it,
            otherwise return the existing one. Does not lock the dicts, you must
            do this yourself! """

        if not identifier in self.events:
            self.events[identifier] = threading.Event()

        return self.events[identifier]

    def set_data(self, identifier, data):
        with self.lock:
            self.data[identifier] = data
            self._get_event(identifier).set()
        
    def get_data(self, identifier):
        with self.lock:
            event = self._get_event(identifier)

        event.wait()

        with self.lock:
            data = self.data[identifier]
            del self.data[identifier]
            del self.events[identifier]

        return data
