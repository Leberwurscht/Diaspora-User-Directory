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

import Queue, threading
from Queue import PriorityQueue, Full

class TwoPriorityQueue:
    """ For Queue-like objects accepting two types of items: high-priority and low-priority ones.
        For both types, separate maxsizes can be specified. """

    def __init__(self, maxsize_low=0, maxsize_high=0):
        self.queue = PriorityQueue()

        self.size_lock = threading.Lock()

        self.size_low = 0
        self.maxsize_low = maxsize_low

        self.size_high = 0
        self.maxsize_high = maxsize_high

    def put_item(self, item):
        priority = item[0]

        self.size_lock.acquire()

        try:
            if priority>0:
                # low-priority
                if self.maxsize_low>0 and self.size_low >= self.maxsize_low:
                    raise Full

                self.queue.put(item)
                self.size_low += 1
            else:
                # high-priority
                if self.maxsize_high>0 and self.size_high >= self.maxsize_high:
                    raise Full

                self.queue.put(item)
                self.size_high += 1
        finally:
            self.size_lock.release()

    def get_item(self):
        item = self.queue.get()

        priority = item[0]

        self.size_lock.acquire()

        if priority>0:
            # low-priority
            self.size_low -= 1
        else:
            # high-priority
            self.size_high -= 1

        self.size_lock.release()

        return item

    def put_high(self, item):
        self.put_item((0, item))

    def put_low(self, item):
        self.put_item((1, item))

    def get(self):
        return self.get_item()[1]

    def task_done(self):
        self.queue.task_done()

    def join(self):
        self.queue.join()
