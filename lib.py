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

from Queue import PriorityQueue, Full
import threading

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

"""
A cron-like job scheduler

Usage:
    pattern = CronPattern("0-30/5", "4,5", "3", "*", "*")
    job = Job(pattern, callback, arguments)
    job.start()

    ... callback(arguments) will be executed regularly ...

    job.terminate()

Notes:
 * callback must call time.time() and return the value
 * Does not check whether pattern is valid; if no timestamp fits
   the pattern there will be an infinite loop.
 * All times are UTC.

"""

import time, calendar, threading

class CronPattern:
    def __init__(self, minute, hour, dom, month, dow):
        self.minute = CronPattern.parse_field(minute, 0, 59)
        self.hour = CronPattern.parse_field(hour, 0, 23)
        self.dom = CronPattern.parse_field(dom, 1, 31)
        self.month = CronPattern.parse_field(month, 1, 12)
        self.dow = CronPattern.parse_field(dow, 0, 6)

    @classmethod
    def parse_field(cls, field, first, last):
        fieldset = set()

        for subfield in field.split(","):
            if "/" in subfield:
                specifier,modulo = subfield.split("/")
                modulo = int(modulo)
            else:
                specifier = subfield
                modulo = 1

            if specifier=="*":
                subset = set(range(first, last+1))
            elif "-" in specifier:
                rfirst,rlast = specifier.split("-")
                rfirst = int(rfirst)
                rlast = int(rlast)

                if rfirst<first: raise ValueError
                if rlast>last: raise ValueError

                subset = set(range(rfirst, rlast+1))
            else:
                i = int(specifier)
                if i<first: raise ValueError
                if i>last: raise ValueError

                subset = set([i])

            for i in list(subset):
                if not i%modulo==0: subset.remove(i)

            fieldset |= subset

        return fieldset

    @classmethod
    def normalize(cls, year, month, dom, hour, minute):
        """ Normalize a datetime. This function can normalize only one of year, month, ... at
            a time, and it can only handle it if this number is at most too big by one. """

        # normalize minute
        if minute==60:
            minute = 0
            hour += 1
        assert minute<60

        # normalize hour
        if hour==24:
            hour = 0
            dom += 1
        assert hour<24

        # normalize dom
        if not month==13:
            first_dow,days_in_month = calendar.monthrange(year, month)
            if dom==days_in_month+1:
                dom = 1
                month += 1

        # normalize month
        if month==13:
            month = 1
            year += 1
        assert month<13

        # assertion for dom
        first_dow,days_in_month = calendar.monthrange(year, month)
        assert dom<=days_in_month

        dow = calendar.weekday(year, month, dom)
        return year, month, dom, hour, minute, dow

    def next_clearance(self, last_clearance):
        year,month,dom,hour,minute,second,dow,doy,isdst = time.gmtime(last_clearance)

        # start with the beginning of the next minute
        minute += 1
        second = 0
        year, month, dom, hour, minute, dow = CronPattern.normalize(year, month, dom, hour, minute)

        while True:
            if month not in self.month:
                # go to beginning of next month
                month += 1
                
                dom=1
                hour=0
                minute=0
                second=0
            elif dom not in self.dom or dow not in self.dow:
                # go to beginning of the next day
                dom += 1
                hour = 0
                minute = 0
                second = 0
            elif hour not in self.hour:
                # go to beginning of the next hour
                hour += 1
                minute = 0
                second = 0
            elif minute not in self.minute:
                # go to beginning of next minute
                minute += 1
                second = 0
            else:
                # everything is fitting, so return this time
                timestamp = calendar.timegm((year,month,dom,hour,minute,second,dow,doy,isdst))
                return timestamp

            # reconvert it to a proper date (i.e. hour=24 -> hour=0)
            year, month, dom, hour, minute, dow = CronPattern.normalize(year, month, dom, hour, minute)

class Job(threading.Thread):
    def __init__(self, pattern, callback, args=(), last_execution=None):
        threading.Thread.__init__(self)

        self.pattern = pattern

        self.callback = callback
        self.args = args
 
        self.last_execution = last_execution

        self.finish = threading.Event()

    def overdue(self, now=time.time()):
        """ This can be called before start() is called to determine
            if calling the callback is overdue. """

        if self.last_execution==None: return True

        clearance = self.pattern.next_clearance(self.last_execution)

        return clearance <= now

    def run(self):
        if self.last_execution==None:
            self.last_execution = self.callback(*self.args)

        while True:
            # calculate the time the callback must be called next
            clearance = self.pattern.next_clearance(self.last_execution)

            # calculate interval
            now = time.time()
            interval = clearance - now
            if interval<=0: interval=0

            # wait 
            self.finish.wait(interval)
            if self.finish.is_set(): return

            # call callback
            self.last_execution = self.callback(*self.args)

    def terminate(self):
        self.finish.set()
        self.join()
