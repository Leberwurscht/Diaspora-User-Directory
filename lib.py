#!/usr/bin/env python

###
# sqlalchemy extensions

import sqlalchemy, sqlalchemy.orm

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

class CalculatedPropertyExtension(sqlalchemy.orm.MapperExtension):
    """ helper class to get sqlalchemy mappings of calculated properties
        (see http://stackoverflow.com/questions/3020394/sqlalchemy-how-to-map-against-a-read-only-or-calculated-property) """
    def __init__(self, properties):
        self.properties = properties

    def _update_properties(self, instance):
        for prop, synonym in self.properties.iteritems():
            value = getattr(instance, prop)
            setattr(instance, synonym, value)

    def before_insert(self, mapper, connection, instance):
        self._update_properties(instance)

    def before_update(self, mapper, connection, instance):
        self._update_properties(instance)

###
# Authentication functionality

import SocketServer

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
        if password==None:
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

        self.handle_partner(username)

    def get_password(self, username):
        raise NotImplementedError, "Override this function in subclasses!"

    def handle_user(self, username):
        raise NotImplementedError, "Override this function in subclasses!"

###
# BaseServer

class BaseServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    """ a threading TCPServer with allow_reuse_address """

    allow_reuse_address = True

    def __init__(self, address, handler_class):
        SocketServer.TCPServer.__init__(self, address, handler_class)

    def terminate(self):
        self.shutdown()
        self.socket.close()

###
# signature checking

import base64, paramiko

def signature_valid(public_key_base64, signature, text):
    data = base64.decodestring(public_key_base64)
    public_key = paramiko.RSAKey(data=data)

    sig_message = paramiko.Message()
    sig_message.add_string("ssh-rsa")
    sig_message.add_string(signature)
    sig_message.rewind()

    return public_key.verify_ssh_sig(text, sig_message)

###
# Implementation of a cron-like scheduler

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

class TimePattern:
    def next_clearance(self, last_clearance):
        """ Must calculate the time a callback must be executed next,
            given that the last execution was at last_clearance.
            last_clearance must be a unix time stamp, as well as
            the return value.  """
        raise NotImplementedError, "override this function"

class IntervalPattern(TimePattern):
    def __init__(self, interval):
        self.interval = interval

    def next_clearance(self, last_clearance):
        return last_clearance + self.interval

class CronPattern(TimePattern):
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
