#!/usr/bin/env python

"""
This is an implementation of a simple job scheduler. It accepts either a cron-like syntax (:class:`CronPattern`), or executes regularly at a fixed time interval (:class:`IntervalPattern`).

Example usage::

    def callback(*args):
        # ... do work ...
        return time.time()

    pattern = CronPattern("0-30/5", "4,5", "3", "*", "*")
    job = Job(pattern, callback, arguments)
    job.start()

    # ... callback(arguments) will be executed regularly ...

    job.terminate()

.. note:: All times are UTC.

"""

import time, calendar, threading

class TimePattern:
    """ Abstract base class for time patterns that specify at which times to execute a callback. """

    def next_clearance(self, last_clearance):
        """ Must be overridden in subclasses.

            This method should calculate the time a callback must be executed next,
            given that the last execution was at `last_clearance`.

            :param last_clearance: unix time stamp of last execution
            :type last_clearance: integer
            :rtype: unix time stamp as integer """
        raise NotImplementedError, "override this function"

class IntervalPattern(TimePattern):
    """ This :class:`TimePattern` is for regular execution with fixed time intervals. """

    def __init__(self, interval):
        """ :param interval: interval in seconds
            :type interval: integer """
        self.interval = interval

    def next_clearance(self, last_clearance):
        return last_clearance + self.interval

def parse_field(field, first, last):
    """ Parses one field in cron-like syntax, used by :class:`CronPattern`.
        A valid field consists of a asterisk (matches every value), or a
        comma-separated list of integers and ranges, where ranges are of the
        form 3-5. A slash followed by an integer may be appended to restrict
        values to multiples of this integer.

        Examples: "0-30/5", "3,5-7", "3", "*"

        :param field: The field with the described syntax
        :type field: string
        :param first: first valid value
        :type first: integer
        :param last: last valid value
        :type last: integer
        :rtype: `set()` of integers
    """
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

def normalize(year, month, dom, hour, minute):
    """ Normalizes as datetime, used by :class:`CronPattern`.
        Normalizing means: If one of the arguments is too large by one,
        the excess it carried to the unit of measurement next in size.
        The function has the limitations that it can only handle an excess
        of one, and that only one argument may have an excess.

        :param year: the year
        :type year: integer
        :param month: the month
        :type month: integer
        :param dom: the day of month
        :type dom: integer
        :param hour: the hour
        :type hour: integer
        :param minute: the minute
        :type minute: integer

        :rtype: (year, month, dom, hour, minute) tuple
    """

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

class NoMatchingTimestamp(Exception):
    """ :class:`Exception` which is raised when :meth:`CronPattern.next_clearance` cannot find a matching timestamp """

    def __init__(self):
        s = "No matching timestamp found in a range of MAX_YEARS. You probably specified an invalid pattern."
        Exception.__init__(self, s)

MAX_YEARS = 1000
class CronPattern(TimePattern):
    """ A :class:`TimePattern` which provides a cron-like syntax. """

    def __init__(self, minute, hour, dom, month, dow):
        """ Each parameter must be of the format accepted by :func:`parse_field`,
            e.g. "0-30/5" or "1,3,5" or "*".

            :param minute: execution minute
            :type minute: string
            :param hour: execution hour
            :type hour: string
            :param dom: execution day of month
            :type dom: string
            :param month: execution month
            :type month: string
            :param dow: execution day of week
            :type dow: string
        """
        self.minute = parse_field(minute, 0, 59)
        self.hour = parse_field(hour, 0, 23)
        self.dom = parse_field(dom, 1, 31)
        self.month = parse_field(month, 1, 12)
        self.dow = parse_field(dow, 0, 6)

    def next_clearance(self, last_clearance):
        """ throws a :class:`NoMatchingTimestamp` exception if no timestamp within a range of MAX_YEARS matches. """
        year,month,dom,hour,minute,second,dow,doy,isdst = time.gmtime(last_clearance)

        # to avoid an infinite loop if no time matches the pattern
        stop_year = year + MAX_YEARS

        # start with the beginning of the next minute
        minute += 1
        second = 0
        year, month, dom, hour, minute, dow = normalize(year, month, dom, hour, minute)

        while True:
            if year>stop_year:
                raise NoMatchingTimestamp

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
            year, month, dom, hour, minute, dow = normalize(year, month, dom, hour, minute)

class Job(threading.Thread):
    """ A :class:`threading.Thread` that executes a given callback
        following a given :class:`TimePattern`.
    """

    def __init__(self, pattern, callback, args=(), last_execution=None):
        """ :param pattern: callback execution pattern
            :type pattern: :class:`TimePattern`
            :param callback: must return the unix time stamp for which the execution should be registered
            :type callback: `function`
            :param args: arguments for the callback
            :type args: tuple
            :param last_execution: unix time stamp of the last execution (optional)
            :type last_execution: integer
        """
        threading.Thread.__init__(self)

        self.pattern = pattern

        self.callback = callback
        self.args = args

        self.last_execution = last_execution

        self.finish = threading.Event()

    def overdue(self, reference_timestamp=None):
        """ This can be called before `start()` to determine
            if callback execution is overdue.

            :param reference_timestamp: the timestamp to compare to (optional)
            :type reference_timestamp: integer
            :rtype: boolean
        """

        if reference_timestamp is None:
            reference_timestamp = time.time()

        if self.last_execution is None: return True

        clearance = self.pattern.next_clearance(self.last_execution)

        return clearance <= reference_timestamp

    def run(self):
        if self.last_execution is None:
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
        """ Terminates the :class:`Job` at the next opportunity; blocks until terminating is finished. """
        self.finish.set()
        self.join()
