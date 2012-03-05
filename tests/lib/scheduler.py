import unittest

from sduds.lib import scheduler
import calendar, time

class IntervalPattern(unittest.TestCase):
    def testClearance(self):
        """ IntervalPattern.next_clearance must return the correct timestamp """

        # invent some last execution date
        last_execution = 1000000000

        # specify pattern -- every 1000 seconds
        interval = 1000
        pattern = scheduler.IntervalPattern(interval)

        # calculate next execution date
        next_execution = pattern.next_clearance(last_execution)

        # make sure difference is correct
        difference = next_execution - last_execution
        self.assertEqual(interval, difference, "IntervalPattern does not return the right clearance time")

class CronPattern(unittest.TestCase):
    def testClearanceValid(self):
        """ test a valid CronPattern """

        # invent some last execution date
        year,month,dom,hour,minute,second,dow,doy,isdst = 2000,1,1, 10,00,01, None,None,None
        last_execution = calendar.timegm((year,month,dom,hour,minute,second,dow,doy,isdst))

        # specify pattern -- every Christmas at 20:15
        minute, hour, dom, month, dow = "15", "20", "24", "12", "*"
        pattern = scheduler.CronPattern(minute, hour, dom, month, dow)

        # calculate next execution date
        next_execution = pattern.next_clearance(last_execution)

        # compare with wanted execution date
        year,month,dom,hour,minute,second,dow,doy,isdst = 2000,12,24, 20,15,0, None,None,None
        wanted_execution = calendar.timegm((year,month,dom,hour,minute,second,dow,doy,isdst))

        self.assertEqual(next_execution, wanted_execution, "CronPattern does not return the right clearance time")

    def testClearanceInvalid(self):
        """ test of an invalid CronPattern -- next_clearance must raise a NoMatchingTimestamp exception """

        # invent some last execution date
        year,month,dom,hour,minute,second,dow,doy,isdst = 2000,1,1, 10,00,01, None,None,None
        last_execution = calendar.timegm((year,month,dom,hour,minute,second,dow,doy,isdst))

        # specify pattern (invalid: november has only 30 days)
        minute, hour, dom, month, dow = "15", "20", "31", "11", "*"
        pattern = scheduler.CronPattern(minute, hour, dom, month, dow)

        # try to calculate next execution date
        with self.assertRaises(scheduler.NoMatchingTimestamp):
            next_execution = pattern.next_clearance(last_execution)

class Job(unittest.TestCase):
    def testOverdue(self):
        """ Job.overdue must return true if next_clearance <= reference_timestamp """

        reference_timestamp = 1000000000

        class MockPattern:
            def next_clearance(self,last_execution):
                return reference_timestamp
        pattern = MockPattern()

        def callback(): return time.time()
        args = ()
        last_execution = reference_timestamp-1

        job = scheduler.Job(pattern, callback, args, last_execution)
        overdue = job.overdue(reference_timestamp)
        self.assertTrue(overdue)

    def testNotOverdue(self):
        """ Job.overdue must return false if next_clearance > reference_timestamp """

        reference_timestamp = 1000000000

        class MockPattern:
            def next_clearance(self,last_execution):
                return reference_timestamp+1
        pattern = MockPattern()

        def callback(): return time.time()
        args = ()
        last_execution = reference_timestamp

        job = scheduler.Job(pattern, callback, args, last_execution)
        overdue = job.overdue(reference_timestamp)
        self.assertFalse(overdue)

if __name__ == '__main__':
    unittest.main()
