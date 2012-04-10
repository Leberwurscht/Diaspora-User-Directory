import unittest

from sduds import states
from sduds.constants import *

# simulate signatures of the CAPTCHA provider
from sduds.lib.signature import sign

private_key_block = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAyxhRjXXXmTxI3c8IqAsbw+idaXfwWkkiVE0/9jn1oVFdYsIQ
qm+7rkdcjVPa8zJnoYPYupCbMX0TB7hIrLOfQcQzb9PRLZ9KSCbY6Q7tShSylOO9
aaNtG2Q+iHvpckNFp/dThdUDK7YqcYcPtQQFVsDPToehrbbCvHZm2wHRB614u8jZ
VXe+jnxmxFxdTIg2TxICbqHc3OAb2w8FS62U5yI5x/dZS1zVNW0exdci7BZYOZv/
5xw5dd2zsQxiXA5n/Hs+F6Xn7LUKBh6cqEkwuvvQhoO9ieDt5V6nzJPJMHKZtW7T
FYZKt3C/3wtoHOPSsZMUVvIcSKjRHd5xOddJvQIBIwKCAQEAi0PgJnyxGJ5d2e0N
QAeeAq4iy/p47XP6SG98URHNAOdV+pOzqBIaS56l3UDQpsN6Qt4Q9PVxu4j3GzyJ
mv7Tms+uPg2WwDK2l+AftcEXvcUMvd3+Oc8mPqsjkMn/Kce6vFHS321+hF+oE1VM
mWHXxnWVd612LfmqGtTY0LDJ2V/JSvVGRtr/a4wAi9KDn/XQXiFDtry47WgM5bnE
vJ97/UAU3SaTqnEQ1iL7xA9G8Plx4Khzux7Rv1hWZdtAD9FXgUjqXu33tOuZk3cS
GCAzfZHqN5AoGxl14KraxaMWAODH/HD3aZfH/rPHoOQg3bNKcyqqYeagGy58zZtv
Ao29iwKBgQDutEhuipiD/reAPEDV8D8CtGGfGAT/lqULc4seEikyR2OvhYrtaAN/
tDTGWzdz0Z0Q2Y6q/E1UurEngYRyIi4N9s4bMgJKAWCXi9aeie11O0QUTFJM1+al
y4Xer+LjNw0rKOBEzj8sr8VIloKkz+jPcjmS/Cf4UcvCHnLyEi5grQKBgQDZz4Vk
TjUwvZi7EYiHaFgOZr/glsmk7ANS7dT9pnEjI8EWf+l/NXRySO/+l168+w2BTcJW
z0Hy2XZjhagyzXm6aSLNGzM/WNIygzpZEIIbE0LxU9SLinUm3N+fZyMQjXL1kb8l
d/RvtDqe0zwTXwA2rLtI4VnPzzlDqOjSmUTfUQKBgQC4JKznj3z4HERqPRwSwKWj
AC4NA+aZSFNvO+BZBrIQ1/xxdaWv0+VxJJ29ltMBkxLD2weoeX174HoIiHwdiBTm
M2vL1h8F484rw6WQPoP7WZrrFk4eBaNMsvI+Eqe2mC665QTHXUatcabRmK3s2ubL
6mbtuzTG4AOVv7fCDgaFFwKBgBKrYzR7u2qT6IUQIaU01FkBfikxfv+B8agFwcyZ
POXBPG+kkFtcWnAyIzMUSfLw8odtEKha6GVF1vKWbYCyhsbVz8hwC7T4/BL1TiTk
KGi4ghSvaf1VArowMGy/soxj5Ug/sUxavS4lZBw9/dXGUHm3CL0aoUxTlzGvZGnS
n4DbAoGBAJOX+2yWGdOmGEM2AVQHeLL0ramQyekZ0GiVFCgi8fL6sO5XhbYoG4r5
VzxTYB161OPL0CjoYxTx15g+aS0wS39xxoQ6iEW0WqKxxUA03TzQcSNkT9rvV3Bm
UpISaDxaSb8WXkNyUQ7ph/lkcmXCtqFvrdYmqTynBFWbE5nXsyrU
-----END RSA PRIVATE KEY-----"""
public_key_base64 = "AAAAB3NzaC1yc2EAAAABIwAAAQEAyxhRjXXXmTxI3c8IqAsbw+idaXfwWkkiVE0/9jn1oVFdYsIQqm+7rkdcjVPa8zJnoYPYupCbMX0TB7hIrLOfQcQzb9PRLZ9KSCbY6Q7tShSylOO9aaNtG2Q+iHvpckNFp/dThdUDK7YqcYcPtQQFVsDPToehrbbCvHZm2wHRB614u8jZVXe+jnxmxFxdTIg2TxICbqHc3OAb2w8FS62U5yI5x/dZS1zVNW0exdci7BZYOZv/5xw5dd2zsQxiXA5n/Hs+F6Xn7LUKBh6cqEkwuvvQhoO9ieDt5V6nzJPJMHKZtW7TFYZKt3C/3wtoHOPSsZMUVvIcSKjRHd5xOddJvQ=="

# example attributes for Profile, exploiting maximal lengths
address = "johndoe@example.org"
full_name = u"John Doe"
hometown = u"Los Angeles"
country_code = "US"
services = "friendica,email,diaspora"
captcha_signature = sign(private_key_block, address)
submission_timestamp = 1000000000

class Profile(unittest.TestCase):
    def test_constructor(self):
        """ create Profile instance using the constructor and check that attributes are set correctly """

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)
        self.assertEqual(profile.full_name, full_name)
        self.assertEqual(profile.hometown, hometown)
        self.assertEqual(profile.country_code, country_code)
        self.assertEqual(profile.services, services)
        self.assertEqual(profile.captcha_signature, captcha_signature)
        self.assertEqual(profile.submission_timestamp, submission_timestamp)

    def test_check_okay(self):
        """ check() must return True if profile is okay """

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        try:
            success = profile.check(address, submission_timestamp, public_key_base64)
        except Exception, e:
            self.fail(str(e))

        self.assertEqual(success, True)

    def test_check_captcha_signature(self):
        """ check() must raise MalformedProfileException if CAPTCHA signature is invalid """

        invalid_signature = sign(private_key_block, "another_address@example.org")
        profile = states.Profile(full_name, hometown, country_code, services, invalid_signature, submission_timestamp)

        with self.assertRaises(states.MalformedProfileException):
            profile.check(address, submission_timestamp, public_key_base64)

    def test_check_future_submission_timestamp(self):
        """ check() must raise MalformedProfileException if submission timestamp is in future """

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        reference_timestamp = submission_timestamp - 1
        with self.assertRaises(states.MalformedProfileException):
            profile.check(address, reference_timestamp, public_key_base64)

    def test_check_expired(self):
        """ check() must raise MalformedProfileException for expired and RecentlyExpiredProfileException
            for recently expired profiles """

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        # test profile that is not yet expired
        reference_timestamp = submission_timestamp + PROFILE_LIFETIME
        try:
            profile.check(address, reference_timestamp, public_key_base64)
        except Exception, e:
            self.fail(str(e))

        # test profiles with age in the recently expired time range
        reference_timestamp = submission_timestamp + PROFILE_LIFETIME + 1
        with self.assertRaises(states.RecentlyExpiredProfileException):
            profile.check(address, reference_timestamp, public_key_base64)

        reference_timestamp = submission_timestamp + PROFILE_LIFETIME + EXPIRY_GRACE_PERIOD - 1
        with self.assertRaises(states.RecentlyExpiredProfileException):
            profile.check(address, reference_timestamp, public_key_base64)

        # test profile with age exceeding grace period
        reference_timestamp = submission_timestamp + PROFILE_LIFETIME + EXPIRY_GRACE_PERIOD
        with self.assertRaises(states.MalformedProfileException):
            profile.check(address, reference_timestamp, public_key_base64)

    def test_check_address_length(self):
        """ check() must raise MalformedProfileException if address is too long """

        # test address with maximal length
        long_address = "X"*(MAX_ADDRESS_LENGTH - len(address)) + address
        assert len(long_address)==MAX_ADDRESS_LENGTH

        captcha_signature = sign(private_key_block, long_address)
        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        try:
            profile.check(long_address, submission_timestamp, public_key_base64)
        except states.MalformedProfileException, e:
            self.fail(str(e))

        # test address that is too long
        long_address = "X" + long_address
        assert len(long_address)>MAX_ADDRESS_LENGTH

        captcha_signature = sign(private_key_block, long_address)
        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        with self.assertRaises(states.MalformedProfileException):
            profile.check(long_address, submission_timestamp, public_key_base64)

    def test_check_name_length(self):
        """ check() must raise MalformedProfileException if full name is too long """

        # test name with maximal length
        long_name = "X"*MAX_NAME_LENGTH
        profile = states.Profile(long_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        try:
            profile.check(address, submission_timestamp, public_key_base64)
        except states.MalformedProfileException, e:
            self.fail(str(e))

        # test name that is too long
        long_name = "X"*MAX_NAME_LENGTH + "X"
        profile = states.Profile(long_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        with self.assertRaises(states.MalformedProfileException):
            profile.check(address, submission_timestamp, public_key_base64)

    def test_check_hometown_length(self):
        """ check() must raise MalformedProfileException if hometown is too long """

        # test hometown with maximal length
        long_hometown = "X"*MAX_HOMETOWN_LENGTH
        profile = states.Profile(full_name, long_hometown, country_code, services, captcha_signature, submission_timestamp)

        try:
            profile.check(address, submission_timestamp, public_key_base64)
        except states.MalformedProfileException, e:
            self.fail(str(e))

        # test hometown that is too long
        long_hometown = "X"*MAX_HOMETOWN_LENGTH + "X"
        profile = states.Profile(full_name, long_hometown, country_code, services, captcha_signature, submission_timestamp)

        with self.assertRaises(states.MalformedProfileException):
            profile.check(address, submission_timestamp, public_key_base64)

    def test_check_country_code_length(self):
        """ check() must raise MalformedProfileException if country code is too long """

        # test country_code with maximal length
        long_country_code = "X"*MAX_COUNTRY_CODE_LENGTH
        profile = states.Profile(full_name, hometown, long_country_code, services, captcha_signature, submission_timestamp)

        try:
            profile.check(address, submission_timestamp, public_key_base64)
        except states.MalformedProfileException, e:
            self.fail(str(e))

        # test country_code that is too long
        long_country_code = "X"*MAX_COUNTRY_CODE_LENGTH + "X"
        profile = states.Profile(full_name, hometown, long_country_code, services, captcha_signature, submission_timestamp)

        with self.assertRaises(states.MalformedProfileException):
            profile.check(address, submission_timestamp, public_key_base64)

    def test_check_services_length(self):
        """ check() must raise MalformedProfileException if services field or a service string is too long """

        # test service field with maximal total and per-service length
        long_service = "X"*MAX_SERVICE_LENGTH

        services = ""
        while len(services)<MAX_SERVICES_LENGTH:
            services += long_service+","
        services = services[:MAX_SERVICES_LENGTH]

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        try:
            profile.check(address, submission_timestamp, public_key_base64)
        except states.MalformedProfileException, e:
            self.fail(str(e))

        # test service field that is too long
        services = ""
        while len(services)<=MAX_SERVICES_LENGTH:
            services += long_service+","
        services = services[:MAX_SERVICES_LENGTH+1]

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        with self.assertRaises(states.MalformedProfileException):
            profile.check(address, submission_timestamp, public_key_base64)

        # test service field with a service that is too long
        services = long_service + "X"
        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        with self.assertRaises(states.MalformedProfileException):
            profile.check(address, submission_timestamp, public_key_base64)

import BaseHTTPServer, threading, time
import urlparse, urllib, json, binascii

class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def host_meta(self):
        # send headers
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/xml")
        self.end_headers()

        # write XRD document
        self.wfile.write("<?xml version='1.0' encoding='UTF-8'?>")
        self.wfile.write("<XRD xmlns='http://docs.oasis-open.org/ns/xri/xrd-1.0' xmlns:hm='http://host-meta.net/xrd/1.0'>")
        self.wfile.write("<hm:Host>%s</hm:Host>" % self.server.profile_host)
        self.wfile.write("<Link rel='lrdd' template='http://%s/describe?uri={uri}'>" % self.server.profile_host)
        self.wfile.write("<Title>Resource Descriptor</Title>")
        self.wfile.write("</Link>")
        self.wfile.write("</XRD>")

    def describe(self):
        # parse query string
        querystring = urlparse.urlparse(self.path).query
        args = urlparse.parse_qs(querystring)
        uri, = args["uri"]

        # send headers
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/xml")
        self.end_headers()

        # write XRD document
        self.wfile.write("<?xml version='1.0' encoding='UTF-8'?>")
        self.wfile.write("<XRD xmlns='http://docs.oasis-open.org/ns/xri/xrd-1.0'>")
        self.wfile.write("<Subject>%s</Subject>" % uri)
        document_url = "http://%s/document?%s" % (self.server.profile_host, urllib.urlencode({"uri":uri}))
        self.wfile.write("<Link rel='http://hoegners.de/sduds/spec' href='%s' />" % document_url)
        self.wfile.write("</XRD>")

    def json_document(self):
        # parse query string
        querystring = urlparse.urlparse(self.path).query
        args = urlparse.parse_qs(querystring)
        uri, = args["uri"]

        webfinger_address = uri.split("acct:",1)[-1]

        # send headers
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/json")
        self.end_headers()

        # create JSON document
        hex_signature = binascii.hexlify(self.server.profile.captcha_signature)

        document = {
            "webfinger_address": self.server.profile_address,
            "full_name": self.server.profile.full_name,
            "hometown": self.server.profile.hometown,
            "country_code": self.server.profile.country_code,
            "services": self.server.profile.services,
            "captcha_signature": hex_signature,
            "submission_timestamp": self.server.profile.submission_timestamp
        }

        # write JSON document
        json_string = json.dumps(document)
        self.wfile.write(json_string)

    def do_GET(self):
        if self.path=="/.well-known/host-meta":
            self.host_meta()
        elif self.path.startswith("/describe"):
            self.describe()
        elif self.path.startswith("/document"):
            self.json_document()

class InvalidHostMetaRequestHandler(RequestHandler):
    def host_meta(self):
        # send headers
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/xml")
        self.end_headers()

        # write invalid XRD document
        self.wfile.write("--- GARBAGE ---")

class InvalidDescribeRequestHandler(RequestHandler):
    def describe(self):
        # send headers
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/xml")
        self.end_headers()

        # write invalid XRD document
        self.wfile.write("--- GARBAGE ---")

class InvalidDocumentRequestHandler(RequestHandler):
    def json_document(self):
        # send headers
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/json")
        self.end_headers()

        # write invalid JSON document
        self.wfile.write("--- GARBAGE ---")

class TimingOutHostMetaRequestHandler(RequestHandler):
    def host_meta(self):
        # send headers
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/xml")
        self.end_headers()

        # write beginning of XRD document
        self.wfile.write("<?xml version='1.0' encoding='UTF-8'?>")

        # wait until stop event is set
        self.server.stopEvent.wait()

class TimingOutDescribeRequestHandler(RequestHandler):
    def describe(self):
        # send headers
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/xml")
        self.end_headers()

        # write beginning of XRD document
        self.wfile.write("<?xml version='1.0' encoding='UTF-8'?>")

        # wait until stop event is set
        self.server.stopEvent.wait()

class TimingOutDocumentRequestHandler(RequestHandler):
    def json_document(self):
        # send headers
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/json")
        self.end_headers()

        # write beginning of JSON document
        self.wfile.write("{")

        # wait until stop event is set
        self.server.stopEvent.wait()

class RetrieveTestCase(unittest.TestCase):
    def setup_server(self, handler):
        # start a web server
        address = ("", 0)
        httpd = BaseHTTPServer.HTTPServer(address, handler)
        thread = threading.Thread(target=httpd.serve_forever)
        thread.start()

        self.addCleanup(thread.join)
        self.addCleanup(httpd.socket.close)
        self.addCleanup(httpd.shutdown)

        # define a profile the server should serve
        host, port = httpd.socket.getsockname()
        profile_host = "%s:%d" % (host, port)
        self.address = "johndoe@%s" % profile_host
        self.captcha_signature = sign(private_key_block, self.address)
        profile = states.Profile(full_name, hometown, country_code, services, self.captcha_signature, submission_timestamp)

        httpd.profile_host = profile_host
        httpd.profile_address = self.address
        httpd.profile = profile

        self.httpd = httpd

class ProfileRetrieve(RetrieveTestCase):
    def test_successful(self):
        """ attributes of profile returned by Profile.retrieve must be correct """

        self.setup_server(RequestHandler)

        # retrieve profile
        profile = states.Profile.retrieve(self.address)

        # check attributes
        self.assertEqual(type(profile.full_name), unicode)
        self.assertEqual(profile.full_name, full_name)

        self.assertEqual(type(profile.hometown), unicode)
        self.assertEqual(profile.hometown, hometown)

        self.assertEqual(type(profile.country_code), str)
        self.assertEqual(profile.country_code, country_code)

        self.assertEqual(type(profile.services), str)
        self.assertEqual(profile.services, services)

        self.assertEqual(type(profile.captcha_signature), str)
        self.assertEqual(profile.captcha_signature, self.captcha_signature)

        self.assertEqual(type(profile.submission_timestamp), int)
        self.assertEqual(profile.submission_timestamp, submission_timestamp)

    def test_invalid_host_meta(self):
        """ Profile.retrieve must raise RetrievalFailed if host meta is invalid """

        self.setup_server(InvalidHostMetaRequestHandler)

        with self.assertRaises(states.RetrievalFailed):
            profile = states.Profile.retrieve(self.address)

    def test_invalid_describe(self):
        """ Profile.retrieve must raise RetrievalFailed if lrdd document is invalid """

        self.setup_server(InvalidDescribeRequestHandler)

        with self.assertRaises(states.RetrievalFailed):
            profile = states.Profile.retrieve(self.address)

    def test_invalid_document(self):
        """ Profile.retrieve must raise RetrievalFailed if JSON document is invalid """

        self.setup_server(InvalidDocumentRequestHandler)

        with self.assertRaises(states.RetrievalFailed):
            profile = states.Profile.retrieve(self.address)

    def test_timingout_host_meta(self):
        """ Profile.retrieve must raise IOError if reading host meta times out """

        self.setup_server(TimingOutHostMetaRequestHandler)
        self.httpd.stopEvent = threading.Event()

        with self.assertRaises(IOError):
            profile = states.Profile.retrieve(self.address, timeout=0.01)

        self.httpd.stopEvent.set()

    def test_timingout_describe(self):
        """ Profile.retrieve must raise IOError if reading lrdd document times out """

        self.setup_server(TimingOutDescribeRequestHandler)
        self.httpd.stopEvent = threading.Event()

        with self.assertRaises(IOError):
            profile = states.Profile.retrieve(self.address, timeout=0.01)

        self.httpd.stopEvent.set()

    def test_timingout_document(self):
        """ Profile.retrieve must raise IOError if reading JSON document times out """

        self.setup_server(TimingOutDocumentRequestHandler)
        self.httpd.stopEvent = threading.Event()

        with self.assertRaises(IOError):
            profile = states.Profile.retrieve(self.address, timeout=0.01)

        self.httpd.stopEvent.set()

class State(unittest.TestCase):
    def test_constructor(self):
        """ create State instance using the constructor and check that attributes are set correctly """

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)
        retrieval_timestamp = submission_timestamp

        state = states.State(address, retrieval_timestamp, profile)

        self.assertEqual(state.address, address)
        self.assertEqual(state.retrieval_timestamp, retrieval_timestamp)
        self.assertEqual(state.profile.captcha_signature, captcha_signature)

    def test_check_valid(self):
        """ check() must return True if a valid state is okay """

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)
        retrieval_timestamp = submission_timestamp
        state = states.State(address, retrieval_timestamp, profile)

        try:
            success = state.check(retrieval_timestamp)
        except Exception, e:
            self.fail(str(e))

        self.assertEqual(success, True)

    def test_check_invalid(self):
        """ check() must return True if an invalid state is okay """

        retrieval_timestamp = submission_timestamp
        state = states.State(address, retrieval_timestamp, None)

        try:
            success = state.check(retrieval_timestamp)
        except Exception, e:
            self.fail(str(e))

        self.assertEqual(success, True)

    def test_check_future_retrieval_timestamp(self):
        """ check() must raise MalformedStateException if retrieval timestamp is in future """

        reference_timestamp = submission_timestamp + 1
        retrieval_timestamp = submission_timestamp + 2

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)
        state = states.State(address, retrieval_timestamp, profile)

        with self.assertRaises(states.MalformedStateException):
            state.check(reference_timestamp)

    def test_check_max_age(self):
        """ check() must raise MalformedStateException if retrieval timestamp too old """

        assert MAX_AGE<PROFILE_LIFETIME, "test fails if PROFILE_LIFETIME is shorter than MAX_AGE"

        retrieval_timestamp = submission_timestamp
        state = states.State(address, retrieval_timestamp, None)

        # test state that is not yet too old
        reference_timestamp = submission_timestamp + MAX_AGE
        try:
            success = state.check(reference_timestamp)
        except Exception, e:
            self.fail(str(e))

        self.assertEqual(success, True)

        # test state that is too old
        reference_timestamp = submission_timestamp + MAX_AGE + 1
        with self.assertRaises(states.MalformedStateException):
            state.check(reference_timestamp)

    def test_check_profile(self):
        """ check() must raise MalformedProfileException if the profile of a valid-up-to-date state is malformed. """

        invalid_signature = sign(private_key_block, "another_address@example.org")
        malformed_profile = states.Profile(full_name, hometown, country_code, services, invalid_signature, submission_timestamp)

        retrieval_timestamp = submission_timestamp
        state = states.State(address, retrieval_timestamp, malformed_profile)

        with self.assertRaises(states.MalformedProfileException):
            reference_timestamp = submission_timestamp
            state.check(reference_timestamp)

    def test_check_time_order(self):
        """ check() must raise MalformedStateException if retrieval_timestamp is before profile.submission_timestamp """

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)

        retrieval_timestamp = submission_timestamp - 1
        state = states.State(address, retrieval_timestamp, profile)

        with self.assertRaises(states.MalformedStateException):
            reference_timestamp = submission_timestamp
            state.check(reference_timestamp)

    def test_hash(self):
        """ State.hash must change if address or profile is changed """

        profile = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp)
        retrieval_timestamp = submission_timestamp
        state = states.State(address, retrieval_timestamp, profile)

        # hash must not change if retrieval timestamp changes
        retrieval_timestamp2 = retrieval_timestamp + 1
        state2 = states.State(address, retrieval_timestamp2, profile)
        self.assertEqual(state.hash, state2.hash)

        # hash must change if address changes
        address2 = "other_address@example.org"
        captcha_signature2 = sign(private_key_block, address2)
        profile2 = states.Profile(full_name, hometown, country_code, services, captcha_signature2, submission_timestamp)
        state2 = states.State(address2, retrieval_timestamp, profile2)
        self.assertNotEqual(state.hash, state2.hash)

        # hash must change if name changes
        full_name2 = u"Other name"
        profile2 = states.Profile(full_name2, hometown, country_code, services, captcha_signature, submission_timestamp)
        state2 = states.State(address, retrieval_timestamp, profile2)
        self.assertNotEqual(state.hash, state2.hash)

        # hash must change if hometown changes
        hometown2 = u"Other hometown"
        profile2 = states.Profile(full_name, hometown2, country_code, services, captcha_signature, submission_timestamp)
        state2 = states.State(address, retrieval_timestamp, profile2)
        self.assertNotEqual(state.hash, state2.hash)

        # hash must change if country code changes
        country_code2 = "ES"
        profile2 = states.Profile(full_name, hometown, country_code2, services, captcha_signature, submission_timestamp)
        state2 = states.State(address, retrieval_timestamp, profile2)
        self.assertNotEqual(state.hash, state2.hash)

        # hash must change if services changes
        services2 = "otherservice1,otherservice2"
        profile2 = states.Profile(full_name, hometown, country_code, services2, captcha_signature, submission_timestamp)
        state2 = states.State(address, retrieval_timestamp, profile2)
        self.assertNotEqual(state.hash, state2.hash)

        # hash must change if submission timestamp changes
        submission_timestamp2 = submission_timestamp + 1
        profile2 = states.Profile(full_name, hometown, country_code, services, captcha_signature, submission_timestamp2)
        state2 = states.State(address, retrieval_timestamp, profile2)
        self.assertNotEqual(state.hash, state2.hash)

class StateRetrieve(RetrieveTestCase):
    def test_successful(self):
        """ attributes of the state returned by State.retrieve must be correct """

        self.setup_server(RequestHandler)

        # retrieve profile
        before = int(time.time())
        state = states.State.retrieve(self.address)
        after = int(time.time())

        # check retrieval timestamp
        self.assertEqual(type(state.retrieval_timestamp), int)
        self.assertTrue(before<=state.retrieval_timestamp)
        self.assertTrue(state.retrieval_timestamp<=after)

        # check address
        self.assertEqual(type(state.address), str)
        self.assertEqual(state.address, self.address)

        # check profile
        self.assertIsInstance(state.profile, states.Profile)
        self.assertEqual(state.profile.captcha_signature, self.captcha_signature)

    def test_invalid_document(self):
        """ State.retrieve() must construct invalid state if JSON document is invalid """

        self.setup_server(InvalidDocumentRequestHandler)

        # retrieve profile
        state = states.State.retrieve(self.address)

        # make sure state is invalid
        self.assertNotEqual(state.retrieval_timestamp, None)
        self.assertEqual(state.profile, None)

    def test_timingout_document(self):
        """ State.retrieve() must construct invalid state if reading JSON document times out """

        self.setup_server(TimingOutDocumentRequestHandler)
        self.httpd.stopEvent = threading.Event()

        # retrieve profile
        state = states.State.retrieve(self.address, timeout=0.01)
        self.httpd.stopEvent.set()

        # make sure state is invalid
        self.assertNotEqual(state.retrieval_timestamp, None)
        self.assertEqual(state.profile, None)

class Ghost(unittest.TestCase):
    def test_constructor(self):
        """ attributes must be set correctly by Ghost constructor """

        binhash = binascii.unhexlify("001122334455667788001122334455667788")
        retrieval_timestamp = 1000000000

        ghost = states.Ghost(binhash, retrieval_timestamp)

        self.assertEqual(ghost.hash, binhash)
        self.assertEqual(ghost.retrieval_timestamp, retrieval_timestamp)

if __name__ == '__main__':
    unittest.main()
