import unittest

import threading, BaseHTTPServer
import os, tempfile, shutil
import time, math

from sduds import partners
from sduds.constants import *

# example attributes
name = "examplepartner"
accept_password = "examplesecret"
base_url = "http://www.example.org/"
control_probability = 0.1
connection_schedule = "0 0 1 */7 *"
provide_username = "test"
provide_password = "testsecret"

class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def do_GET(self):
        if self.path=="/synchronization_address":
            self.send_response(200)
            self.end_headers()
            self.wfile.write('["www.example.org", 20000]')

class Partner(unittest.TestCase):
    def test_constructor(self):
        """ create Partner instance using the constructor and check that attributes are set correctly """

        # without connection schedule
        partner = partners.Partner(name, accept_password, base_url, control_probability)
        self.assertEqual(partner.name, name)
        self.assertEqual(partner.accept_password, accept_password)
        self.assertEqual(partner.base_url, base_url)
        self.assertEqual(partner.control_probability, control_probability)
        self.assertEqual(partner.last_connection, None)
        self.assertEqual(partner.kicked, False)
        self.assertEqual(partner.connection_schedule, None)
        self.assertEqual(partner.provide_username, None)
        self.assertEqual(partner.provide_password, None)

        # with connection schedule
        partner = partners.Partner(name, accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)
        self.assertEqual(partner.name, name)
        self.assertEqual(partner.accept_password, accept_password)
        self.assertEqual(partner.base_url, base_url)
        self.assertEqual(partner.control_probability, control_probability)
        self.assertEqual(partner.last_connection, None)
        self.assertEqual(partner.kicked, False)
        self.assertEqual(partner.connection_schedule, connection_schedule)
        self.assertEqual(partner.provide_username, provide_username)
        self.assertEqual(partner.provide_password, provide_password)

    def test_get_synchronization_address(self):
        # start a web server handling exactly one request
        address = ("", 0)
        httpd = BaseHTTPServer.HTTPServer(address, RequestHandler)
        thread = threading.Thread(target=httpd.handle_request)
        thread.start()

        address = httpd.socket.getsockname()

        # create partner
        name = "examplepartner"
        accept_password = "examplesecret"
        base_url = "http://"+address[0]+":"+str(address[1])+"/"
        control_probability = 0.1
        partner = partners.Partner(name, accept_password, base_url, control_probability)

        # fetch synchronization address
        host, port = partner.get_synchronization_address()
        thread.join()

        # check synchronization address
        self.assertEqual(type(host), str)
        self.assertEqual(type(port), int)
        self.assertEqual(host, "www.example.org")
        self.assertEqual(port, 20000)

class PartnerDatabase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.mkdtemp() # create temporary directory
        self.addCleanup(shutil.rmtree, directory)

        self.database_path = os.path.join(directory, "partners.sqlite")
        reference_timestamp = 1000000000
        self.database = partners.PartnerDatabase(self.database_path, reference_timestamp)
        self.addCleanup(lambda: self.database.close)

    def test_save_get(self):
        """ saves partner in database and reads it again """

        # save partner
        partner = partners.Partner(name, accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)

        self.database.save_partner(partner)

        # make sure that partner instance is still readable after it was saved to the
        # database (session.add instead of session.merge causes problems)
        read_name = partner.name
        self.assertEqual(read_name, name)

        # close database and open again to make sure instance was persisted
        self.database.close()
        self.database = partners.PartnerDatabase(self.database_path)

        # load partner
        loaded_partner = self.database.get_partner(name)

        # check partner
        self.assertEqual(loaded_partner.name, name)
        self.assertEqual(loaded_partner.accept_password, accept_password)
        self.assertEqual(loaded_partner.base_url, base_url)
        self.assertEqual(loaded_partner.control_probability, control_probability)
        self.assertEqual(loaded_partner.last_connection, None)
        self.assertEqual(loaded_partner.kicked, False)
        self.assertEqual(loaded_partner.connection_schedule, connection_schedule)
        self.assertEqual(loaded_partner.provide_username, provide_username)
        self.assertEqual(loaded_partner.provide_password, provide_password)

    def test_get_invalid(self):
        """ get_partner must return None if partner_name is invalid """

        partner = self.database.get_partner("nonexistant")
        self.assertEqual(partner, None)

    def test_delete_partner(self):
        """ partner must be gone after delete """

        # save partner
        partner = partners.Partner(name, accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)

        self.database.save_partner(partner)

        # delete partner
        self.database.delete_partner(name)

        # read partner
        partner = self.database.get_partner(name)
        self.assertEqual(partner, None)

    def test_delete_partner_invalid(self):
        """ delete_partner must not fail if partner_name is invalid """

        self.database.delete_partner("nonexistant")

    def test_get_partners(self):
        """ get_partners must yield all stored partners """

        # save three partners
        partner = partners.Partner("partner1", accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)
        self.database.save_partner(partner)

        partner = partners.Partner("partner2", accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)
        self.database.save_partner(partner)

        partner = partners.Partner("partner3", accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)
        self.database.save_partner(partner)

        # get partners
        names = set()

        for partner in self.database.get_partners():
            names.add(partner.name)

        # check list
        self.assertEqual(names, set(["partner1", "partner2", "partner3"]))

    def test_register_connection(self):
        """ register_connection must set last_connection attribute """

        # save partner
        partner = partners.Partner(name, accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)

        self.database.save_partner(partner)

        # register connection
        before = int(time.time())
        timestamp = self.database.register_connection(name)
        after = int(time.time())

        self.assertEqual(type(timestamp), int)
        self.assertTrue(before <= timestamp)
        self.assertTrue(timestamp <= after)

        # load partner
        partner = self.database.get_partner(name)

        # check last_connection attribute
        self.assertEqual(timestamp, partner.last_connection)

    def test_register_connection_invalid(self):
        """ register_connection must not throw exception if partner_name is invalid """

        self.database.register_connection("nonexistant")

    def test_register_control_sample(self):
        """ kicked attribute must be set after too many failed control samples """

        # save partner
        partner = partners.Partner(name, accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)

        self.database.save_partner(partner)

        # register control samples
        failed = int(math.ceil(SIGNIFICANCE_THRESHOLD*MAX_FAILED_PERCENTAGE/100.)) + 1
        successful = SIGNIFICANCE_THRESHOLD - failed

        reference_timestamp = 1000000000

        ## successful ones
        for i in xrange(successful):
            success = self.database.register_control_sample(name, reference_timestamp)
            self.assertEqual(success, True)

        ## failed ones
        for i in xrange(failed):
            webfinger_address = "johndoe%d@example.org" % i
            success = self.database.register_control_sample(name, reference_timestamp, webfinger_address)
            self.assertEqual(success, True)

        # load partner
        partner = self.database.get_partner(name)

        # check kicked attribute
        self.assertEqual(partner.kicked, True)

    def test_register_control_sample_invalid(self):
        """ register_control_sample must return False if partner name is invalid """

        reference_timestamp = 1000000000

        # successful one
        success = self.database.register_control_sample(name, reference_timestamp)
        self.assertEqual(success, False)

        # failed one
        address = "johndoe@example.org"
        success = self.database.register_control_sample(name, reference_timestamp, address)
        self.assertEqual(success, False)

    def test_one_address_failed_samples(self):
        """ failed samples produced by one webfinger address may not get the partner kicked """

        # save partner
        partner = partners.Partner(name, accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)

        self.database.save_partner(partner)

        # register control samples
        reference_timestamp = 1000000000
        for i in xrange(SIGNIFICANCE_THRESHOLD):
            self.database.register_control_sample(name, reference_timestamp, "johndoe@example.org")

        # load partner
        partner = self.database.get_partner(name)

        # check kicked attribute
        self.assertEqual(partner.kicked, False)

    def test_register_malformed_state(self):
        """ kicked attribute must be set after violation is registered """

        # save partner
        partner = partners.Partner(name, accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)

        self.database.save_partner(partner)

        # register malformed state
        self.database.register_malformed_state(name)

        # load partner
        partner = self.database.get_partner(name)

        # check kicked attribute
        self.assertEqual(partner.kicked, True)

    def test_unkick_noclear(self):
        """ kicked attribute must be set to False after unkick with the delete_control_samples
            argument set to False, but control samples must be preserved """

        # save partner
        partner = partners.Partner(name, accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)
        self.database.save_partner(partner)

        # register many failed control samples to get the partner kicked
        reference_timestamp = 1000000000
        for i in xrange(SIGNIFICANCE_THRESHOLD):
            webfinger_address = "johndoe%d@example.org" % i
            self.database.register_control_sample(name, reference_timestamp, webfinger_address)

        # load partner and make sure it is kicked
        partner = self.database.get_partner(name)
        self.assertEqual(partner.kicked, True)

        # unkick partner with delete_control_samples set to True
        success = self.database.unkick_partner(name, False)
        self.assertEqual(success, True)

        # make sure partner is not kicked anymore
        partner = self.database.get_partner(name)
        self.assertEqual(partner.kicked, False)

        # register one more failed control sample to check if old ones were preserved
        webfinger_address = "one_more_johndoe@example.org"
        self.database.register_control_sample(name, reference_timestamp, webfinger_address)

        # make sure partner was kicked again
        partner = self.database.get_partner(name)
        self.assertEqual(partner.kicked, True)

    def test_unkick_clear(self):
        """ one more failed control sample may not get the partner kicked again after unkick
            with the delete_control_samples argument set to True """

        # save partner
        partner = partners.Partner(name, accept_password, base_url, control_probability,
                                   connection_schedule, provide_username, provide_password)
        self.database.save_partner(partner)

        # register many failed control samples to get the partner kicked
        reference_timestamp = 1000000000
        for i in xrange(SIGNIFICANCE_THRESHOLD):
            webfinger_address = "johndoe%d@example.org" % i
            self.database.register_control_sample(name, reference_timestamp, webfinger_address)

        # load partner and make sure it is kicked
        partner = self.database.get_partner(name)
        self.assertEqual(partner.kicked, True)

        # unkick partner with delete_control_samples set to True
        success = self.database.unkick_partner(name, True)
        self.assertEqual(success, True)

        # register one more failed control sample
        assert SIGNIFICANCE_THRESHOLD>0, "test fails if significance threshold is too small"
        webfinger_address = "one_more_johndoe@example.org"
        self.database.register_control_sample(name, reference_timestamp, webfinger_address)

        # make sure partner was not kicked
        partner = self.database.get_partner(name)
        self.assertEqual(partner.kicked, False)

    def test_unkick_invalid(self):
        """ unkick_partner must return False if partner name is invalid """

        success = self.database.unkick_partner("nonexistant", True)
        self.assertEqual(success, False)

import sqlalchemy

class ControlSampleCache(unittest.TestCase):
    def setUp(self):
        # create in-memory database
        engine = sqlalchemy.create_engine("sqlite://")

        # create tables for control samples
        partners.DatabaseObject.metadata.create_all(engine)

        # create session
        Session = sqlalchemy.orm.sessionmaker(bind=engine)

        self.first_interval = 0 + CONTROL_SAMPLE_WINDOW - 1
        self.cache = partners.ControlSampleCache(Session, self.first_interval)

        # check CONTROL_SAMPLE_WINDOW
        assert CONTROL_SAMPLE_WINDOW>=2, "testing fails when window is too small"

    def test_successful(self):
        """ successful control sample must be counted as long as it is valid """

        partner_id = 0
        self.cache.add_successful_sample(partner_id, self.first_interval)

        # count while in cache
        count = self.cache.count_successful_samples(partner_id, self.first_interval)
        self.assertEqual(type(count), int)
        self.assertEqual(count, 1)

        # count after commit
        interval = self.first_interval + 1
        count = self.cache.count_successful_samples(partner_id, interval)
        self.assertEqual(count, 1)

        # count shortly before expired
        interval = self.first_interval + CONTROL_SAMPLE_WINDOW - 1
        count = self.cache.count_successful_samples(partner_id, interval)
        self.assertEqual(count, 1)

        # count after expired
        interval = self.first_interval + CONTROL_SAMPLE_WINDOW
        count = self.cache.count_successful_samples(partner_id, interval)
        self.assertEqual(count, 0)

    def test_partial_expire_successful(self):
        """ successful samples count must decrease as samples expire """

        partner_id = 0

        # add control samples, two in each interval
        total_samples = 0

        last_interval = self.first_interval + CONTROL_SAMPLE_WINDOW - 1
        for interval in xrange(self.first_interval, last_interval+1):
            self.cache.add_successful_sample(partner_id, interval)
            self.cache.add_successful_sample(partner_id, interval)
            total_samples += 2

        # watch control samples count decrease while increasing interval
        first_interval = last_interval
        last_interval = first_interval + CONTROL_SAMPLE_WINDOW
        for interval in xrange(first_interval, last_interval+1):
            count = self.cache.count_successful_samples(partner_id, interval)
            self.assertEqual(count, total_samples)

            # two samples expire until the next step
            total_samples -= 2

    def test_failed(self):
        """ failed control sample must be counted as long as it is valid """

        partner_id = 0
        address = "johndoe@example.org"
        self.cache.add_failed_sample(partner_id, self.first_interval, address)

        # count while in cache
        count = self.cache.count_failed_samples(partner_id, self.first_interval)
        self.assertEqual(type(count), int)
        self.assertEqual(count, 1)

        # count after commit
        interval = self.first_interval + 1
        count = self.cache.count_failed_samples(partner_id, interval)
        self.assertEqual(count, 1)

        # count shortly before expired
        interval = self.first_interval + CONTROL_SAMPLE_WINDOW - 1
        count = self.cache.count_failed_samples(partner_id, interval)
        self.assertEqual(count, 1)

        # count after expired
        interval = self.first_interval + CONTROL_SAMPLE_WINDOW
        count = self.cache.count_failed_samples(partner_id, interval)
        self.assertEqual(count, 0)

    def test_failed_count_once(self):
        """ failed control samples must only be counted once per webfinger address """

        partner_id = 0
        address = "johndoe@example.org"

        # add first control sample
        self.cache.add_failed_sample(partner_id, self.first_interval, address)

        # add second control sample shortly before first one expires
        interval = self.first_interval + CONTROL_SAMPLE_WINDOW - 1
        self.cache.add_failed_sample(partner_id, interval, address)

        # count while in cache
        count = self.cache.count_failed_samples(partner_id, interval)
        self.assertEqual(count, 1)

        # count after commit and when first one would already be expired
        interval = self.first_interval + CONTROL_SAMPLE_WINDOW
        count = self.cache.count_failed_samples(partner_id, interval)
        self.assertEqual(count, 1)

    def test_partial_expire_failed(self):
        """ failed samples count must decrease as samples expire """

        partner_id = 0

        # add control samples, two in each interval
        total_samples = 0
        address_counter = 0

        last_interval = self.first_interval + CONTROL_SAMPLE_WINDOW - 1
        for interval in xrange(self.first_interval, last_interval+1):
            address = "johndoe%d@example.org" % address_counter
            address_counter += 1
            self.cache.add_failed_sample(partner_id, interval, address)

            address = "johndoe%d@example.org" % address_counter
            address_counter += 1
            self.cache.add_failed_sample(partner_id, interval, address)

            total_samples += 2

        # watch control samples count decrease while increasing interval
        first_interval = last_interval
        last_interval = first_interval + CONTROL_SAMPLE_WINDOW
        for interval in xrange(first_interval, last_interval+1):
            count = self.cache.count_failed_samples(partner_id, interval)
            self.assertEqual(count, total_samples)

            # two samples expire until the next step
            total_samples -= 2

    def test_clear_cached(self):
        """ samples count must be zero after the clear method was called (count from cache) """

        partner_id = 0
        address = "johndoe@example.org"

        # add one successful and one failed control sample
        self.cache.add_successful_sample(partner_id, self.first_interval)
        self.cache.add_failed_sample(partner_id, self.first_interval, address)

        # call clear method
        self.cache.clear(partner_id)

        # count successful control samples
        count = self.cache.count_successful_samples(partner_id, self.first_interval)
        self.assertEqual(count, 0)

        # count failed control samples
        count = self.cache.count_failed_samples(partner_id, self.first_interval)
        self.assertEqual(count, 0)

    def test_clear_commited(self):
        """ samples count must be zero after the clear method was called (count from database) """

        partner_id = 0
        address = "johndoe@example.org"

        # add one successful and one failed control sample
        self.cache.add_successful_sample(partner_id, self.first_interval)
        self.cache.add_failed_sample(partner_id, self.first_interval, address)

        # call clear method
        self.cache.clear(partner_id)

        # make sure we count commited samples
        interval = self.first_interval + 1

        # count successful control samples
        count = self.cache.count_successful_samples(partner_id, interval)
        self.assertEqual(count, 0)

        # count failed control samples
        count = self.cache.count_failed_samples(partner_id, interval)
        self.assertEqual(count, 0)

if __name__ == '__main__':
    unittest.main()
