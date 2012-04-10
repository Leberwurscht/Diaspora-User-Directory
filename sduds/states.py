#!/usr/bin/env python

import urllib2, json, pywebfinger, binascii, hashlib, time

from constants import *
from lib.signature import signature_valid

class RetrievalFailed(Exception):
    """ Raised by :meth:`Profile.retrieve` if the profile retrieval fails for other reasons than
        connection problems. """
    pass

class CheckFailed(Exception):
    info = None

    def __init__(self, info, message):
        Exception.__init__(self, message)
        self.info = info

    def __str__(self):
        r = self.message+"\n"
        r += "\n"
        r += "Info:\n"
        r += self.info
        return r

class MalformedProfileException(CheckFailed):
    def __str__(self):
        s = CheckFailed.__str__(self)
        return "Profile malformed: %s" % s

class MalformedStateException(CheckFailed):
    def __str__(self):
        s = CheckFailed.__str__(self)
        return "State malformed: %s" % s

class RecentlyExpiredProfileException(Exception):
    info = None

    def __init__(self, info, reference_timestamp):
        message = "Profile recently expired (reference timestamp: %d)" % reference_timestamp
        Exception.__init__(self, message)

        self.info = info

    def __str__(self):
        r = self.message+"\n"
        r += "\n"
        r += "Info:\n"
        r += self.info
        return r

class Profile:
    full_name = None # unicode
    hometown = None # unicode
    country_code = None # str
    services = None # str
    captcha_signature = None # str
    submission_timestamp = None # int

    def __init__(self, full_name, hometown, country_code, services, captcha_signature, submission_timestamp):
        self.full_name = full_name
        self.hometown = hometown
        self.country_code = country_code
        self.services = services
        self.captcha_signature = captcha_signature
        self.submission_timestamp = submission_timestamp

    def __composite_values__(self):
        return self.full_name, self.hometown, self.country_code, self.services, self.captcha_signature, self.submission_timestamp

    def __str__(self):
        s = "Full name: "+self.full_name.encode("utf8")+"\n"+\
            "Hometown: "+self.hometown.encode("utf8")+"\n"+\
            "Country code: "+self.country_code+"\n"+\
            "Services: "+self.services+"\n"+\
            "Captcha signature: "+binascii.hexlify(self.captcha_signature[:8])+"...\n"+\
            "Submission time: "+time.ctime(self.submission_timestamp)

        return s

    def check(self, webfinger_address, reference_timestamp=None, public_key=None):
        """ Validates the profile against a certain webfinger address. Checks CAPTCHA signature,
            submission_timestamp, and field lengths. Also checks whether webfinger address is
            too long. """

        if reference_timestamp is None:
            reference_timestamp = int(time.time())

        if public_key is None:
            public_key = CAPTCHA_PUBLIC_KEY

        # validate CAPTCHA signature for given webfinger address
        if not signature_valid(public_key, self.captcha_signature, webfinger_address):
            raise MalformedProfileException(str(self), "Invalid captcha signature")

        # make sure that submission_timestamp is not in future
        if not self.submission_timestamp <= reference_timestamp:
            raise MalformedProfileException(str(self), "Submitted in future (reference timestamp: %d)" % reference_timestamp)

        # make sure that profile is not expired
        expiry_date = self.submission_timestamp + PROFILE_LIFETIME

        if reference_timestamp>expiry_date:
            if not reference_timestamp < expiry_date + EXPIRY_GRACE_PERIOD:
                raise MalformedProfileException(str(self), "Profile expired (reference timestamp: %d)" % reference_timestamp)

            raise RecentlyExpiredProfileException(str(self), reference_timestamp)

        # check lengths of webfinger address
        if not len(webfinger_address)<=MAX_ADDRESS_LENGTH:
            raise MalformedProfileException(str(self), "Address too long")

        # check lengths of profile fields
        if not len(self.full_name.encode("utf8"))<=MAX_NAME_LENGTH:
            raise MalformedProfileException(str(self), "Full name too long")

        if not len(self.hometown.encode("utf8"))<=MAX_HOMETOWN_LENGTH:
            raise MalformedProfileException(str(self), "Hometown too long")

        if not len(self.country_code)<=MAX_COUNTRY_CODE_LENGTH:
            raise MalformedProfileException(str(self), "Country code too long")

        if not len(self.services)<=MAX_SERVICES_LENGTH:
            raise MalformedProfileException(str(self), "Services list too long")

        for service in self.services.split(","):
            if not len(service)<=MAX_SERVICE_LENGTH:
                raise MalformedProfileException(str(self), "Service %s too long" % service)

        return True

    @classmethod
    def retrieve(cls, address, timeout=None):

        try:
            wf = pywebfinger.finger(address, timeout=timeout)
            sduds_uri = wf.find_link("http://hoegners.de/sduds/spec", attr="href")
        except IOError:
            raise
        except Exception, e:
            raise RetrievalFailed("Could not get the sduds URL from the webfinger profile: %s" % str(e))

        try:
            f = urllib2.urlopen(sduds_uri, timeout=timeout)
            json_string = f.read()
            f.close()

            json_dict = json.loads(json_string)
        except IOError:
            raise
        except Exception, e:
            raise RetrievalFailed("Could not load the sduds document specified in the profile: %s" % str(e))

        try:
            specified_address = json_dict["webfinger_address"]
        except KeyError:
            raise RetrievalFailed("Document does not contain a webfinger_address field.")

        if not specified_address==address:
            raise RetrievalFailed("Profile does not contain the specified address.")

        try:
            full_name = json_dict["full_name"]
            hometown = json_dict["hometown"]
            country_code = json_dict["country_code"].encode("utf8")
            services = json_dict["services"].encode("utf8")
            captcha_signature = binascii.unhexlify(json_dict["captcha_signature"])

            submission_timestamp = int(json_dict["submission_timestamp"])
        except Exception, e:
            raise RetrievalFailed("Unable to extract profile information: %s" % str(e))

        profile = cls(
            full_name,
            hometown,
            country_code,
            services,
            captcha_signature,
            submission_timestamp
        )

        return profile

class State(object):
    address = None
    retrieval_timestamp = None
    profile = None

    def __init__(self, address, retrieval_timestamp, profile):
        self.address = address
        self.retrieval_timestamp = retrieval_timestamp
        self.profile = profile

    def __eq__(self, other):
        assert self.retrieval_timestamp is not None
        assert other.retrieval_timestamp is not None

        assert self.address==other.address

        if self.profile and other.profile:
            return self.hash==other.hash
        elif not self.profile and not other.profile:
            return True
        else:
            return False

    def __str__(self):
        s = "Webfinger address: "+self.address+"\n"
        s += "Retrieval time: "+time.ctime(self.retrieval_timestamp)+"\n"

        if self.profile is not None:
            s += "Hash: "+binascii.hexlify(self.hash)+"\n"

        s += "PROFILE:\n"
        s += str(self.profile)

        return s

    def check(self, reference_timestamp=None):
        """ Checks if a state was valid at a given time. Returns True if it was, raises
            an exception otherwise. """

        assert self.retrieval_timestamp is not None

        if reference_timestamp is None:
            reference_timestamp = time.time()

        # make sure retrieval timestamp is not in future
        if not self.retrieval_timestamp <= reference_timestamp:
            raise MalformedStateException(str(self), "Retrieved in future (reference_timestamp: %d)" % reference_timestamp)

        # make sure retrieval_timestamp is up-to-date
        if not self.retrieval_timestamp >= reference_timestamp - MAX_AGE:
            raise MalformedStateException(str(self), "Not up to date (reference timestamp: %d)" % reference_timestamp)

        if self.profile:
            # check profile for valid-up-to-date states
            self.profile.check(self.address, reference_timestamp)

            # make sure retrieval_timestamp is not before submission timestamp
            if not self.retrieval_timestamp>=self.profile.submission_timestamp:
                raise MalformedStateException(str(self), "Retrieval time lies before submission time")

        return True

    @classmethod
    def retrieve(cls, address, timeout=None):
        try:
            profile = Profile.retrieve(address, timeout)
        except (RetrievalFailed, IOError), e:
            # TODO: logging
            profile = None

        retrieval_timestamp = int(time.time())

        state = cls(address, retrieval_timestamp, profile)

        return state

    @property
    def hash(self):
        assert self.profile is not None

        combinedhash = hashlib.sha1()

        relevant_data = [self.address, self.profile.full_name,
            self.profile.hometown, self.profile.country_code,
            self.profile.services, int(self.profile.submission_timestamp)]

        for data in relevant_data:
            # convert data to string
            if type(data)==unicode:
                data_str = data.encode("utf8")
            else:
                data_str = str(data)

            # TODO: take better hash function? (also for combinedhash)
            subhash = hashlib.sha1(data_str).digest()
            combinedhash.update(subhash)

        # TODO: is it unsecure to take only 16 bytes of the hash?
        binhash = combinedhash.digest()[:16]
        return binhash

class Ghost(object):
    hash = None
    retrieval_timestamp = None

    def __init__(self, binhash, retrieval_timestamp):
        self.hash = binhash
        self.retrieval_timestamp = retrieval_timestamp
