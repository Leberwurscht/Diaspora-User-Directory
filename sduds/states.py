#!/usr/bin/env python

import urllib2, json, pywebfinger, binascii, hashlib, time

from constants import *
from lib.signature import signature_valid

class RetrievalFailed(Exception):
    """ Raised by :meth:`Profile.retrieve` if the profile retrieval fails for other reasons than
        connection problems. """
    pass

class CheckFailed(Exception):
    """ Base class for exceptions raised by :meth:`Profile.check` and :meth:`State.check`.
        Subclasses of this exception are raised if the :class:`~sduds.partners.Partner` transmits a severely
        malformed profile or state which should get this partner kicked.
    """

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
    """ Subclass of :class:`CheckFailed` which is raised by :meth:`Profile.check` or
        :meth:`State.check`, which calls the former. Used if the profile is malformed,
        so that the partner that transmitted it should be kicked. """

    def __str__(self):
        s = CheckFailed.__str__(self)
        return "Profile malformed: %s" % s

class MalformedStateException(CheckFailed):
    """ Subclass of :class:`CheckFailed` which is raised by :meth:`State.check`. Used if the
        state is severely malformed, so that the partner that transmitted it should be kicked. """

    def __str__(self):
        s = CheckFailed.__str__(self)
        return "State malformed: %s" % s

class RecentlyExpiredProfileException(Exception):
    """ Raised by :meth:`Profile.check` if the profile has expired, but is still within
        the ``EXPIRY_GRACE_PERIOD``.
    """

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
    """ Represents a user profile, i.e. the data contained in the JSON document linked to from the
        webfinger XRD document. """

    #: The full name of the profile owner (unicode).
    full_name = None

    #: The hometown of the profile owner (unicode).
    hometown = None

    #: The ISO 3166-1 alpha-2 country code of the hometown (string).
    country_code = None

    #: A comma-separated list of service identifiers for the services the profile owner uses (string).
    #: Used to filter search results. Example: ``"friendica,email,diaspora"``
    services = None

    #: The raw signature of the CAPTCHA provider (string). Must be of the format returned by :func:`sduds.lib.signature.sign`.
    captcha_signature = None # str

    #: The submission timestamp specified in the profile, used to have the states expire (integer).
    submission_timestamp = None # int

    def __init__(self, full_name, hometown, country_code, services, captcha_signature, submission_timestamp):
        """ For a description of the arguments see the documentation of the attributes of this class. """

        self.full_name = full_name
        self.hometown = hometown
        self.country_code = country_code
        self.services = services
        self.captcha_signature = captcha_signature
        self.submission_timestamp = submission_timestamp

    def __composite_values__(self):
        """ This method is used for composite mappings in sqlalchemy-based database backends. """

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
        """ Checks the profile against a certain webfinger address. It performs the following checks:

            * The CAPTCHA provider's :attr:`signature <captcha_signature>` of the webfinger address is
              validated using the provider's public key. Otherwise, :class:`MalformedProfileException`
              is raised.
            * The :attr:`submission_timestamp` must not be in the future, or :class:`MalformedProfileException`
              is raised.
            * If the age of the profile (calculated using :attr:`submission_timestamp`) is beyond
              ``PROFILE_LIFETIME``, the profile is expired. If the additional ``EXPIRY_GRACE_PERIOD``
              is exceeded, :class:`MalformedProfileException` is raised, otherwise
              :class:`RecentlyExpiredProfileException`.
            * The length of the ``webfinger_address`` argument is checked using ``MAX_ADDRESS_LENGTH``.
              If it is too long, :class:`MalformedProfileException` is raised.
            * The :attr:`full_name`, :attr:`hometown`, :attr:`country_code` and :attr:`services` attributes
              are checked in the same way. Additionally, each service string of the services attribute is
              checked against ``MAX_SERVICE_LENGTH``.

            If the profile is okay, this method returns ``True``.

            :param webfinger_address: the webfinger address of this profile -- needed to check
                                      the signature of the CAPTCHA provider
            :type webfinger_address: string
            :param reference_timestamp: the timestamp to compare the submission timestamp against
                                        (optional) -- defaults to the current time
            :type reference_timestamp: integer
            :param public_key: the base64-encoded public key of the CAPTCHA provider (optional)
                               -- defaults to ``constants.CAPTCHA_PUBLIC_KEY``
            :type public_key: string
            :rtype: boolean
        """

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
        """ Retrieves a profile from the web, given the webfinger address.
            Raises :class:`IOError` if there are connection problems or :class:`RetrievalFailed`
            if the profile could not be retrieved for other reasons.

            :param address: the webfinger address of the profile that should be retrieved
            :type address: string
            :param timeout: timout in seconds (optional)
            :type timeout: float
            :rtype: :class:`Profile`
        """

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
    """ Given a certain webfinger address, a profile may be accessible there or not.
        The accessible profile, or whether there is one at all, may change with time.
        This class is used to represent the state of a webfinger address at a certain
        time.

        States are used in two different contexts: As an argument to
        :meth:`StateDatabase.save <sduds.statedatabase.interface.StateDatabase.save>`
        or in :class:`StateMessages <sduds.synchronization.StateMessage>` to communicate
        profile states to partners.
        Therefore, there are three kinds of states, defined as follows:

        * A *valid-up-to-date* state is a state with none of the :attr:`address`,
          :attr:`retrieval_timestamp` and :attr:`profile` attributes set to ``None``.
        * A *valid-out-dated* state is a state with both :attr:`retrieval_timestamp`
          and :attr:`profile` set to ``None``.
        * An *invalid* state is a state with only :attr:`profile` set to ``None``.

        Only valid-up-to-date states are actually stored in the database, the two other kinds
        of states are only used as intermediary objects.
    """

    #: The webfinger address (string).
    address = None

    #: The timestamp to which the data saved in the :attr:`profile` attribute refers (integer).
    #: In :class:`State` instances used in :class:`StateMessages <sduds.synchronization.StateMessage>`,
    #: this may also be set to ``None`` to indicate to a partner that the profile has changed,
    #: but we do not have up-to-date data, so the partner should retrieve the profile by himself.
    #: If set to ``None``, :attr:`profile` must also be ``None``.
    retrieval_timestamp = None

    #: The :class:`Profile` which is accessible at the given webfinger address, or ``None``
    #: if no profile can be retrieved from this address.
    profile = None

    def __init__(self, address, retrieval_timestamp, profile):
        """ For a description of the arguments see the documentation of the attributes of this class. """

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
        """ This method is only applicable for invalid or valid-up-to-date states, i.e.
            states that can be used for `StateDatabase.save`.

            It performs the following checks:

            * The :attr:`retrieval_timestamp` must not be in the future, otherwise
              :class:`MalformedStateException` is raised.
            * The retrieval timestamp must not be older than ``MAX_AGE``, otherwise
              :class:`MalformedStateException` is raised.
            * For valid-up-to-date states, the profile is checked using :meth:`Profile.check`,
              which may raise :class:`MalformedProfileException` or :class:`RecentlyExpiredProfileException`.
            * For valid-up-to-date states, the retrieval timestamp must not lie before the
              :attr:`~Profile.submission_timestamp` of the profile, or :class:`MalformedStateException`
              is raised.

            If the state is okay, ``True`` is returned.

            :param reference_timestamp: the timestamp to compare the submission timestamp against
                                        (optional) -- defaults to the current time
            :type reference_timestamp: integer
            :rtype: boolean
        """

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
        """ Retrieves the profile from the web and constructs a :class:`State` instance. If no profile
            is accessible, an invalid state is constructed. The retrieval timestamp of the constructed
            state is set to the current time.

            :param address: the webfinger address of the profile for which a state should be constructed
            :type address: string
            :param timeout: timout in seconds (optional)
            :type timeout: float
            :rtype: :class:`State`
        """

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
        """ Calculated property which returns a raw 16-byte hash value (string) built of the
            :attr:`address` and the :attr:`profile`. Used to keep track of profile changes.
            Only applicable for valid-up-to-date states.
        """

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
    """ Represents a former :class:`State` which was made obsolete by a more recent state.
        An instance of this class is saved when a :attr:`~State.hash` is deleted from the
        database. When synchronizing, all hashes a partner offers to us are compared to the
        saved ghost hashes, and if one belongs to a 'ghost', we do not accept the State the
        partner offers us, but tell him to delete this state from his database.
    """

    #: The :attr:`~State.hash` of the former state.
    hash = None

    #: The :attr:`~State.retrieval_timestamp` of the former state.
    retrieval_timestamp = None

    def __init__(self, binhash, retrieval_timestamp):
        """ For a description of the arguments see the documentation of the attributes of this class. """

        self.hash = binhash
        self.retrieval_timestamp = retrieval_timestamp
