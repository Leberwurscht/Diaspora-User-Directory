#!/usr/bin/python

from profile_server import ProfileServer, Profile
from sduds import SDUDS

import sys, binascii, time

profile_server = ProfileServer("localhost", 3000)
sduds = SDUDS(("localhost", 20000))

webfinger_address = "JohnDoe@%s:%d" % profile_server.address
profile = Profile(webfinger_address)
profile_server.profiles[webfinger_address] = profile
sduds.submit_address(webfinger_address)
sduds.context.queue.join()
sduds.terminate()
