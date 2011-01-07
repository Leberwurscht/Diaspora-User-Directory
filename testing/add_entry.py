#!/usr/bin/python

from profile_server import ProfileServer
from sduds import SDUDS

import sys, binascii, time

profile_server = ProfileServer("localhost", 3000)
sduds = SDUDS(("localhost", 20000))

webfinger_address = "JohnDoe@%s:%d" % profile_server.address
binhash = sduds.submit_address(webfinger_address)

if binhash==None:
    print >>sys.stdout, "Adding entry failed"
else:
    print "Added entry with hash %s" % binascii.hexlify(binhash)

# wait a bit so trieserver can save the hashes
time.sleep(1.0)

sduds.close()
