#!/usr/bin/env python

from testing import tests
from testing.profile_server import ProfileServer

profile_server = ProfileServer("localhost", 3000)

tests.synchronization(profile_server)
