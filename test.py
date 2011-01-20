#!/usr/bin/env python

from testing import tests
from testing.profile_server import ProfileServer

profile_server = ProfileServer("localhost", 3000)

tests.simple_synchronization(profile_server)

tests.captcha_signature(profile_server)

tests.NonConcurrenceOffense(profile_server)

tests.twoway_synchronization(profile_server)

tests.delete_from_trie(profile_server)

tests.delete_entry_by_submission(profile_server)

tests.delete_entry_by_synchronization(profile_server)

tests.overwrite_entry_by_submission(profile_server)

tests.overwrite_entry_by_synchronization(profile_server)
