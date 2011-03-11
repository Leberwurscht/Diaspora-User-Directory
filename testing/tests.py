#!/usr/bin/env python

import sduds, partners, entries
import hashlib, threading, time

from profile_server import Profile

def _get_partners(start_port=20000, control_probability=.7):
    webserver_port1 = start_port
    control_port1 = start_port+1

    webserver_port2 = start_port+3
    control_port2 = start_port+4

    ### run two servers
    sduds1 = sduds.SDUDS(("", webserver_port1), "_test1", erase=True)
    sduds1.run_synchronization_server("localhost", "", control_port1)

    sduds2 = sduds.SDUDS(("", webserver_port2), "_test2", erase=True)
    sduds2.run_synchronization_server("localhost", "", control_port2)

    ### connect the servers
    partner_name1 = "test1"
    password1 = "12345"
    partner_name2 = "test2"
    password2 = "54321"
    passwordhash1 = hashlib.sha1(password1).digest()
    passwordhash2 = hashlib.sha1(password2).digest()

    # add server2 as a client to server1
    client = partners.Client(sduds1.context.partnerdb,
        address="http://localhost:%d/" % webserver_port2,
        control_probability=control_probability,
        identity=partner_name1,
        password=password1,
        partner_name=partner_name2,
        passwordhash=passwordhash2,
    )

    sduds1.context.partnerdb.Session.add(client)
    sduds1.context.partnerdb.Session.commit()

    # add server1 as a server to server2
    server = partners.Server(sduds2.context.partnerdb,
        address="http://localhost:%d/" % webserver_port1,
        control_probability=control_probability,
        identity=partner_name2,
        password=password2,
        partner_name=partner_name1,
        passwordhash=passwordhash1
    )

    sduds2.context.partnerdb.Session.add(server)
    sduds2.context.partnerdb.Session.commit()

    ### give the servers some time to start up
    time.sleep(0.5)

    return sduds1, partner_name1, sduds2, partner_name2

def simple_synchronization(profile_server, start_port=20000, erase=True):
    """ One webfinger address is submitted to one server, which will synchronize
        with another server. This test verifies that the entry gets to the second
        server. """

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    profile = Profile(webfinger_address)
    profile_server.profiles[webfinger_address] = profile
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### verify that the entry was saved
    session = sduds1.context.entrydb.Session()
    binhash, = session.query(entries.Entry.hash).one()
    session.close()

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()

    ### verify that the entry got transmitted
    session = sduds2.context.entrydb.Session()
    entry = session.query(entries.Entry).one()
    assert entry.hash==binhash

    session.close()

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def captcha_signature(profile_server, start_port=20000, erase=True):
    """ An entry with a bad captcha signature will be sent from one server to the other.
        This test verifies that the server gets kicked. """

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port, 0.)

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    profile = Profile(webfinger_address)
    profile_server.profiles[webfinger_address] = profile
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### verify that the entry was saved
    session = sduds1.context.entrydb.Session()
    binhash, = session.query(entries.Entry.hash).one()
    session.close()

    ### manipulate the captcha signature at the first server
    session = sduds1.context.entrydb.Session()
    entry = session.query(entries.Entry).one()

    if entry.captcha_signature[0]==0:
        entry.captcha_signature = '\x01'+entry.captcha_signature[1:]
    else:
        entry.captcha_signature = '\x00'+entry.captcha_signature[1:]

    session.add(entry)
    session.commit()
    session.close()

    ### make server2 connect to server1 for synchronization
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()

    ### verify that the server is kicked
    assert server.kicked()

    ### verify that no entry got transmitted
    session = sduds2.context.entrydb.Session()
    number_of_entries = session.query(entries.Entry).count()
    session.close()

    assert number_of_entries==0

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def NonConcurrenceOffense(profile_server, start_port=20000, num_entries=70, erase=True):
    """ This test verifies that a server that serves too many entries that do not match the real
        webfinger profiles gets kicked. """

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port, 1.)

    ### add entries to the server
    profiles = []

    for i in xrange(num_entries):
        webfinger_address = "JohnDoe%d@%s:%d" % ((i,) + profile_server.address)
        profile = Profile(webfinger_address)
        profile_server.profiles[webfinger_address] = profile
        profiles.append(profile)
        sduds1.submit_address(webfinger_address)

    sduds1.context.queue.join()

    ### manipulate profiles
    for profile in profiles:
        profile.name = ""

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()

    ### verify that the server is kicked
    assert server.kicked()

#    ### verify that no entry got transmitted
#    session = sduds2.context.entrydb.Session()
#    number_of_entries = session.query(entries.Entry).count()
#    session.close()
#
#    assert number_of_entries==0

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def twoway_synchronization(profile_server, start_port=20000, num_entries=30, erase=True):
    """ This test adds many entries to two servers who will then synchronize with each other.
        It verifies that both servers know all entries afterwards. """

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    hashes = set()

    ### add entries to the first server
    for i in xrange(num_entries):
        webfinger_address = "First%d@%s:%d" % ((i,) + profile_server.address)
        profile = Profile(webfinger_address)
        profile_server.profiles[webfinger_address] = profile
        sduds1.submit_address(webfinger_address)

    sduds1.context.queue.join()

    session = sduds1.context.entrydb.Session()
    for entry in session.query(entries.Entry):
        hashes.add(entry.hash)
    session.close()

    ### add entries to the second server
    for i in xrange(num_entries):
        webfinger_address = "Second%d@%s:%d" % ((i,) + profile_server.address)
        profile = Profile(webfinger_address)
        profile_server.profiles[webfinger_address] = profile
        sduds2.submit_address(webfinger_address)

    sduds2.context.queue.join()

    session = sduds2.context.entrydb.Session()
    for entry in session.query(entries.Entry):
        hashes.add(entry.hash)
    session.close()

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()
    sduds1.context.queue.join()

#    ### give sduds1 some time to finish processing
#    time.sleep(1)

    ### verify that both servers have got all entries
    hashes1 = set()
    session = sduds1.context.entrydb.Session()
    for entry in session.query(entries.Entry):
        hashes1.add(entry.hash)
    session.close()

    assert hashes1==hashes

    hashes2 = set()
    session = sduds2.context.entrydb.Session()
    for entry in session.query(entries.Entry):
        hashes2.add(entry.hash)
    session.close()

    assert hashes2==hashes

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def delete_from_trie(profile_server, start_port=20000, erase=True):
    """ Tests the HashTrie.delete function by verifying that an entry
        is not transmitted to another server if the hash is deleted
        from the trie. """

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    profile = Profile(webfinger_address)
    profile_server.profiles[webfinger_address] = profile
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### verify that the entry was saved
    session = sduds1.context.entrydb.Session()
    binhash, = session.query(entries.Entry.hash).one()
    session.close()

    ### remove the corresponding hash from the trie
    sduds1.context.hashtrie.delete([binhash])    

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()

    ### verify that the entry didn't get transmitted
    session = sduds2.context.entrydb.Session()
    num_entries = session.query(entries.Entry).count()
    session.close()

    assert num_entries==0

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def delete_entry_by_submission(profile_server, start_port=20000, erase=True):
    """ Tests that an entry is deleted from the database and from the
        trie when an invalid webfinger address is resubmitted. """

    ### PROBLEM: A user may bother the server by resubmitting an address very often.
    ### We can't reject him if he sets his first submission timestamp very far in the
    ### past and increases it by the amount needed every time.
    # -> Solution: backlog

    #
    # Problem: WILL ALSO CAUSE SYNCHRONIZATION TRAFFIC
    # but only once per synchronization cycle
    #

    ### PROBLEM: A single user can still resubmit all addresses, which will cause a lot of
    ### traffic (but no synchronization traffic). This should be handled by a queue jam.

    now = int(time.time())
    submission_timestamp = now - 3600*24*4

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    profile = Profile(webfinger_address, submission_timestamp=submission_timestamp)
    profile_server.profiles[webfinger_address] = profile
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### remove the entry by resubmitting the now dead address
    del profile_server.profiles[webfinger_address]
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### check that the database entry vanished
    session = sduds1.context.entrydb.Session()
    num_entries = session.query(entries.Entry).count()
    session.close()

    assert num_entries==0

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()

    ### verify that no entry got transmitted
    session = sduds2.context.entrydb.Session()
    num_entries = session.query(entries.Entry).count()
    session.close()

    assert num_entries==0

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def delete_entry_by_synchronization(profile_server, start_port=20000, erase=True):
    """ Tests that an entry is deleted from the database when a partner claims
        that the profile vanished. """

    now = int(time.time())
    submission_timestamp = now - 3600*24*4

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port, control_probability=0)

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    profile = Profile(webfinger_address, submission_timestamp=submission_timestamp)
    profile_server.profiles[webfinger_address] = profile
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()

    ### remove the entry by resubmitting the now dead address
    del profile_server.profiles[webfinger_address]
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### check that the database entry vanished at server1
    session = sduds1.context.entrydb.Session()
    num_entries = session.query(entries.Entry).count()
    session.close()

    assert num_entries==0

    ### make server2 connect to server1 for synchronisation again
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()

    ### check that the database entry vanished at server2
    session = sduds2.context.entrydb.Session()
    num_entries = session.query(entries.Entry).count()
    session.close()

    assert num_entries==0

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def overwrite_entry_by_submission(profile_server, start_port=20000, erase=True):
    """ Tests overwriting an entry by resubmitting the webfinger address. """

    now = int(time.time())

    submission_timestamp1 = now - 3600*24*4
    name1 = u"JohnDoe"

    submission_timestamp2 = now
    name2 = u"JohnDoe2"

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    profile = Profile(webfinger_address, submission_timestamp=submission_timestamp1, name=name1)
    profile_server.profiles[webfinger_address] = profile
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### Change the profile
    profile.submission_timestamp = submission_timestamp2
    profile.name = name2

    ### overwrite the entry by resubmitting the address
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### check that the old database entry vanished
    session = sduds1.context.entrydb.Session()
    num_entries = session.query(entries.Entry).filter_by(full_name=name1).count()
    session.close()

    assert num_entries==0

    ### check that the new database entry is there
    session = sduds1.context.entrydb.Session()
    num_entries = session.query(entries.Entry).filter_by(full_name=name2).count()
    session.close()

    assert num_entries==1

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def overwrite_entry_by_synchronization(profile_server, start_port=20000, erase=True):
    """ Tests if overwritten entries arrive at partners. """

    now = int(time.time())

    submission_timestamp1 = now - 3600*24*4
    name1 = u"JohnDoe"

    submission_timestamp2 = now
    name2 = u"JohnDoe2"

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    profile = Profile(webfinger_address, submission_timestamp=submission_timestamp1, name=name1)
    profile_server.profiles[webfinger_address] = profile
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()

    ### Change the profile
    profile.submission_timestamp = submission_timestamp2
    profile.name = name2

    ### overwrite the entry by resubmitting the address
    sduds1.submit_address(webfinger_address)
    sduds1.context.queue.join()

    ### make server2 connect to server1 for synchronisation again
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)
    sduds2.context.queue.join()

    ### check that the new database entry arrived at the partner
    session = sduds2.context.entrydb.Session()
    num_entries = session.query(entries.Entry).filter_by(full_name=name2).count()
    session.close()

    assert num_entries==1

    ### check that the old database entry vanished at the partner
    session = sduds2.context.entrydb.Session()
    num_entries = session.query(entries.Entry).filter_by(full_name=name1).count()
    session.close()

    assert num_entries==0

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)
