#!/usr/bin/env python

import sduds, partners, entries
import hashlib, threading, time

def _get_partners(start_port=20000):
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
        control_probability=0.1,
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
        control_probability=0.7,
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
    binhash = sduds1.submit_address(webfinger_address)

    assert not binhash==None

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)

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

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    binhash = sduds1.submit_address(webfinger_address)

    assert not binhash==None

    ### manipulate the captcha signature
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
        webfinger profiles gets kicked. This test is probabilistic, it may fail even if everything
        works as it should. """

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add entries to the server
    for i in xrange(num_entries):
        webfinger_address = "Random%d@%s:%d" % ((i,) + profile_server.address)
        binhash = sduds1.submit_address(webfinger_address)

        assert not binhash==None

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)

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

def twoway_synchronization(profile_server, start_port=20000, num_entries=30, erase=True):
    """ This test adds many entries to two servers who will then synchronize with each other.
        It verifies that both servers know all entries afterwards. """

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add entries to the server
    hashes = set()

    # first server
    for i in xrange(num_entries):
        webfinger_address = "First%d@%s:%d" % ((i,) + profile_server.address)
        binhash = sduds1.submit_address(webfinger_address)

        assert not binhash==None

        hashes.add(binhash)

    ### add entries to the second server
    for i in xrange(num_entries):
        webfinger_address = "Second%d@%s:%d" % ((i,) + profile_server.address)
        binhash = sduds2.submit_address(webfinger_address)

        assert not binhash==None

        hashes.add(binhash)

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)

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
    binhash = sduds1.submit_address(webfinger_address)

    assert not binhash==None

    ### remove the corresponding hash from the trie
    sduds1.context.hashtrie.delete([binhash])    

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)

    ### verify that the entry didn't get transmitted
    session = sduds2.context.entrydb.Session()
    num_entries = session.query(entries.Entry).count()
    session.close()

    assert num_entries==0

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def delete_entry(profile_server, start_port=20000, erase=True):
    """ Tests that an entry is deleted from the database and from the
        trie when an invalid webfinger address is resubmitted. """

    now = int(time.time())
    submission_timestamp1 = now - 3600*24*4
    submission_timestamp2 = now

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add an entry to the first server
    webfinger_address = "Vanish@%s:%d" % profile_server.address
    binhash = sduds1.context.process_submission(webfinger_address, submission_timestamp1)

    assert not binhash==None

    ### remove the entry by resubmitting the now dead address
    binhash = sduds1.context.process_submission(webfinger_address, submission_timestamp2)

    assert binhash==None

    ### check that the database entry vanished
    session = sduds1.context.entrydb.Session()
    num_entries = session.query(entries.Entry).count()
    session.close()

    assert num_entries==0

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.context.partnerdb, partner_name=partner_name1)
    assert not server.kicked()
    sduds2.synchronize_with_partner(server)

    ### verify that no entry got transmitted
    session = sduds2.context.entrydb.Session()
    num_entries = session.query(entries.Entry).count()
    session.close()

    assert num_entries==0

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)

def overwrite_entry(profile_server, start_port=20000, erase=True):
    """ Tests overwriting an entry by resubmitting the webfinger address.
        This test is probabilistic. It may fail even if everything works as
        it should. """

    now = int(time.time())
    submission_timestamp1 = now - 3600*24*4
    submission_timestamp2 = now

    sduds1, partner_name1, sduds2, partner_name2 = _get_partners(start_port)

    ### add an entry to the first server
    webfinger_address = "Random@%s:%d" % profile_server.address
    binhash1 = sduds1.context.process_submission(webfinger_address, submission_timestamp1)

    assert not binhash1==None

    ### overwrite the entry by resubmitting the address
    binhash2 = sduds1.context.process_submission(webfinger_address, submission_timestamp2)

    assert not binhash2==None
    assert not binhash1==binhash2

    ### check that the old database entry vanished
    session = sduds1.context.entrydb.Session()
    num_entries = session.query(entries.Entry).filter_by(hash=binhash1).count()
    session.close()

    assert num_entries==0

    ### check that the new database entry is there
    session = sduds1.context.entrydb.Session()
    num_entries = session.query(entries.Entry).filter_by(hash=binhash2).count()
    session.close()

    assert num_entries==1

    ### close servers
    sduds1.terminate(erase=erase)
    sduds2.terminate(erase=erase)
