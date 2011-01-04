#!/usr/bin/env python

import sduds, partners, entries
import hashlib, threading, time

def simple_synchronization(profile_server, start_port=20000, keep=False):
    """ One webfinger address is submitted to one server, which will synchronize
        with another server. This test verifies that the entry gets to the second
        server. """
    entryserver_port1 = start_port
    entryserver_port2 = start_port+1
    synchronization_port1 = start_port+2

    ### run two servers
    sduds1 = sduds.SDUDS(("localhost", entryserver_port1), "_test1", erase=True)
    sduds2 = sduds.SDUDS(("localhost", entryserver_port2), "_test2", erase=True)

    ### connect the servers
    username = "test"
    password = "12345"
    passwordhash = hashlib.sha1(password).digest()

    # add server2 as a client to server1
    client = partners.Client(sduds1.partnerdb,
        host="localhost",
        entryserver_port=entryserver_port2,
        username=username,
        passwordhash=passwordhash,
        control_probability=0.1
    )

    sduds1.partnerdb.Session.add(client)
    sduds1.partnerdb.Session.commit()

    # add server1 as a server to server2
    server = partners.Server(sduds2.partnerdb,
        host="localhost",
        username=username,
        password=password,
        synchronization_port=synchronization_port1,
        entryserver_port=entryserver_port1,
        control_probability=0.1
    )

    sduds2.partnerdb.Session.add(server)
    sduds2.partnerdb.Session.commit()

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    binhashes = sduds1.submit_address(webfinger_address)

    assert len(binhashes)==1
    binhash = binhashes[0]

    ### run a synchronization server on the first server
    thread = threading.Thread(target=sduds1.run_synchronization_server, args=("localhost", synchronization_port1))
    thread.daemon = True
    thread.start()
    time.sleep(0.5)

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.partnerdb, host="localhost", synchronization_port=synchronization_port1)
    assert not server.kicked()
    sduds2.connect_to_server(server)

    ### verify that the entry got transmitted
    session = sduds2.entrydb.Session()
    entry = session.query(entries.Entry).one()
    assert entry.hash==binhash

    session.close()

    ### close servers
    sduds1.close()
    sduds2.close()

    ### remove database files
    if not keep:
        sduds1.erase()
        sduds2.erase()

def captcha_signature(profile_server, start_port=20000, keep=False):
    """ An entry with a bad captcha signature will be sent from one server to the other.
        This test verifies that the server gets kicked. """
    entryserver_port1 = start_port
    entryserver_port2 = start_port+1
    synchronization_port1 = start_port+2

    ### run two servers
    sduds1 = sduds.SDUDS(("localhost", entryserver_port1), "_test1", erase=True)
    sduds2 = sduds.SDUDS(("localhost", entryserver_port2), "_test2", erase=True)

    ### connect the servers
    username = "test"
    password = "12345"
    passwordhash = hashlib.sha1(password).digest()

    # add server2 as a client to server1
    client = partners.Client(sduds1.partnerdb,
        host="localhost",
        entryserver_port=entryserver_port2,
        username=username,
        passwordhash=passwordhash,
        control_probability=0.1
    )

    sduds1.partnerdb.Session.add(client)
    sduds1.partnerdb.Session.commit()

    # add server1 as a server to server2
    server = partners.Server(sduds2.partnerdb,
        host="localhost",
        username=username,
        password=password,
        synchronization_port=synchronization_port1,
        entryserver_port=entryserver_port1,
        control_probability=0.1
    )

    sduds2.partnerdb.Session.add(server)
    sduds2.partnerdb.Session.commit()

    ### add an entry to the first server
    webfinger_address = "JohnDoe@%s:%d" % profile_server.address
    binhashes = sduds1.submit_address(webfinger_address)

    assert len(binhashes)==1
    binhash = binhashes[0]

    ### manipulate the captcha signature
    session = sduds1.entrydb.Session()
    entry = session.query(entries.Entry).one()

    if entry.captcha_signature[0]==0:
        entry.captcha_signature = '\x01'+entry.captcha_signature[1:]
    else:
        entry.captcha_signature = '\x00'+entry.captcha_signature[1:]

    session.add(entry)
    session.commit()
    session.close()

    ### run a synchronization server on the first server
    thread = threading.Thread(target=sduds1.run_synchronization_server, args=("localhost", synchronization_port1))
    thread.daemon = True
    thread.start()
    time.sleep(0.5)

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.partnerdb, host="localhost", synchronization_port=synchronization_port1)
    assert not server.kicked()
    sduds2.connect_to_server(server)

    ### verify that the server is kicked
    assert server.kicked()

    ### verify that no entry got transmitted
    session = sduds2.entrydb.Session()
    number_of_entries = session.query(entries.Entry).count()
    assert number_of_entries==0

    ### close servers
    sduds1.close()
    sduds2.close()

    ### remove database files
    if not keep:
        sduds1.erase()
        sduds2.erase()

def NonConcurrenceOffense(profile_server, start_port=20000, num_entries=70, keep=False):
    """ This test verifies that a server that serves too many entries that do not match the real
        webfinger profiles gets kicked. This test is probabilistic. """

    entryserver_port1 = start_port
    entryserver_port2 = start_port+1
    synchronization_port1 = start_port+2

    ### run two servers
    sduds1 = sduds.SDUDS(("localhost", entryserver_port1), "_test1", erase=True)
    sduds2 = sduds.SDUDS(("localhost", entryserver_port2), "_test2", erase=True)

    ### connect the servers
    username = "test"
    password = "12345"
    passwordhash = hashlib.sha1(password).digest()

    # add server2 as a client to server1
    client = partners.Client(sduds1.partnerdb,
        host="localhost",
        entryserver_port=entryserver_port2,
        username=username,
        passwordhash=passwordhash,
        control_probability=0.1
    )

    sduds1.partnerdb.Session.add(client)
    sduds1.partnerdb.Session.commit()

    # add server1 as a server to server2
    server = partners.Server(sduds2.partnerdb,
        host="localhost",
        username=username,
        password=password,
        synchronization_port=synchronization_port1,
        entryserver_port=entryserver_port1,
        control_probability=0.5
    )

    sduds2.partnerdb.Session.add(server)
    sduds2.partnerdb.Session.commit()

    ### add entries to the server
    for i in xrange(num_entries):
        webfinger_address = "Random%d@%s:%d" % ((i,) + profile_server.address)
        binhashes = sduds1.submit_address(webfinger_address)

        assert len(binhashes)==1
        binhash = binhashes[0]

    ### run a synchronization server on the first server
    thread = threading.Thread(target=sduds1.run_synchronization_server, args=("localhost", synchronization_port1))
    thread.daemon = True
    thread.start()
    time.sleep(0.5)

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.partnerdb, host="localhost", synchronization_port=synchronization_port1)
    assert not server.kicked()
    sduds2.connect_to_server(server)

    ### verify that the server is kicked
    assert server.kicked()

    ### verify that no entry got transmitted
    session = sduds2.entrydb.Session()
    number_of_entries = session.query(entries.Entry).count()
    assert number_of_entries==0

    ### close servers
    sduds1.close()
    sduds2.close()

    ### remove database files
    if not keep:
        sduds1.erase()
        sduds2.erase()

def twoway_synchronization(profile_server, start_port=20000, num_entries=30, keep=False):
    entryserver_port1 = start_port
    entryserver_port2 = start_port+1
    synchronization_port1 = start_port+2

    ### run two servers
    sduds1 = sduds.SDUDS(("localhost", entryserver_port1), "_test1", erase=True)
    sduds2 = sduds.SDUDS(("localhost", entryserver_port2), "_test2", erase=True)

    ### connect the servers
    username = "test"
    password = "12345"
    passwordhash = hashlib.sha1(password).digest()

    # add server2 as a client to server1
    client = partners.Client(sduds1.partnerdb,
        host="localhost",
        entryserver_port=entryserver_port2,
        username=username,
        passwordhash=passwordhash,
        control_probability=0.1
    )

    sduds1.partnerdb.Session.add(client)
    sduds1.partnerdb.Session.commit()

    # add server1 as a server to server2
    server = partners.Server(sduds2.partnerdb,
        host="localhost",
        username=username,
        password=password,
        synchronization_port=synchronization_port1,
        entryserver_port=entryserver_port1,
        control_probability=0.1
    )

    sduds2.partnerdb.Session.add(server)
    sduds2.partnerdb.Session.commit()

    ### add entries to the server
    hashes = set()

    # first server
    for i in xrange(num_entries):
        webfinger_address = "First%d@%s:%d" % ((i,) + profile_server.address)
        binhashes = sduds1.submit_address(webfinger_address)

        assert len(binhashes)==1
        binhash = binhashes[0]

        hashes.add(binhash)

    ### add entries to the second server
    for i in xrange(num_entries):
        webfinger_address = "Second%d@%s:%d" % ((i,) + profile_server.address)
        binhashes = sduds2.submit_address(webfinger_address)

        assert len(binhashes)==1
        binhash = binhashes[0]

        hashes.add(binhash)

    ### run a synchronization server on the first server
    thread = threading.Thread(target=sduds1.run_synchronization_server, args=("localhost", synchronization_port1))
    thread.daemon = True
    thread.start()
    time.sleep(0.5)

    ### make server2 connect to server1 for synchronisation
    server = partners.Server.from_database(sduds2.partnerdb, host="localhost", synchronization_port=synchronization_port1)
    assert not server.kicked()
    sduds2.connect_to_server(server)

    ### verify that both servers have got all entries
    hashes1 = set()
    session = sduds1.entrydb.Session()
    for entry in session.query(entries.Entry):
        hashes1.add(entry.hash)
    session.close()

    assert hashes1==hashes

    hashes2 = set()
    session = sduds2.entrydb.Session()
    for entry in session.query(entries.Entry):
        hashes2.add(entry.hash)
    session.close()

    assert hashes2==hashes

    ### close servers
    sduds1.close()
    sduds2.close()

    ### remove database files
    if not keep:
        sduds1.erase()
        sduds2.erase()
