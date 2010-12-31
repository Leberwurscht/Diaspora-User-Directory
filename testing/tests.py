#!/usr/bin/env python

import sduds, partners, entries
import hashlib, threading, time

def synchronization(profile_server, start_port=20000, keep=False):
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

    ### close servers
    sduds1.close()
    sduds2.close()

    ### remove database files
    if not keep:
        sduds1.erase()
        sduds2.erase()
