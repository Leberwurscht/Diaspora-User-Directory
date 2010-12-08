#!/usr/bin/env python

## Reminder: Make clear in documentation that server/client is only about who
## initiates the connection; the synchronisation is always happening in both
## directions.

import logging

import sys
import hashlib
import socket

# initialize database
import sqlite3 as db

con = db.connect("partners.sqlite", check_same_thread=False)

# check if tables exist and create them if not

cur = con.cursor()
cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='servers'")
tables_exist = cur.fetchone()

if not tables_exist:
    cur.execute("PRAGMA legacy_file_format=0")
    cur.execute("CREATE TABLE servers (host TEXT, synchronisation_port INT, entryserver_port INT, username TEXT, password TEXT, control_probability FLOAT, kicked INTEGER)")
    cur.execute("CREATE TABLE clients (username TEXT, passwordhash TEXT, host TEXT, entryserver_port INT, control_probability FLOAT, kicked INTEGER)")

cur.close()

class Partner:
    @classmethod
    def from_database(self, *args):
        raise NotImplementedError, "Override this function in subclasses."

    def kick(self, reason):
        raise NotImplementedError, "Not implemented yet."

    def save(self):
        raise NotImplementedError, "Override this function in subclasses."

    def delete(self):
        raise NotImplementedError, "Override this function in subclasses."

class Server(Partner):
    @classmethod
    def from_database(cls, host, synchronisation_port):
        global con

        cur = con.cursor()

        cur.execute(
            "SELECT host, username, password, synchronisation_port, entryserver_port, control_probability, kicked FROM servers WHERE host=? AND synchronisation_port=?", (
            host,
            synchronisation_port
        ))

        args = cur.fetchone()
        if not args: return None

        server = cls(*args)

        return server

    def __init__(self, host, username, password, synchronisation_port=20000, entryserver_port=20001, control_probability=0.1, kicked=False):
        self.host = host
        self.synchronisation_port = synchronisation_port
        self.entryserver_port = entryserver_port
        self.username = username
        self.password = password
        self.control_probability = control_probability
        self.kicked = kicked

    def save(self):
        global con

        cur = con.cursor()

        cur.execute("INSERT INTO servers (host, synchronisation_port, entryserver_port, username, password, control_probability, kicked) VALUES (?,?,?,?,?,?,?)", (
            self.host,
            self.synchronisation_port,
            self.entryserver_port,
            self.username,
            self.password,
            self.control_probability,
            self.kicked
        ))

        con.commit()

    def delete(self):
        global con

        cur = con.cursor()

        cur.execute("DELETE FROM servers WHERE host=? and synchronisation_port=?", (
            self.host,
            self.synchronisation_port
        ))

        con.commit()

    def authenticated_socket(self):
        address = (self.host, self.synchronisation_port)
        asocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        asocket.connect(address)

        asocket.sendall(self.username+"\n")
        asocket.sendall(self.password+"\n")

        f = asocket.makefile()
        answer = f.readline().strip()
        f.close()

        if answer=="OK":
            logging.info("Successfully authenticated to server %s." % str(address))
            return asocket
        else:
            logging.error("Authentication to server %s failed." % str(address))
            asocket.close()
            return False

class Client(Partner):
    @classmethod
    def from_database(cls, username):
        global con

        cur = con.cursor()

        cur.execute(
            "SELECT host, entryserver_port, username, passwordhash, control_probability, kicked FROM clients WHERE username=?", (
            username,
        ))

        args = cur.fetchone()
        if not args: return None

        client = cls(*args)

        return client

    def __init__(self, host, entryserver_port, username, passwordhash, control_probability=0.1, kicked=False):
        self.host = host
        self.entryserver_port = entryserver_port
        self.username = username
        self.passwordhash = passwordhash
        self.control_probability = control_probability
        self.kicked = kicked

    def save(self):
        global con

        cur = con.cursor()

        cur.execute("INSERT INTO clients (username, passwordhash, host, entryserver_port, control_probability, kicked) VALUES (?,?,?,?,?,?)", (
            self.username,
            self.passwordhash,
            self.host,
            self.entryserver_port,
            self.control_probability,
            self.kicked
        ))

        con.commit()

    def delete(self):
        global con

        cur = con.cursor()

        cur.execute("DELETE FROM clients WHERE username=?", (
            self.username,
        ))

        con.commit()

    def password_valid(self, password):
        comparehash = hashlib.sha1(password).hexdigest()
        
        return comparehash==self.passwordhash

# command line interface
if __name__=="__main__":
    import optparse
    import getpass

    def read_password():
        while True:
            first = getpass.getpass("Password: ")
            second = getpass.getpass("Repeat password: ")

            if first==second:
                password = first
                return password
            else:
                print >>sys.stderr, "ERROR: Passwords do not match."

    parser = optparse.OptionParser(
        usage = "%prog -a -s HOST PORT ENTRYSERVERPORT USERNAME [CONTROL_PROBABILITY]\nOr: %prog -d -s HOST PORT\nOr: %prog -a -c USERNAME HOST ENTRYSERVERPORT [CONTROL_PROBABILITY]\nOr: %prog -d -c USERNAME\nOr: %prog -l [-s|-c]",
        description="manage the synchronisation partners list"
    )
    
    parser.add_option( "-s", "--server", action="store_true", dest="server", help="action deals with server")
    parser.add_option( "-c", "--client", action="store_true", dest="client", help="action deals with client")
    
    parser.add_option( "-l", "--list", action="store_true", dest="list", help="list partners")
    parser.add_option( "-a", "--add", action="store_true", dest="add", help="add or change a partner")
    parser.add_option( "-d", "--delete", action="store_true", dest="delete", help="delete a partner")

    (options, args) = parser.parse_args()

    if options.list:
        # if neither -s nor -c given, display both servers and clients
        if not options.server and not options.client:
            options.server = True
            options.client = True

        cur = con.cursor()

        if options.server:
            cur.execute("SELECT username, host, synchronisation_port, control_probability FROM servers")

            print "Servers:"
            for username, host, synchronisation_port, control_probability in cur:
                print username+"@"+host+":"+str(synchronisation_port)+" ["+str(control_probability)+"]"
            print

        if options.client:
            cur.execute("SELECT host, username, control_probability FROM clients")

            print "Clients:"
            for host, username, control_probability in cur:
                print host+" (using username '"+username+"') ["+str(control_probability)+"]"
            print

    elif options.add and options.server:
        try:
            host,synchronisation_port,entryserver_port,username = args[:4]
        except ValueError:
            print >>sys.stderr, "ERROR: Need host, port, EntryServer port and username."
            sys.exit(1)
            
        try:
            synchronisation_port = int(synchronisation_port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid port."
            sys.exit(1)
            
        try:
            entryserver_port = int(entryserver_port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid EntryServer port."
            sys.exit(1)

        control_probability = 0.1

        if len(args)>5:
            print >>sys.stderr, "ERROR: Too many arguments."
            sys.exit(1)
        elif len(args)==5:
            try:
                control_probability = float(args[4])
            except ValueError:
                print >>sys.stderr, "ERROR: Invalid probability."
                sys.exit(1)

        print "Adding server \"%s\"." % host

        password = read_password()

        old_server = Server.from_database(host, synchronisation_port)
        if old_server:
            old_server.delete()

        server = Server(host, username, password, synchronisation_port, entryserver_port, control_probability)
        server.save()

    elif options.add and options.client:
        try:
            username,host,entryserver_port = args[:3]
        except ValueError:
            print >>sys.stderr, "ERROR: Need username, host and EntryServer port."
            sys.exit(1)

        try:
            entryserver_port = int(entryserver_port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid EntryServer port."
            sys.exit(1)

        control_probability = 0.1

        if len(args)>4:
            print >>sys.stderr, "ERROR: Too many arguments."
            sys.exit(1)
        elif len(args)==4:
            try:
                control_probability = float(args[3])
            except ValueError:
                print >>sys.stderr, "ERROR: Invalid probability."
                sys.exit(1)

        print "Adding client \"%s\"." % username

        password = read_password()
        passwordhash = hashlib.sha1(password).hexdigest()

        old_client = Client.from_database(username)
        if old_client:
            old_client.delete()

        client = Client(host, entryserver_port, username, passwordhash, control_probability)
        client.save()

    elif options.add:
        print >>sys.stderr, "ERROR: Need either -s or -c."
        sys.exit(1)

    elif options.delete and options.server:
        try:
            host,synchronisation_port = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need host and port."
            sys.exit(1)
            
        try:
            synchronisation_port = int(synchronisation_port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid port."
            sys.exit(1)

        server = Server.from_database(host, synchronisation_port)

        if not server:
            print >>sys.stderr, "ERROR: Server \"%s:%d\" is not in list." % (host, synchronisation_port)
            sys.exit(1)

        print "Deleting server \"%s:%d\"." % (host, synchronisation_port)

        server.delete()

    elif options.delete and options.client:
        try:
            username, = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need username."
            sys.exit(1)

        client = Client.from_database(username)

        if not client:
            print >>sys.stderr, "ERROR: Client username \"%s\" is not in list." % username
            sys.exit(1)

        print "Deleting client username \"%s\"." % username

        client.delete()

    elif options.delete:
        print >>sys.stderr, "ERROR: Need either -s or -c."
        sys.exit(1)

    else:
        parser.print_help()
