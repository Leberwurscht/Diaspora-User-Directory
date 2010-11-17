#!/usr/bin/env python

## Reminder: Make clear in documentation that server/client is only about who
## initiates the connection; the synchronisation is always happening in both
## directions.

import logging

import sys
import hashlib

# config file path
partners_file = "partners"

# create partners file if not existing
open(partners_file, "a").close()

# read partners file and fill servers/clients dictionary

servers = {} # server address -> password mapping
clients = {} # username -> password hash mapping

for line in open(partners_file):
    try:
        direction, identifier, password = line.split()
    except ValueError:
        logging.error("Invalid line in partners file!")
        continue

    if direction=="server":
        address = identifier

        servers[address] = password

    elif direction=="client":
        username = identifier
        passwordhash = password

        clients[username] = passwordhash

    else:
        logging.error("Invalid line in partners file: first word must be 'server' or 'client'.")

# access functions
def client_allowed(username, password):
    global clients

    if not username in clients: return False

    passwordhash = hashlib.sha1(password).hexdigest()

    if clients[username]==passwordhash: return True
    else: return False

def server_password(address):
    if address in servers:
        return servers[address]
    else:
        return None

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

    def write_partners():
        global servers, clients

        f = open(partners_file, "w")

        for address,password in servers.iteritems():
            f.write("server "+address+" "+password+"\n")

        for username,passwordhash in clients.iteritems():
            f.write("client "+username+" "+passwordhash+"\n")

        f.close()

    parser = optparse.OptionParser(
        usage = "%prog ( -s -l|-a|-d|-p SERVER_ADDRESS ) | ( -c -l|-a|-d|-p CLIENT_USERNAME )",
        description="manage the synchronisation partners list"
    )
    
    parser.add_option( "-s", "--server", action="store_true", dest="server", help="action deals with server")
    parser.add_option( "-c", "--client", action="store_true", dest="client", help="action deals with client")
    
    parser.add_option( "-l", "--list", action="store_true", dest="list", help="list partners")
    parser.add_option( "-a", "--add", metavar="IDENTIFIER", dest="add", help="add a new partner")
    parser.add_option( "-d", "--delete", metavar="IDENTIFIER", dest="delete", help="delete a partner")
    parser.add_option( "-p", "--password", metavar="IDENTIFIER", dest="password", help="change password of a partner")

    (options, args) = parser.parse_args()

    if options.list:
        # if neither -s nor -c given, display both
        if not options.server and not options.client:
            options.server = True
            options.client = True

        if options.server:
            print "Server addresses:"
            for server in servers: print server
            print

        if options.client:
            print "Client usernames:"
            for client in clients: print client
            print

    elif options.add and options.server:
        address = options.add

        if address in servers:
            print >>sys.stderr, "ERROR: Server address \"%s\" is already in list."
            sys.exit(1)

        print "Adding server \"%s\"." % address

        password = read_password()
        servers[address] = password
        write_partners()

    elif options.add and options.client:
        username = options.add

        if username in clients:
            print >>sys.stderr, "ERROR: Client username \"%s\" is already in list."
            sys.exit(1)

        print "Adding client \"%s\"." % username

        password = read_password()
        passwordhash = hashlib.sha1(password).hexdigest()
        clients[username] = passwordhash
        write_partners()

    elif options.delete and options.server:
        address = options.delete

        if not address in servers:
            print >>sys.stderr, "ERROR: Server \"%s\" is not in list."
            sys.exit(1)

        print "Deleting server address \"%s\"." % address

        del servers[address]
        write_partners()

    elif options.delete and options.server:
        username = options.delete

        if not username in clients:
            print >>sys.stderr, "ERROR: Client \"%s\" is not in list."
            sys.exit(1)

        print "Deleting client username \"%s\"." % username

        del clients[username]
        write_partners()

    elif options.password and options.server:
        address = options.password

        if not address in servers:
            print >>sys.stderr, "ERROR: Server \"%s\" is not in list."
            sys.exit(1)

        print "Setting password for server \"%s\"." % address

        password = read_password()
        servers[address] = password
        write_partners()

    elif options.password and options.client:
        username = options.password

        if not username in clients:
            print >>sys.stderr, "ERROR: Client \"%s\" is not in list."
            sys.exit(1)

        print "Changing password of client \"%s\"." % username

        password = read_password()
        passwordhash = hashlib.sha1(password).hexdigest()
        clients[username] = passwordhash
        write_partners()

    else:
        parser.print_help()
