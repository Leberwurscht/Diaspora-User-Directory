#!/usr/bin/env python

## Reminder: Make clear in documentation that server/client is only about who
## initiates the connection; the synchronisation is always happening in both
## directions.

import logging

import sys
import hashlib
import socket

# config file path
partners_file = "partners"

# create partners file if not existing
open(partners_file, "a").close()

# read partners file and fill servers/clients dictionary

servers = {} # server address -> Server object mapping, where address=(host, port)
clients = {} # username -> Client object mapping

class Server:
    def __init__(self, host, port, entryserverport, username, password):
        self.address = (host, port)
        self.entryserver_address = (host, entryserverport)
        self.username = username
        self.password = password

    def config_line(self):
        return "server "+self.address[0]+" "+str(self.address[1])+" "+str(self.entryserver_address[1])+" "+self.username+" "+self.password

    def authenticated_socket(self):
        asocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        asocket.connect(self.address)

        asocket.sendall(self.username+"\n")
        asocket.sendall(self.password+"\n")

        f = asocket.makefile()
        answer = f.readline().strip()
        f.close()

        if answer=="OK":
            logging.info("Successfully authenticated to server %s." % str(self.address))
            return asocket
        else:
            logging.error("Authentication to server %s failed." % str(self.address))
            asocket.close()
            return False

    def __str__(self):
        return self.username+"@"+self.address[0]+":"+str(self.address[1])

class Client:
    def __init__(self, host, entryserverport, username, passwordhash):
        self.entryserver_address = (host, entryserverport)
        self.username = username
        self.passwordhash = passwordhash

    def config_line(self):
        return "client "+self.entryserver_address[0]+" "+str(self.entryserver_address[1])+" "+self.username+" "+self.passwordhash

    def password_valid(self, password):
        comparehash = hashlib.sha1(password).hexdigest()
        
        return comparehash==self.passwordhash

    def __str__(self):
        return self.entryserver_address[0]+" (username: "+self.username+")"

for line in open(partners_file):
    # check if line is empty
    if not line.strip(): continue

    # get first word of line
    direction = line.split()[0]

    if direction=="server":
        try:
            direction, host, port, entryserverport, username, password = line.split()
        except ValueError:
            logging.error("Invalid server line in partners file!")
            continue

        try:
            port = int(port)
        except ValueError:
            logging.error("Invalid port %s for server %s in partners file!" % (port, host))
            continue

        try:
            entryserverport = int(entryserverport)
        except ValueError:
            logging.error("Invalid port %s for entry server %s in partners file!" % (entryserverport, host))
            continue

        server = Server(host, port, entryserverport, username, password)
        servers[server.address] = server

    elif direction=="client":
        try:
            direction, host, entryserverport, username, passwordhash = line.split()
        except ValueError:
            logging.error("Invalid client line in partners file!")
            continue

        try:
            entryserverport = int(entryserverport)
        except ValueError:
            logging.error("Invalid port %s for entry server %s in partners file!" % (entryserverport, host))
            continue

        clients[username] = Client(host, entryserverport, username, passwordhash)

    else:
        logging.error("Invalid line in partners file: first word must be 'server' or 'client'.")

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

        configfile = open(partners_file, "w")

        for server in servers.values():
            print >>configfile, server.config_line()

        for client in clients.values():
            print >>configfile, client.config_line()

        configfile.close()

    parser = optparse.OptionParser(
        usage = "%prog -a -s HOST PORT ENTRYSERVERPORT USERNAME\nOr: %prog -d -s HOST PORT\nOr: %prog -a -c USERNAME HOST ENTRYSERVERPORT\nOr: %prog -d -c USERNAME\nOr: %prog -l [-s|-c]",
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

        if options.server:
            print "Servers:"
            for server in servers.values(): print server
            print

        if options.client:
            print "Clients:"
            for client in clients.values(): print client
            print

    elif options.add and options.server:
        try:
            host,port,entryserverport,username = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need host, port, EntryServer port and username."
            sys.exit(1)
            
        try:
            port = int(port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid port."
            sys.exit(1)
            
        try:
            entryserverport = int(entryserverport)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid EntryServer port."
            sys.exit(1)

        print "Adding server \"%s\"." % host

        password = read_password()
        server = Server(host, port, entryserverport, username, password)
        servers[server.address] = server
        write_partners()

    elif options.add and options.client:
        try:
            username,host,entryserverport = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need username, host and EntryServer port."
            sys.exit(1)

        try:
            entryserverport = int(entryserverport)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid EntryServer port."
            sys.exit(1)

        print "Adding client \"%s\"." % username

        password = read_password()
        passwordhash = hashlib.sha1(password).hexdigest()
        clients[username] = Client(host, entryserverport, username, passwordhash)
        write_partners()

    elif options.add:
        print >>sys.stderr, "ERROR: Need either -s or -c."
        sys.exit(1)

    elif options.delete and options.server:
        try:
            host,port = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need host and port."
            sys.exit(1)

        address = (host, port)

        if not address in servers:
            print >>sys.stderr, "ERROR: Server \"%s\" is not in list." % str(address)
            sys.exit(1)

        print "Deleting server \"%s\"." % address

        del servers[address]
        write_partners()

    elif options.delete and options.client:
        try:
            username, = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need username."
            sys.exit(1)

        if not username in clients:
            print >>sys.stderr, "ERROR: Client \"%s\" is not in list."
            sys.exit(1)

        print "Deleting client username \"%s\"." % username

        del clients[username]
        write_partners()

    elif options.delete:
        print >>sys.stderr, "ERROR: Need either -s or -c."
        sys.exit(1)

    else:
        parser.print_help()
