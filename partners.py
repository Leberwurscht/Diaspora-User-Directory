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

servers = {} # server address -> Server object mapping
clients = {} # username -> Client object mapping

class Server:
    def __init__(self, address, port, username, password):
        self.address = address
        self.port = port
        self.username = username
        self.password = password

    def config_line(self):
        return "server "+self.address+" "+str(self.port)+" "+self.username+" "+self.password

    def authenticated_socket(self):
        socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket.connect((self.address, self.port))

        f = socket.makefile()

        f.write(self.username+"\n")
        f.write(self.password+"\n")

        answer = f.readline().strip()

        f.close()

        if answer=="OK":
            logging.info("Successfully authenticated with server %s." % self.address)
            return socket
        else:
            logging.error("Authentication with server %s failed." % self.address)
            socket.close()
            return False

    def __str__(self):
        return self.username+"@"+self.address+":"+str(self.port)

class Client:
    def __init__(self, username, passwordhash):
        self.username = username
        self.passwordhash = passwordhash

    def config_line(self):
        return "client "+self.username+" "+self.passwordhash

    def check_password(self, password):
        comparehash = hashlib.sha1(password).hexdigest()
        
        return comparehash==self.passwordhash

    def __str__(self):
        return self.username

for line in open(partners_file):
    # check if line is empty
    if not line.strip(): continue

    # get first word of line
    direction = line.split()[0]

    if direction=="server":
        try:
            direction, address, port, username, password = line.split()
        except ValueError:
            logging.error("Invalid server line in partners file!")
            continue

        try:
            port = int(port)
        except ValueError:
            logging.error("Invalid port for server %s in partners file!" % address)
            continue

        servers[address] = Server(address, port, username, password)

    elif direction=="client":
        try:
            direction, username, passwordhash = line.split()
        except ValueError:
            logging.error("Invalid client line in partners file!")
            continue

        clients[username] = Client(username, passwordhash)

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

        for address,server in servers.iteritems():
            print >>configfile, server.config_line()

        for username,client in clients.iteritems():
            print >>configfile, client.config_line()

        configfile.close()

    parser = optparse.OptionParser(
        usage = "%prog ( -s -l|-a|-d ADDRESS PORT USERNAME ) | ( -c -l|-a|-d USERNAME )",
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
            print "Server addresses:"
            for server in servers.values(): print server
            print

        if options.client:
            print "Client usernames:"
            for client in clients.values(): print client
            print

    elif options.add and options.server:
        try:
            address,port,username = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need address, port and username."
            sys.exit(1)
            
        try:
            port = int(port)
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid port."
            sys.exit(1)

        print "Adding server \"%s\"." % address

        password = read_password()
        servers[address] = Server(address, port, username, password)
        write_partners()

    elif options.add and options.client:
        try:
            username, = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need username."
            sys.exit(1)

        print "Adding client \"%s\"." % username

        password = read_password()
        passwordhash = hashlib.sha1(password).hexdigest()
        clients[username] = Client(username, passwordhash)
        write_partners()

    elif options.delete and options.server:
        try:
            address, = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need server address."
            sys.exit(1)

        if not address in servers:
            print >>sys.stderr, "ERROR: Server \"%s\" is not in list."
            sys.exit(1)

        print "Deleting server \"%s\"." % address

        del servers[address]
        write_partners()

    elif options.delete and options.server:
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

    else:
        parser.print_help()
