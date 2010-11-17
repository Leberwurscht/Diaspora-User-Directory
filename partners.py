#!/usr/bin/env python

import logging

import sys
import hashlib

# config file path
partners_file = "partners"

# create partners file if not existing
open(partners_file, "a").close()

# read partners file and fill partners dictionary
partners = {}

for line in open(partners_file):
    try:
        identifier, password = line.split()
    except ValueError:
        logging.error("Invalid line in partners file!")
        sys.exit(1)

    partners[identifier.strip()] = password.strip()

def authenticate(identifier, password):
    global partners

    if not identifier in partners: return False

    pwhash = hashlib.sha1(password).hexdigest()

    if partners[identifier]==pwhash: return True
    else: return False

# command line interface
if __name__=="__main__":
    import optparse
    import getpass

    def read_password():
        while True:
            first = getpass.getpass("Password: ")
            second = getpass.getpass("Repeat password: ")

            if first==second:
                passwordhash = hashlib.sha1(first).hexdigest()
                return passwordhash
            else:
                print >>sys.stderr, "ERROR: Passwords do not match."

    def write_partners():
        global partners

        f = open(partners_file, "w")

        for identifier,password in partners.iteritems():
            f.write(identifier+" "+password+"\n")

        f.close()

    parser = optparse.OptionParser(
        usage = "%prog [-ladp] [IDENTIFIER]",
        description="manage the synchronisation partners list"
    )
    
    parser.add_option( "-l", "--list", action="store_true", dest="list", help="list partners")
    parser.add_option( "-a", "--add", metavar="IDENTIFIER", dest="add", help="add a new partner")
    parser.add_option( "-d", "--delete", metavar="IDENTIFIER", dest="delete", help="delete a partner")
    parser.add_option( "-p", "--password", metavar="IDENTIFIER", dest="password", help="change password of a partner")

    (options, args) = parser.parse_args()

    if options.list:
        for identifier in partners:
            print identifier

    elif options.add:
        identifier = options.add

        if identifier in partners:
            print >>sys.stderr, "ERROR: Partner \"%s\" is already in list."
            sys.exit(1)

        print "Adding partner \"%s\"." % identifier
        passwordhash = read_password()
        partners[identifier] = passwordhash
        write_partners()

    elif options.delete:
        identifier = options.delete

        if not identifier in partners:
            print >>sys.stderr, "ERROR: Partner \"%s\" is not in list."
            sys.exit(1)

        print "Deleting partner \"%s\"." % identifier
        del partners[identifier]
        write_partners()

    elif options.password:
        identifier = options.password

        if not identifier in partners:
            print >>sys.stderr, "ERROR: Partner \"%s\" is not in list."
            sys.exit(1)

        print "Changing password of partner \"%s\"." % identifier
        passwordhash = read_password()
        partners[identifier] = passwordhash
        write_partners()

    else:
        parser.print_help()
