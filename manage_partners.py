#!/usr/bin/env python

if __name__=="__main__":
    import optparse, sys, random
    import getpass

    from sduds.partners import *
    from sduds.lib import scheduler

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
        usage = "%prog [-p PATH] -e PARTNER_NAME ...\nOr: %prog [-p PATH] -u|U PARTNER_NAME\nOr: %prog [-p PATH] -d PARTNER_NAME\nOr: %prog [-p PATH] -l\nOr: %prog [-p PATH] -c\nArguments for editing: BASE_URL CONTROL_PROBABILITY\nand optionally PROVIDE_USERNAME [CONNECTION_SCHEDULE]",
        description="manage the synchronization partners list"
    )

    parser.add_option( "-p", "--path", metavar="DATABASE_PATH", dest="path", help="the database path")
    parser.add_option( "-l", "--list", action="store_true", dest="list", help="list partners")
    parser.add_option( "-c", "--cleanup", action="store_true", dest="cleanup", help="delete outdated control samples")
    parser.add_option( "-e", "--edit", action="store_true", dest="edit", help="add or edit a partner")
    parser.add_option( "-d", "--delete", action="store_true", dest="delete", help="delete a partner")
    parser.add_option( "-u", "--unkick", action="store_true", dest="unkick", help="unkick a partner")
    parser.add_option( "-U", "--unkick-clear", action="store_true", dest="unkick_clear", help="unkick a partner and delete control samples")

    (options, args) = parser.parse_args()

    try:
        database_path = int(options.path)
    except TypeError:
        database_path = "partners.sqlite"

    database = PartnerDatabase(database_path)

    if options.cleanup:
        database.cleanup()

    elif options.list:
        for partner in database.get_partners():
            print str(partner)

    elif options.edit:
        try:
            partner_name,base_url,control_probability = args[:3]
        except ValueError:
            print >>sys.stderr, "ERROR: Need at least partner name, base URL and control probability."
            sys.exit(1)

        if len(args)>5:
            print >>sys.stderr, "ERROR: Too many arguments."
            sys.exit(1)

        # check base url
        if not base_url.endswith("/"):
            print >>sys.stderr, "ERROR: Base URL must end with '/'."
            sys.exit(1)

        # control probability
        try:
            # might raise ValueError if string is invalid
            control_probability = float(control_probability)

            # if number is not in range, raise ValueError ourselves
            if control_probability<0 or control_probability>1:
                raise ValueError
        except ValueError:
            print >>sys.stderr, "ERROR: Invalid probability."
            sys.exit(1)

        if not len(args)>3:
            provide_username = None
            provide_password = None
            connection_schedule = None
        else:
            provide_username = args[3]
            if "\n" in provide_username:
                print >>sys.stderr, "ERROR: PROVIDE_USERNAME may not contain newline."
                sys.exit(1)

            # connection schedule
            connection_schedule=None

            if len(args)==5:
                try:
                    # check cron pattern format by trying to parse it
                    minute,hour,dom,month,dow = args[4].split()
                    scheduler.CronPattern(minute,hour,dom,month,dow)
                except ValueError:
                    print >>sys.stderr, "ERROR: Invalid format of cron line."
                    print >>sys.stderr, "Use 'MINUTE HOUR DAY_OF_MONTH MONTH DAY_OF_WEEK'"
                    print >>sys.stderr, "Valid fields are e.g. 5, */3, 5-10 or a comma-separated combination of them."
                    sys.exit(1)

                connection_schedule = args[4]
            else:
                minute = random.randrange(0,60)
                hour = random.randrange(0,24)
                connection_schedule = "%d %d * * *" % (minute, hour)

            print "Type the password we use to authenticate to the partner (password for %s)" % provide_username
            provide_password = read_password()

        print "Type the password the partner uses to authenticate to us (password for %s)" % partner_name
        accept_password = read_password()

        partner = database.get_partner(partner_name)
        if partner:
            partner.accept_password = accept_password
            partner.base_url = base_url
            partner.control_probability = control_probability
            partner.connection_schedule = connection_schedule
            partner.provide_username = provide_username
            partner.provide_password = provide_password
        else:
            partner = Partner(partner_name, accept_password, base_url, control_probability, connection_schedule, provide_username, provide_password)

        database.save_partner(partner)

    elif options.unkick or options.unkick_clear:
        try:
            partner_name, = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need partner name."
            sys.exit(1)

        success = database.unkick_partner(partner_name, options.unkick_clear)

        if success:
            print "Unkicked partner \"%s\"." % partner_name
        else:
            print "No partner named \"%s\"." % partner_name

    elif options.delete:
        try:
            partner_name, = args
        except ValueError:
            print >>sys.stderr, "ERROR: Need partner name."
            sys.exit(1)

        print "Deleting partner \"%s\"." % partner_name
        database.delete_partner(partner_name)

    else:
        parser.print_help()
