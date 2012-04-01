#!/usr/bin/env python

if __name__=="__main__":
    """
    Command line interface.
    """

    import logging
    logging.basicConfig(level=logging.DEBUG)

    import optparse, sys
    import socket, time

    from sduds.context import Context
    from sduds.application import *

    parser = optparse.OptionParser(
        usage = "%prog  [-p WEBSERVER_PORT] [-s SYNCHRONIZATION_PORT] [-f FQDN] [PARTNER]",
        description="run a sduds server or connect manually to another one"
    )

    parser.add_option( "-p", "--webserver-port", metavar="PORT", dest="webserver_port", help="the webserver port of the own server")
    parser.add_option( "-s", "--synchronization-port", metavar="PORT", dest="synchronization_port", help="the synchronization port of the own server")
    parser.add_option( "-f", "--fqdn", metavar="FQDN", dest="fqdn", help="the fully qualified domain name of the system")

    (options, args) = parser.parse_args()

    try:
        webserver_port = int(options.webserver_port)
    except TypeError:
        webserver_port = 20000
    except ValueError:
        print >>sys.stderr, "Invalid webserver port."
        sys.exit(1)

    try:
        synchronization_port = int(options.synchronization_port)
    except TypeError:
        synchronization_port = webserver_port + 1
    except ValueError:
        print >>sys.stderr, "Invalid synchronization port."
        sys.exit(1)

    interface = "localhost"

    context = Context(partnerdb_path="partners.sqlite", statedb_path="states.sqlite", hashtrie_path="PTree")
    sduds = Application(context)

    sduds.configure_workers()
    sduds.configure_web_server(interface, webserver_port)

    if len(args)>0:
        ### initiate connection if a partner is passed
        try:
            partner_name, = args
        except ValueError:
            print >>sys.stderr, "Invalid number of arguments."
            sys.exit(1)

        sduds.configure_jobs(synchronization=False, partnerdb_cleanup=False, statedb_cleanup=True)
        sduds.start(synchronization_server=False)

        # synchronize with another server
        if not context.partnerdb.get_partner(partner_name):
            print >>sys.stderr, "Unknown server - add it with partners.py."
            sduds.terminate()
            sys.exit(1)

        try:
            sduds.synchronize_with_partner(partner_name)
        except socket.error,e:
            print >>sys.stderr, "Connecting to %s failed: %s" % (partner_name, str(e))
            sys.exit(1)

        sduds.terminate()

    else:
        ### otherwise simply run a server

        sduds.configure_jobs()
        sduds.configure_synchronization_server(options.fqdn, "", synchronization_port)
        sduds.start()

        # define exitfunc
        def exit_function():
            global sduds
            sduds.terminate()
            sys.exitfunc = lambda: 0
            sys.exit(0)

        sys.exitfunc = exit_function

        # call exitfunc also for signals
        import signal

        def signal_handler(sig, frame):
            print >>sys.stderr, "Terminated by signal %d." % sig
            exit_function()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGHUP, signal_handler)

        # wait until program is interrupted
        while True: time.sleep(100)
