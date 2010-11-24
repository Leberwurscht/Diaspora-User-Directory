#!/usr/bin/env python

"""
This module will run the trieserver executable when imported, and take care of
terminating it when the interpreter terminates.
"""

import os, sys, time, subprocess, signal

# delete remaining unix domain sockets
for name in ["server","client","add"]:
    path = name+".ocaml2py.sock"
    if os.path.exists(path): os.remove(path)

# run trieserver
trieserver = subprocess.Popen("./trieserver")

# define exitfunc
def exit():
    global trieserver

    trieserver.terminate()
    sys.exit(0)

sys.exitfunc = exit

# call exitfunc also for signals
def signal_handler(signal, frame):
    print >>sys.stderr, "Server terminated by signal %d." % signal
    exit()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)

# wait until all unix domain sockets are set up by trieserver
while True:
    time.sleep(1)

    if not os.path.exists("server.ocaml2py.sock"): continue
    if not os.path.exists("client.ocaml2py.sock"): continue
    if not os.path.exists("add.ocaml2py.sock"): continue

    break
