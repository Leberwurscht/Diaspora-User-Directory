#!/usr/bin/env python

import subprocess

t1 = subprocess.Popen(["./test", "PTree1", "ptree1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
t2 = subprocess.Popen(["./test", "PTree2", "ptree2"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

t1.stdin.write("SYNCHRONIZE_AS_SERVER\n")
t1.stdin.flush()
t2.stdin.write("SYNCHRONIZE_AS_CLIENT\n")
t2.stdin.flush()

assert t1.stdout.readline()=="OK\n"
assert t2.stdout.readline()=="OK\n"

import select

channels = [t1.stdout,t2.stdout]

while channels:
    inputready,outputready,exceptready = select.select(channels,[],[])

    for cin in inputready:
        if cin==t1.stdout:
            debug_name = "t1"
            cout=t2.stdin
        elif cin==t2.stdout:
            debug_name = "t2"
            cout=t1.stdin

        announcement = cin.read(1)
        message_length = ord(announcement)

        print debug_name, "announced", message_length

        message = ""
        while len(message)<message_length:
            message += cin.read(message_length - len(message))

        print debug_name, "sent", message_length

        cout.write(announcement)
        cout.write(message)
        cout.flush()

        print "message of", debug_name, "transmitted"

        if message_length==0: channels.remove(cin)

assert t1.stdout.readline()=="DONE\n"
assert t2.stdout.readline()=="DONE\n"

print "communication finished"

t1.stdin.write("EXIT\n")
t1.stdin.flush()
t2.stdin.write("EXIT\n")
t2.stdin.flush()
print "sent EXIT"

print "wait for t1"
t1.wait()
print "wait for t2"
t2.wait()
