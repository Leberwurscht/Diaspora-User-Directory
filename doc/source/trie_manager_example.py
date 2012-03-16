import subprocess, struct, threading

# packet forwarder function
def forward_packets(cout, cin):
    while True:
        # read packet from cout
        announcement = cout.read(1)
        packet_length, = struct.unpack("!B", announcement)
        packet = cout.read(packet_length)
        assert len(packet)==packet_length # i.e. no EOF

        # forward packet to cin
        cin.write(announcement)
        cin.write(packet)
        cin.flush()

        # terminate after packet of length 0
        if packet_length==0: return

# run two manager instances: server and client
executable = "trie_manager/manager"
manager_server = subprocess.Popen([executable, "server.bdb", "server"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
manager_client = subprocess.Popen([executable, "client.bdb", "client"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

# send synchronization commands
manager_server.stdin.write("SYNCHRONIZE_AS_SERVER\n")
manager_server.stdin.flush()
response = manager_server.stdout.readline()
assert response=="OK\n"

manager_client.stdin.write("SYNCHRONIZE_AS_CLIENT\n")
manager_client.stdin.flush()
response = manager_client.stdout.readline()
assert response=="OK\n"

# establish tunnel between manager instances
server2client = threading.Thread(target=forward_packets, args=(manager_server.stdout, manager_client.stdin))
client2server = threading.Thread(target=forward_packets, args=(manager_client.stdout, manager_server.stdin))

server2client.start()
client2server.start()

# wait until synchronization is completed and tunnel is closed
server2client.join()
client2server.join()

# print missing hashes of the server
response = manager_server.stdout.readline()
assert response=="NUMBERS\n"

print "missing hashes of server:"
while True:
    hex_number = manager_server.stdout.readline().strip()
    print hex_number

    if not hex_number: break

response = manager_server.stdout.readline()
assert response=="DONE\n"

# print missing hashes of the client
response = manager_client.stdout.readline()
assert response=="NUMBERS\n"

print "missing hashes of client:"
while True:
    hex_number = manager_client.stdout.readline().strip()
    print hex_number

    if not hex_number: break

response = manager_client.stdout.readline()
assert response=="DONE\n"

# close both manager instances
manager_server.stdin.write("EXIT\n")
manager_client.stdin.write("EXIT\n")
manager_server.stdin.flush()
manager_client.stdin.flush()
