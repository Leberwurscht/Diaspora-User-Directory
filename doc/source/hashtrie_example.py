import socket, binascii

from sduds import hashtrie

# create a hash trie and save a hash in it
server_trie = hashtrie.HashTrie("server.bdb")

hexhash = "00112233445566778899AABBCCDDEEFF"
binhash = binascii.unhexlify(hexhash)
server_trie.add([binhash])

assert server_trie.contains(binhash)

# create another hash trie
client_trie = hashtrie.HashTrie("client.bdb")

# create a socketpair
server_socket, client_socket = socket.socketpair()

# start a thread for the server side
import threading
thread = threading.Thread(target=server_trie.get_missing_hashes_as_server, args=(server_socket,))
thread.start()

# let client determine the missing hashes
missing_hashes = client_trie.get_missing_hashes_as_client(client_socket)
thread.join()

# print the missing hashes
for binhash in missing_hashes:
    hexhash = binascii.hexlify(binhash)
    print hexhash

# close hash tries
server_trie.close()
client_trie.close()
