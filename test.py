#!/usr/bin/env python

"""
This module tests the functions provided by the entries module. It
will create a test database entry, run some checks and output some
information.

Two notes:
 - Do not run this while sduds.py is running, as both will try to
   run the trieserver executable.
 - This script does not only need the public key of the captcha
   provider, but also the private one. It must be placed at the path
   './captchakey'.
"""

# run the trieserver executable
from hashtrie import HashTrie
hashtrie = HashTrie()

# setup entries database
import entries
entrydb = entries.Database()

# cryptography functions
import paramiko

def get_private_key(path="captchakey"):
    private_key = paramiko.RSAKey(filename=path)
    return private_key

def sign(private_key, text):
    sig_message = private_key.sign_ssh_data(paramiko.randpool, text)
    sig_message.rewind()

    keytype = sig_message.get_string()
    assert keytype=="ssh-rsa"

    signature = sig_message.get_string()

    return signature

private_key = get_private_key()

import binascii

# test server providing a webfinger profile

import BaseHTTPServer, urlparse, urllib, json, threading

class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path=="/.well-known/host-meta":
            self.send_response(200, "OK")
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write("""<?xml version='1.0' encoding='UTF-8'?>
<XRD xmlns='http://docs.oasis-open.org/ns/xri/xrd-1.0'
     xmlns:hm='http://host-meta.net/xrd/1.0'>
 
    <hm:Host>localhost:3000</hm:Host>
 
    <Link rel='lrdd'
          template='http://localhost:3000/describe?uri={uri}'>
        <Title>Resource Descriptor</Title>
    </Link>
</XRD>""")
        elif self.path.startswith("/describe"):
            querystring = urlparse.urlparse(self.path).query
            args = urlparse.parse_qs(querystring)
            uri, = args["uri"]
            self.send_response(200, "OK")
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write("""<?xml version='1.0' encoding='UTF-8'?>
<XRD xmlns='http://docs.oasis-open.org/ns/xri/xrd-1.0'>

<Subject>"""+uri+"""</Subject>

<Link rel='http://hoegners.de/sduds/spec'
      href='http://localhost:3000/sduds?"""+urllib.urlencode({"uri":uri})+"""' />
</XRD>""")
        elif self.path.startswith("/sduds"):
            querystring = urlparse.urlparse(self.path).query
            args = urlparse.parse_qs(querystring)
            uri, = args["uri"]

            webfinger_address = uri.split("acct:",1)[-1]

            json_dict = {}
            json_dict["webfinger_address"] = webfinger_address
            json_dict["full_name"] = webfinger_address.split("@")[0]
            json_dict["hometown"] = u"Los Angeles"
            json_dict["country_code"] = u"US"
            json_dict["services"] = "diaspora,email"
            json_dict["captcha_signature"] = binascii.hexlify(sign(private_key, webfinger_address.encode("utf-8")))

            json_string = json.dumps(json_dict)

            self.send_response(200, "OK")
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(json_string)

def run_webfinger_profile_server():
    webfinger_profile_server = BaseHTTPServer.HTTPServer(('', 3000), RequestHandler)
    webfinger_profile_server.serve_forever()

wps_thread = threading.Thread(target=run_webfinger_profile_server)
wps_thread.daemon = True
wps_thread.start()

# create an example database entry, retrieving a webfinger profile from the test server

entry = entries.Entry.from_webfinger_address("JohnDoe@localhost:3000", 1290117971)

assert entry.captcha_signature_valid()

entrylist = entries.EntryList([entry])

# print entry
print str(entry)

# save it
hashes = entrylist.save(entrydb)
hashtrie.add(hashes)

# print hash
binhash = str(entry.hash)
hexhash = binascii.hexlify(binhash)
print hexhash

# test loading from database
entrylist2 = entries.EntryList.from_database(entrydb, [binhash])
entry2 = entrylist2[0]
hexhash2 = binascii.hexlify(entry2.hash)
assert hexhash2==hexhash

# test exporting a EntryList as JSON
json_string = entrylist2.json()
print json_string

# test loading a EntryList from JSON
entrylist3 = entries.EntryList.from_json(json_string)
entry3 = entrylist3[0]
hexhash3 = binascii.hexlify(entry3.hash)
assert hexhash3==hexhash

# run a EntryServer
entryserver_interface = "localhost"
entryserver_port = 20001
entryserver = entries.EntryServer(entrydb, entryserver_interface, entryserver_port)

# get a EntryList from the EntryServer
entrylist4 = entries.EntryList.from_server([binhash], (entryserver_interface, entryserver_port))
entry4 = entrylist4[0]
hexhash4 = binascii.hexlify(entry4.hash)
assert hexhash4==hexhash

hashtrie.close()
entryserver.terminate()
