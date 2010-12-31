#!/usr/bin/env python

# cryptography functions
import paramiko, os

def get_private_key(path="captchakey"):
    if not os.path.exists(path):
        raise Exception, "Private key '%s' not found!" % path

    private_key = paramiko.RSAKey(filename=path)
    return private_key

def sign(private_key, text):
    sig_message = private_key.sign_ssh_data(paramiko.randpool, text)
    sig_message.rewind()

    keytype = sig_message.get_string()
    assert keytype=="ssh-rsa"

    signature = sig_message.get_string()

    return signature

# load private key from the default location
private_key = get_private_key()

# run a test server providing a webfinger profile
import BaseHTTPServer, urlparse, urllib, json, binascii
import threading

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

class ProfileServer(threading.Thread):
    def __init__(self, interface, port):
        threading.Thread.__init__(self)

        self.address = (interface, port)
        self.daemon = True
        self.start()

    def run(self):
        webfinger_profile_server = BaseHTTPServer.HTTPServer(self.address, RequestHandler)
        webfinger_profile_server.serve_forever()
