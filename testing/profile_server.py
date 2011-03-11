#!/usr/bin/env python

# cryptography functions
import paramiko, os

def get_private_key(path):
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
private_key_directory = os.path.dirname(__file__)
private_key_path = os.path.join(private_key_directory ,"captchakey")
private_key = get_private_key(private_key_path)

# run a test server providing a webfinger profile
import BaseHTTPServer, urlparse, urllib, json, binascii, random
import threading, time

class Profile:
    def __init__(self, address, **kwargs):
        self.address = address

        if "name" in kwargs:
            self.name = kwargs["name"]
        else:
            self.name = self.address.split("@")[0]
        
        if "hometown" in kwargs:
            self.hometown = kwargs["hometown"]
        else:
            self.hometown = "Los Angeles"

        if "country_code" in kwargs:
            self.country_code = kwargs["country_code"]
        else:
            self.country_code = "US"

        if "services" in kwargs:
            self.services = kwargs["services"]
        else:
            self.services = "diaspora,email"

        if "submission_timestamp" in kwargs:
            self.submission_timestamp = kwargs["submission_timestamp"]
        else:
            self.submission_timestamp = time.time()

        if "captcha_signature" in kwargs:
            self.captcha_signature = kwargs["captcha_signature"]
        else:
            self.captcha_signature = sign(private_key, self.address.encode("utf-8"))

    def json(self):
        json_dict = {}
        json_dict["webfinger_address"] = self.address
        json_dict["full_name"] = unicode(self.name)
        json_dict["hometown"] = unicode(self.hometown)
        json_dict["country_code"] = unicode(self.country_code)
        json_dict["services"] = unicode(self.services)
        json_dict["submission_timestamp"] = self.submission_timestamp
        json_dict["captcha_signature"] = unicode(binascii.hexlify(self.captcha_signature))

        json_string = json.dumps(json_dict)
        
        return json_string

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

            if not webfinger_address in self.server.profiles: return

            profile = self.server.profiles[webfinger_address]
            json_string = profile.json()

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

        self.profiles = {}

    def run(self):
        webfinger_profile_server = BaseHTTPServer.HTTPServer(self.address, RequestHandler)
        webfinger_profile_server.profiles = self.profiles
        webfinger_profile_server.serve_forever()
