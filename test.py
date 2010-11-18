#!/usr/bin/env python

import entries, binascii

webfinger_address = "test@example.com"
full_name = "John Doe"
hometown = "Los Angeles"
country_code = "US"
captcha_signature = ""
timestamp = 1290117971

e = entries.Entry(webfinger_address, full_name, hometown, country_code, captcha_signature, timestamp)
con = entries.get_db_connection()
e.save(con.cursor(), con)

print binascii.hexlify(e.hash)
