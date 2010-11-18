#!/usr/bin/env python

import entries

webfinger_address = "test@example.com"
full_name = "John Doe"
hometown = "Los Angeles"
country_code = "US"
captcha_signature = ""

e = entries.Entry(webfinger_address, full_name, hometown, country_code, captcha_signature)
con = entries.get_db_connection()
e.save(con.cursor(), con)
