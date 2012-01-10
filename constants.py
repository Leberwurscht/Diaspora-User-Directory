#!/usr/bin/env python

MAX_AGE = 3600*24*3 # specifies how long ago a state transmitted by a partner
                    # may have been retrieved so that we still don't have to
                    # retrieve it ourselves

STATEDB_CLEANUP_INTERVAL = 3600*24
EXPIRY_GRACE_PERIOD = 3600*24*3 # if states transmitted by a partner are
                                # expired, only reduce trust in him if grace
                                # period is over.

MIN_RESUBMISSION_INTERVAL = 3600*24*3
STATE_LIFETIME = 3600*24*365

MAX_ADDRESS_LENGTH = 1024
MAX_NAME_LENGTH = 1024
MAX_HOMETOWN_LENTGTH = 1024
MAX_COUNTRY_CODE_LENGTH = 2
MAX_SERVICES_LENGTH = 1024
MAX_SERVICE_LENGTH = 16

CAPTCHA_PUBLIC_KEY = "AAAAB3NzaC1yc2EAAAABIwAAAQEAyxhRjXXXmTxI3c8IqAsbw+idaXfwWkkiVE0/9jn1oVFdYsIQqm+7rkdcjVPa8zJnoYPYupCbMX0TB7hIrLOfQcQzb9PRLZ9KSCbY6Q7tShSylOO9aaNtG2Q+iHvpckNFp/dThdUDK7YqcYcPtQQFVsDPToehrbbCvHZm2wHRB614u8jZVXe+jnxmxFxdTIg2TxICbqHc3OAb2w8FS62U5yI5x/dZS1zVNW0exdci7BZYOZv/5xw5dd2zsQxiXA5n/Hs+F6Xn7LUKBh6cqEkwuvvQhoO9ieDt5V6nzJPJMHKZtW7TFYZKt3C/3wtoHOPSsZMUVvIcSKjRHd5xOddJvQ==" #TODO: only for testing
