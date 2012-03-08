import unittest

from sduds.lib import signature

# define a key pair
private_key_block = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAyxhRjXXXmTxI3c8IqAsbw+idaXfwWkkiVE0/9jn1oVFdYsIQ
qm+7rkdcjVPa8zJnoYPYupCbMX0TB7hIrLOfQcQzb9PRLZ9KSCbY6Q7tShSylOO9
aaNtG2Q+iHvpckNFp/dThdUDK7YqcYcPtQQFVsDPToehrbbCvHZm2wHRB614u8jZ
VXe+jnxmxFxdTIg2TxICbqHc3OAb2w8FS62U5yI5x/dZS1zVNW0exdci7BZYOZv/
5xw5dd2zsQxiXA5n/Hs+F6Xn7LUKBh6cqEkwuvvQhoO9ieDt5V6nzJPJMHKZtW7T
FYZKt3C/3wtoHOPSsZMUVvIcSKjRHd5xOddJvQIBIwKCAQEAi0PgJnyxGJ5d2e0N
QAeeAq4iy/p47XP6SG98URHNAOdV+pOzqBIaS56l3UDQpsN6Qt4Q9PVxu4j3GzyJ
mv7Tms+uPg2WwDK2l+AftcEXvcUMvd3+Oc8mPqsjkMn/Kce6vFHS321+hF+oE1VM
mWHXxnWVd612LfmqGtTY0LDJ2V/JSvVGRtr/a4wAi9KDn/XQXiFDtry47WgM5bnE
vJ97/UAU3SaTqnEQ1iL7xA9G8Plx4Khzux7Rv1hWZdtAD9FXgUjqXu33tOuZk3cS
GCAzfZHqN5AoGxl14KraxaMWAODH/HD3aZfH/rPHoOQg3bNKcyqqYeagGy58zZtv
Ao29iwKBgQDutEhuipiD/reAPEDV8D8CtGGfGAT/lqULc4seEikyR2OvhYrtaAN/
tDTGWzdz0Z0Q2Y6q/E1UurEngYRyIi4N9s4bMgJKAWCXi9aeie11O0QUTFJM1+al
y4Xer+LjNw0rKOBEzj8sr8VIloKkz+jPcjmS/Cf4UcvCHnLyEi5grQKBgQDZz4Vk
TjUwvZi7EYiHaFgOZr/glsmk7ANS7dT9pnEjI8EWf+l/NXRySO/+l168+w2BTcJW
z0Hy2XZjhagyzXm6aSLNGzM/WNIygzpZEIIbE0LxU9SLinUm3N+fZyMQjXL1kb8l
d/RvtDqe0zwTXwA2rLtI4VnPzzlDqOjSmUTfUQKBgQC4JKznj3z4HERqPRwSwKWj
AC4NA+aZSFNvO+BZBrIQ1/xxdaWv0+VxJJ29ltMBkxLD2weoeX174HoIiHwdiBTm
M2vL1h8F484rw6WQPoP7WZrrFk4eBaNMsvI+Eqe2mC665QTHXUatcabRmK3s2ubL
6mbtuzTG4AOVv7fCDgaFFwKBgBKrYzR7u2qT6IUQIaU01FkBfikxfv+B8agFwcyZ
POXBPG+kkFtcWnAyIzMUSfLw8odtEKha6GVF1vKWbYCyhsbVz8hwC7T4/BL1TiTk
KGi4ghSvaf1VArowMGy/soxj5Ug/sUxavS4lZBw9/dXGUHm3CL0aoUxTlzGvZGnS
n4DbAoGBAJOX+2yWGdOmGEM2AVQHeLL0ramQyekZ0GiVFCgi8fL6sO5XhbYoG4r5
VzxTYB161OPL0CjoYxTx15g+aS0wS39xxoQ6iEW0WqKxxUA03TzQcSNkT9rvV3Bm
UpISaDxaSb8WXkNyUQ7ph/lkcmXCtqFvrdYmqTynBFWbE5nXsyrU
-----END RSA PRIVATE KEY-----"""
public_key_base64 = "AAAAB3NzaC1yc2EAAAABIwAAAQEAyxhRjXXXmTxI3c8IqAsbw+idaXfwWkkiVE0/9jn1oVFdYsIQqm+7rkdcjVPa8zJnoYPYupCbMX0TB7hIrLOfQcQzb9PRLZ9KSCbY6Q7tShSylOO9aaNtG2Q+iHvpckNFp/dThdUDK7YqcYcPtQQFVsDPToehrbbCvHZm2wHRB614u8jZVXe+jnxmxFxdTIg2TxICbqHc3OAb2w8FS62U5yI5x/dZS1zVNW0exdci7BZYOZv/5xw5dd2zsQxiXA5n/Hs+F6Xn7LUKBh6cqEkwuvvQhoO9ieDt5V6nzJPJMHKZtW7TFYZKt3C/3wtoHOPSsZMUVvIcSKjRHd5xOddJvQ=="

class Signature(unittest.TestCase):
    def setUp(self):
        self.data = "1234567890abcdefghijklmnopqrstuvwxyz"
        self.signature = signature.sign(private_key_block, self.data)

    def test_valid(self):
        valid = signature.signature_valid(public_key_base64, self.signature, self.data)
        self.assertTrue(valid)

    def test_invalid_signature(self):
        invalid_signature = "\0" * len(self.signature)
        valid = signature.signature_valid(public_key_base64, invalid_signature, self.data)
        self.assertFalse(valid)

    def test_invalid_data(self):
        invalid_data = "000000000000000000000"
        valid = signature.signature_valid(public_key_base64, self.signature, invalid_data)
        self.assertFalse(valid)

if __name__ == '__main__':
    unittest.main()
