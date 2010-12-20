#!/usr/bin/env python

import sqlalchemy.types

class String(sqlalchemy.types.TypeDecorator):
    """ This type is a workaround for the fact that sqlite returns only unicode but not
        str objects. We need str objects to save URLs for example. """

    impl = sqlalchemy.types.Binary

    def process_bind_param(self, value, dialect):
        return buffer(value)

    def process_result_value(self, value, dialect):
        return str(value)

    def copy(self):
        return String(self.impl.length)

