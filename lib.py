#!/usr/bin/env python

import sqlalchemy.types

class Binary(sqlalchemy.types.TypeDecorator):
    """ This type allows saving str objects in Binary columns. It converts between
        str and buffer automatically. """

    impl = sqlalchemy.types.Binary

    def process_bind_param(self, value, dialect):
        return buffer(value)

    def process_result_value(self, value, dialect):
        return str(value)

    def copy(self):
        return Binary(self.impl.length)


class Text(sqlalchemy.types.TypeDecorator):
    """ This type is a workaround for the fact that sqlite returns only unicode but not
        str objects. We need str objects to save URLs for example. """

    impl = sqlalchemy.types.UnicodeText

    def process_bind_param(self, value, dialect):
        return unicode(value, "latin-1")

    def process_result_value(self, value, dialect):
        return value.encode("latin-1")

    def copy(self):
        return Text(self.impl.length)
