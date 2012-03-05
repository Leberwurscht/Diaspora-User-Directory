#!/usr/bin/env python

import sqlalchemy, sqlalchemy.orm

class Binary(sqlalchemy.types.TypeDecorator):
    """ This type allows saving str objects in Binary columns. It converts between
        str and buffer automatically. """

    impl = sqlalchemy.types.Binary

    def process_bind_param(self, value, dialect):
        if value is None: return None

        return buffer(value)

    def process_result_value(self, value, dialect):
        if value is None: return None

        return str(value)

    def copy(self):
        return Binary(self.impl.length)

class Text(sqlalchemy.types.TypeDecorator):
    """ This type is a workaround for the fact that sqlite returns only unicode but not
        str objects. We need str objects to save URLs for example. """

    impl = sqlalchemy.types.UnicodeText

    def process_bind_param(self, value, dialect):
        if value is None: return None

        return unicode(value, "latin-1")

    def process_result_value(self, value, dialect):
        if value is None: return None

        return value.encode("latin-1")

    def copy(self):
        return Text(self.impl.length)

class CalculatedPropertyExtension(sqlalchemy.orm.MapperExtension):
    """ helper class to get sqlalchemy mappings of calculated properties
        (see http://stackoverflow.com/questions/3020394/sqlalchemy-how-to-map-against-a-read-only-or-calculated-property) """
    def __init__(self, properties):
        self.properties = properties

    def _update_properties(self, instance):
        for prop, synonym in self.properties.iteritems():
            value = getattr(instance, prop)
            setattr(instance, synonym, value)

    def before_insert(self, mapper, connection, instance):
        self._update_properties(instance)

    def before_update(self, mapper, connection, instance):
        self._update_properties(instance)
