#!/usr/bin/env python

"""
This module defines two custom sqlalchemy types for dealing with strings (:class:`Binary`
and :class:`String`), and one extension to map calculated properties (:class:`CalculatedPropertyExtension`).

.. _usage:

Usage::

    # define a class with two string properties that will be mapped to Binary
    # and Unicode columns, and one calculated property
    class Test(object):
        bin_data = None
        str_data = None

        @property
        def calculated_data(self):
            return time.time()

    # define table using the two custom types
    table = sqlalchemy.Table('test', metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("bin_data", Binary),
        sqlalchemy.Column("str_data", String),
        sqlalchemy.Column("calculated_data", sqlalchemy.Float)
    )

    # define mapping using CalculatedPropertyExtension
    sqlalchemy.orm.mapper(Test, table,
        extension=CalculatedPropertyExtension({"calculated_data":"_calculated_data"}),
        properties={
            "bin_data": table.c.bin_data,
            "str_data": table.c.str_data,
            "calculated_data": sqlalchemy.orm.synonym('_calculated_data', map_column=True),
        }
    )
"""

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

class String(sqlalchemy.types.TypeDecorator):
    """ This type allows saving str objects in Unicode columns.

        It is a workaround for the fact that sqlite returns only unicode but not
        str objects, even if ``sqlalchemy.String`` is used. We need str objects to
        save URLs for example.
    """

    impl = sqlalchemy.types.UnicodeText

    def process_bind_param(self, value, dialect):
        if value is None: return None

        return unicode(value, "latin-1")

    def process_result_value(self, value, dialect):
        if value is None: return None

        return value.encode("latin-1")

    def copy(self):
        return String(self.impl.length)

class CalculatedPropertyExtension(sqlalchemy.orm.MapperExtension):
    """ This is a sqlalchemy extension to map calculated properties (``@property`` decorator).
        See this `stackoverflow question`_.

        To use it, you have to set the respective entry in the properties dictionary of the
        mapping to ``sqlalchemy.orm.synonym("synonym_name", map_column=True)`` and add an
        instance of this extension (see `usage`_).

        .. _stackoverflow question: http://stackoverflow.com/questions/3020394/sqlalchemy-how-to-map-against-a-read-only-or-calculated-property
    """
    def __init__(self, properties):
        """ :param properties: specifies the respective synonym name for each property
                               that should be mapped
            :type properties: dict
        """
        self.properties = properties

    def _update_properties(self, instance):
        for prop, synonym in self.properties.iteritems():
            value = getattr(instance, prop)
            setattr(instance, synonym, value)

    def before_insert(self, mapper, connection, instance):
        self._update_properties(instance)

    def before_update(self, mapper, connection, instance):
        self._update_properties(instance)
