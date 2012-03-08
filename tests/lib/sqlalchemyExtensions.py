import unittest

from sduds.lib import sqlalchemyExtensions

import sqlalchemy
import sqlalchemy, sqlalchemy.orm, sqlalchemy.ext.declarative

# define class and mapping for the custom types test with declarative
DatabaseObject = sqlalchemy.ext.declarative.declarative_base()

class BinaryStringTest(DatabaseObject):
    __tablename__ = "test_binary_string"
    id = sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True)
    bin_data = sqlalchemy.Column("bin_data", sqlalchemyExtensions.Binary)
    str_data = sqlalchemy.Column("str_data", sqlalchemyExtensions.String)

# define class for the calculated property extension test
class PropertyTest(object):
    __tablename__ = "test_property"

    @property
    def calculated_data(self):
        return 42

# define mapping for the calculated property extension test
metadata = sqlalchemy.MetaData()

propertytest_table = sqlalchemy.Table('test', metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("calculated_data", sqlalchemy.Float)
)

sqlalchemy.orm.mapper(PropertyTest, propertytest_table,
    extension=sqlalchemyExtensions.CalculatedPropertyExtension({"calculated_data":"_calculated_data"}),
    properties={
        "calculated_data": sqlalchemy.orm.synonym('_calculated_data', map_column=True),
    }
)

class CustomTypes(unittest.TestCase):
    def setUp(self):
        # create in-memory database
        engine = sqlalchemy.create_engine("sqlite://")
        DatabaseObject.metadata.create_all(engine)
        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)
        self.addCleanup(self.Session.close_all)

        # create one database entry for testing
        s = self.Session()
        t = BinaryStringTest()
        t.bin_data = "abc"
        t.str_data = "abc"
        s.add(t)
        s.commit()
        s.close()

    def test_binary(self):
        # make sure that the bin_data property of the database entry has the
        # right type and content
        s = self.Session()
        t = s.query(BinaryStringTest).one()
        self.assertEqual(type(t.bin_data), str)
        self.assertEqual(t.bin_data, "abc")
        s.close()

    def test_str(self):
        # make sure that the str_data property of the database entry has the
        # right type and content
        s = self.Session()
        t = s.query(BinaryStringTest).one()
        self.assertEqual(type(t.str_data), str)
        self.assertEqual(t.str_data, "abc")
        s.close()

class CalculatedPropertyExtension(unittest.TestCase):
    def setUp(self):
        # create in-memory database
        engine = sqlalchemy.create_engine("sqlite://")
        metadata.create_all(engine)
        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)
        self.addCleanup(self.Session.close_all)

        # create one database entry for testing
        s = self.Session()
        t = PropertyTest()
        s.add(t)
        s.commit()
        s.close()

    def test(self):
        # make sure we can find the database entry by filtering by the
        # calculated property
        s = self.Session()
        q = s.query(PropertyTest).filter_by(calculated_data=42)
        results = q.count()
        self.assertEqual(results, 1)
        s.close()

if __name__ == '__main__':
    unittest.main()
