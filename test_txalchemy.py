from twisted.trial import unittest

from txalchemy import Connection, ConnectionPool

from sqlalchemy import Table, MetaData, \
    Column, Integer, String

class TXAlchemyTest(unittest.TestCase):

    def setUp(self):
        self.connection_pool = ConnectionPool()
        self.metadata = MetaData()
        self.test = Table('test', self.metadata,
           Column('id', Integer, primary_key=True),
           Column('a', String(50)),
           Column('b', String(50)),
           Column('c', String(12))
        )

    def test_create(self):
        def success2(arg):
            print "Succces2!"
            return

        def success(arg):
            print "Success!"
            d2 = self.connection_pool.runWithConnection('execute', ins)
            d2.addCallback(success2)
            return d2

        d = self.connection_pool.runWithEngine(self.metadata.create_all)
        d.addCallback(success)
        ins = self.test.insert()
        return d


