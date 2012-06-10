"""
An asynchronous mapping to SQLAlchemy.
"""

import sys

from twisted.internet import threads, reactor
from twisted.python import reflect, log
from twisted.python.deprecate import deprecated
from twisted.python.versions import Version

from sqlalchemy import orm
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.interfaces import MapperExtension, SessionExtension,\
     EXT_CONTINUE
from sqlalchemy.interfaces import ConnectionProxy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.util import to_list

class Connection(object):
    def __init__(self, pool):
        self._pool = pool
        self._connection = None
        self.reconnect()

    def reconnect(self):
        if self._connection is not None:
            self._pool.disconnect(self._connection)
        self._connection = self._pool.connect()

    def close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._connection, name)

class ConnectionPool(object):
    connectionFactory = Connection
    min = 3
    max = 5
    running = False

    def __init__(self, engine_type="sqlite:///test.db",
                 engine_options=dict(), session_options=None):

        self.engine = create_engine(engine_type, **engine_options)
        self._reactor = reactor

        self.connections = {}
        import thread
        from twisted.python import threadpool
        self.threadID = thread.get_ident
        self.threadpool = threadpool.ThreadPool(self.min, self.max)
        self.startID = self._reactor.callWhenRunning(self._start)
        log.startLogging(sys.stdout)

    def _start(self):
        self.startID = None
        return self.start()

    def start(self):
        if not self.running:
            self.threadpool.start()
            self.shutdownID = self._reactor.addSystemEventTrigger(
                    'during', 'shutdown', self.finalClose)
            self.running = True

    def runWithEngine(self, func, *args, **kw):
        """
        Execute a function with the engine as argument.

        @param func: The function to be called with argument the current
                     database engine.
        """
        from twisted.internet import reactor
        return threads.deferToThreadPool(reactor, self.threadpool,
                                         self._runWithEngine,
                                         func, *args, **kw)

    def _runWithEngine(self, func, *args, **kw):
        result = func(self.engine, *args, **kw)
        return result

    def runWithConnection(self, method, *args, **kw):
        """
        Call the specified method of a connection with the desired arguments.

        @param method: a string representing the method to be called on
                       a connection.
        @param args: The arguments to be passed to the connection method.

        @param kw: The keyword arguments to be passed to the method of
                   connection.
        """
        from twisted.internet import reactor
        return threads.deferToThreadPool(reactor, self.threadpool,
                                         self._runWithConnection,
                                         method, *args, **kw)

    def _runWithConnection(self, method, *args, **kw):
        conn = self.connectionFactory(self)
        try:
            f = getattr(conn, method)
            result = f(*args, **kw)
            return result
        except:
            excType, excValue, excTraceback = sys.exc_info()
            try:
                conn.rollback()
            except:
                log.err(None, "Roolback failed")
            raise excType, excValue, excTraceback

    def connect(self):
        tid = self.threadID()
        conn = self.connections.get(tid)
        if conn is None:
            conn = self.engine.connect()
            self.connections[tid] = conn
        return conn

    def close(self):
        """
        Close all pool connections and shutdown the pool.
        """
        if self.shutdownID:
            self._reactor.removeSystemEventTrigger(self.shutdownID)
            self.shutdownID = None
        if self.startID:
            self._reactor.removeSystemEventTrigger(self.startID)
            self.startID = None
        self.finalClose()

    def _close(self, conn):
        conn.close()

    def finalClose(self):
        """This should only be called by the shutdown trigger."""

        self.shutdownID = None
        self.threadpool.stop()
        self.running = False
        for conn in self.connections.values():
            self._close(conn)
        self.connections.clear()

