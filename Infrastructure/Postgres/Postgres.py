import psycopg2
import os
from io import StringIO


class Postgres(object):
    __database: str
    __conn = None
    __timeout: int = 30

    def __init__(self, database: str, timeout: int = 30):
        """
        :param database: Database name
        :param timeout: Connection timeout
        """
        self.__database = database
        self.__connect()
        self.__timeout = timeout

    def __connect(self):

        assert 'POSTGRE_HOST' in os.environ, 'POSTGRE_HOST env variable must be informed'
        assert 'POSTGRE_PORT' in os.environ, 'POSTGRE_PORT env variable must be informed'
        assert 'POSTGRE_USERNAME' in os.environ, 'POSTGRE_USERNAME env variable must be informed'
        assert 'POSTGRE_PASSWORD' in os.environ, 'POSTGRE_PASSWORD env variable must be informed'

        self.__conn = psycopg2.connect(host=os.environ['POSTGRE_HOST'],
                                       port=os.environ['POSTGRE_PORT'],
                                       database=self.__database,
                                       user=os.environ['POSTGRE_USERNAME'],
                                       password=os.environ['POSTGRE_PASSWORD'],
                                       connect_timeout=self.__timeout)

    def insert(self, query: str, params: list) -> int:
        """
        Execute an insert operation on database
        :param query: Write statement
        :param params: List of tuples with data to be inserted
        :return: Number of inserted rows.
        """
        try:
            if self.__conn.close:
                self.__connect()
            # create a new cursor
            cur = self.__conn.cursor()
            # execute the INSERT statement
            rows_count = cur.executemany(query, params)
            # commit the changes to the database
            self.__conn.commit()
            # close communication with the database
            cur.close()
            return rows_count
        except (Exception, psycopg2.DatabaseError) as error:
            self.__conn.rollback()
            raise error
        finally:
            if self.__conn is not None:
                self.__conn.close()

    def bulk_insert(self, table: str, data: str, columns: tuple, sep: str = '\t') -> None:
        """
        Function that apply a bulk insert at database
        :param table: table name where data will be insert
        :param data: Multiline string with data. Break line must be \n.
        :param columns: Columns to which data refers
        :param sep: Character column separator
        """
        try:
            if self.__conn.close:
                self.__connect()
            # create a new cursor
            cur = self.__conn.cursor()
            # execute the INSERT statement
            file = StringIO(data)
            cur.copy_from(file=file, table=table, sep=sep, columns=columns, null='None')
            self.__conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            self.__conn.rollback()
            raise error
        finally:
            if self.__conn is not None:
                self.__conn.close()

    def execute_query(self, query: str, params: tuple = None) -> None:
        """
        Execute a statement with no return.
        :param query: Query to be executed
        :param params: Query params

        """
        try:
            if self.__conn.close:
                self.__connect()
            cur = self.__conn.cursor()
            cur.execute(query, params)
            self.__conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            self.__conn.rollback()
            raise error
        finally:
            if self.__conn is not None:
                self.__conn.close()

    def read(self, query: str, params: tuple = None, to_dict: bool = False) -> list:
        """
        Retrieve data from database based on informed query
        :param query: Read statement
        :param params: Params of read statement (%s must be used)
        :param to_dict: If true, change function returns to a list o dict
        :return: List of tuple or dict
        """
        try:
            if self.__conn.close:
                self.__connect()
            cur = self.__conn.cursor()
            rows_count = cur.execute(query, params)
            if rows_count == 0:
                cur.close()
                return
            rows = cur.fetchall()
            cur.close()
            if to_dict:
                column_names = [desc[0] for desc in cur.description]
                return [dict(zip(column_names, x)) for x in rows]
            return rows
        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if self.__conn is not None:
                self.__conn.close()
