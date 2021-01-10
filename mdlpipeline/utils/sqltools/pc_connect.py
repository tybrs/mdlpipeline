import pymssql
import pandas as pd
from os import getenv


class PcConnect:
    """Database handler class for runninf quiries on
    PowerCampus databases.
    """
    def __init__(self, environment, user='', password=''):
        self.user = user or getenv('MS_USER')
        self.password = password or getenv('MS_PW')
        self.db_name = self.get_db_name(environment)
        self.db_host = self.get_db_host(environment)

    def get_db_name(self, environment):
        return {
            'test': 'PowerCampus_prd_test',
            'prod': 'PowerCampus_prd',
        }.get(environment)

    def get_db_host(self, environment):

        return {
            'prod':
            'uwssqlprd01.uwsnet.net',
            'test':
            'uwssqltest01.uwsnet.net'
        }.get(environment)


    def get_connection(self):
        host = self.db_host
        try:
            conn = pymssql.connect(
                server=self.db_host,
                database=self.db_name,
                user=self.user,
                password=self.password)

        except EnvironmentError:
            print(f"Unable to connect to the database: {EnvironmentError}")
        return conn

    def connect_to_db(self):
        connection = self.get_connection()
        cur = connection.cursor()
        return cur

    def get_records(self, query):
        with self.connect_to_db() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()
            # list of records as tuples
            return records

    def get_record(self, query, result=0):
        with self.connect_to_db() as cursor:
            cursor.execute(query)
            # tuple containing first record
            record = cursor.fetchone()
            return record

    def get_value(self, query, result=0):
        with self.connect_to_db() as cursor:
            cursor.execute(query)
            # tuple containing first record
            record = cursor.fetchone()
            if len(record) == 1:
                record = record[0]
                return record
            else:
                raise ValueError('More than one value for record'
                                 ' {record}'.format(record=record))

    def get_list(self, query, result=0):
        with self.connect_to_db() as cursor:
            cursor.execute(query)
            # tuple containing first record
            records = cursor.fetchall()
            return list(map(lambda record: record[0], records))

    def get_df(self, query):
        with self.connect_to_db() as cursor:
            cursor.execute(query)
            names = [item[0] for item in cursor.description]
            records = cursor.fetchall()
            return pd.DataFrame(records, columns=names)
