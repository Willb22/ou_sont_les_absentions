import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, select, distinct
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import registry
import os
import sys
from resource import getrusage, RUSAGE_SELF
from config import configurations


def log_memory_after(message):
	memory_message = f"Max Memory after {message} (MiB): {int(getrusage(RUSAGE_SELF).ru_maxrss / 1024)} \n"
	return memory_message

database_name = configurations['database']
query_aws_table = configurations['query_aws_table']
class User:
    pass


class Connectdb:
	def __init__(self, database_name, query_aws_table):
		self.query_aws_table=query_aws_table
		self.database_name = database_name

	def get_credentials(self, use_aws):
		"""
		Return user, password, host and port for database and table connection

		Parameters
		---------
		Nothing is passed. user, password, host and port are defined within the function

		Returns
		-------
		string
		    user, password, host and port
		"""
		user = 'postgres'
		if use_aws:
			host = 'ec2-54-173-241-113.compute-1.amazonaws.com' # Public IPv4 DNS, contains elastic IP to AWS instance
			port = os.environ.get('PORT_POSTGRESQL_AWS')
			passw = os.environ.get('PASSPOSTGRES')
		else:
			host = '127.0.0.1'
			port = os.environ.get('PORT_POSTGRESQL')
			passw = os.environ.get('PASSPOSTGRES')

		return user, passw, host, port

	def connect_driver(self):
		"""
        Return psycopg2 connection and cursor objects

        Parameters
        ---------
        None

        Returns
        -------
        connction and cursor class
        """
		user, passw, host, port = self.get_credentials(self.query_aws_table)

		# establishing the connection
		conn = psycopg2.connect(
			user=user, password=passw, host=host, port=port
		)
		conn.autocommit = True
		# Creating a cursor object using the cursor() method
		cursor = conn.cursor()
		return conn, cursor

	def connect_orm(self):
		"""
        Return sqlalchemy Engine instance and connection

        Parameters
        ---------
        None

        Returns
        -------
        sqlalchemy Engine instance and connection
        """
		user, passw, host, port = self.get_credentials(self.query_aws_table)

		conn_string = f'postgresql://{user}:{passw}@{host}:{port}/{database_name}'
		engine = create_engine(conn_string, pool_size=42)
		conn_orm = engine.connect()
		return conn_orm, engine
