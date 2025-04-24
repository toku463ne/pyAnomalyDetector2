import psycopg2
import time
from utils.config_loader import *
from jinja2 import Template
import pandas as pd

class PostgreSqlDB:
	def __init__(self, config):
		self.conn = None
		self.config = config
		self.retries = config.get('retries', 1)
		self.delay = config.get('delay', 3)

	def connect(self):
		config = self.config
		conn_string = "host=%s dbname=%s user=%s password=%s" % (
			config["host"],
			config["dbname"],
			config["user"],
			config["password"])
		conn = psycopg2.connect(conn_string)
		conn.autocommit = True

		# Connect to the specified schema
		schema = config.get("schema", "public")
		cur = conn.cursor()
		cur.execute(f"SET search_path TO {schema};")

		return conn
	

	def exec_sql(self, sql):
		retries = self.retries
		delay = self.delay
		cur = None
		for i in range(retries):
			try:
				cur = self.connect().cursor()
				cur.execute(sql)
				return cur
			except psycopg2.errors.SerializationFailure:
				if i < retries - 1:
					time.sleep(delay)  # Wait before retrying
				else:
					raise  # Give up after max retries
		raise Exception("SQL failed after max tries")
	

	def truncate_table(self, tablename):
		sql = "truncate table %s;" % tablename
		return self.exec_sql(sql)
		
	def count_table(self, tablename, whereList=[]):
		if self.table_exists(tablename) == False:
			return 0
		strwhere = ""
		if len(whereList) > 0:
			strwhere = "where %s" % (" and ".join(whereList))
		sql = "select count(*) as cnt from %s %s" % (tablename, strwhere)
		cur = self.exec_sql(sql)
		row = cur.fetchone()
		cur.close()
		return row[0]

	def close(self):
		if self.conn != None:
			self.conn.close()

	def _create_table_from_template(self, sqlFile, tableName, context={}):
		try:
			with open(sqlFile, "r") as f:
				template = Template(f.read())
			context["TABLENAME"] = tableName
			sql = template.render(context)
			cur = self.exec_sql(sql)
			cur.close()
		except:
			raise

	def create_table(self, tableName, templateName="", replaces={}):
		if templateName != "":
			self._create_table_from_template("%s/postgresql/create_table_%s.sql.j2" % (SQL_DIR, 
                                                        templateName), tableName, replaces)
		else:
			self._create_table_from_template("%s/postgresql/create_table_%s.sql" % (SQL_DIR, 
                                                        tableName), tableName)
	
	def table_exists(self, tableName, schema_name=""):
		schemacond = ""
		if schema_name != "":
			schemacond = f" AND schemaname = '{schema_name}'"
		sql = """SELECT EXISTS (
    SELECT FROM pg_catalog.pg_tables
    WHERE tablename  = '%s' %s
);
""" % (tableName.lower(), schemacond)

		(b,) = self.select1rec(sql)
		return b


	def drop_table(self, tableName):
		self.exec_sql("drop table if exists %s;" % tableName)

	def select1rec(self, sql):
		cur = self.exec_sql(sql)
		row = cur.fetchone()
		cur.close()
		if row:
			return row
		return None

	def select1value(self, tableName, field, whereList=[]):
		sql = "select %s from %s" % (field, tableName)
		if len(whereList) > 0:
			sql += " where %s" % (" and ".join(whereList))
		row = self.select1rec(sql)
		if row:
			(val,) = row
			return val
		return None

	def read_sql(self, sql) -> pd.DataFrame:
		cur = self.exec_sql(sql)
		rows = cur.fetchall()
		cur.close()
		return pd.DataFrame(rows)
	
	
	# create schema if not exists
	def create_schema(self, schema_name):
		sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
		self.exec_sql(sql)

def exec_sql(sql):
	return PostgreSqlDB().exec_sql(sql)