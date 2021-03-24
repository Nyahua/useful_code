"""Database client."""
import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor


# configuration
class config:
    DATABASE_HOST = "localhost"
    DATABASE_USERNAME = "karibu"
    DATABASE_PASSWORD = "123456"
    DATABASE_PORT = "5432"
    DATABASE_NAME = "testdb"


class PostgresDB:
    """PostgreSQL Database class."""

    def __init__(self, config):
        self.host = config.DATABASE_HOST
        self.username = config.DATABASE_USERNAME
        self.password = config.DATABASE_PASSWORD
        self.port = config.DATABASE_PORT
        self.dbname = config.DATABASE_NAME
        self.conn = None

    def connect(self):
        """Connect to a Postgres database."""
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    user=self.username,
                    password=self.password,
                    port=self.port,
                    dbname=self.dbname
                )
            except psycopg2.DatabaseError as e:
                print(e)
                raise e
            finally:
                print(
                    f'Database "{self.dbname}" connected successfully via port {self.port}.')

    def select_sql(self, query):
        """Run SELECT query and return list of dicts."""
        # self.connect()
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query)
            records = cur.fetchall()
        cur.close()
        return records

    def show_tables(self):
        """Show Table names."""
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;"""
        return self.select_sql(sql)

    def show_table_columns(self, tablename: str):
        """Show Table columns """
        sql = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns
            WHERE table_name = '{tablename}';"""
        return self.select_sql(sql)


db = PostgresDB(config)
db.connect()
print(db.show_table_columns('notes'))
