###################################################################
# This is a class that provides utilities for working with the
# database
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
import pandas as pd
import geopandas as gpd
import pickle
from tqdm import tqdm


class DBUtils:
    """pyBNA database utilities class"""

    def __init__(self, db_connection_string, verbose=False, debug=False):
        """Connects to the BNA database

        kwargs:
        db_connection_string -- fully formed connection string for connecting to database
        verbose -- output useful messages
        debug -- set to debug mode

        return: DBUtils object
        """
        self.db_connection_string = db_connection_string
        self.verbose = verbose
        self.debug = debug


    def get_db_connection(self):
        """
        Returns a new db connection using the settings from the parent pyBNA class
        """
        return psycopg2.connect(self.db_connection_string)


    def get_pkid_col(self, table, schema=None):
        # connect to pg and read id col
        conn = self.get_db_connection()
        cur = conn.cursor()

        if schema:
            full_table = schema + "." + table
        else:
            full_table = table

        q = sql.SQL(" \
            SELECT a.attname \
            FROM   pg_index i \
            JOIN   pg_attribute a ON a.attrelid = i.indrelid \
                    AND a.attnum = ANY(i.indkey) \
            WHERE  i.indrelid = {}::regclass \
            AND    i.indisprimary;"
        ).format(
            sql.Literal(full_table)
        )
        cur.execute(q)

        if cur.rowcount == 0:
            raise Error("No primary key defined on table %s" % table)

        row = cur.fetchone()
        if self.verbose:
            print("   ID: %s" % row[0])
        cur.close()
        conn.close()
        return row[0]


    def get_schema(self,table):
        conn = self.get_db_connection()
        cur = conn.cursor()
        cur.execute(" \
            select nspname::text \
            from pg_namespace n, pg_class c \
            where n.oid = c.relnamespace \
            and c.oid = '%s'::regclass \
        " % table)
        return cur.next()[0]


    def get_srid(self,table,geom="geom"):
        schema = self.get_schema(table)
        conn = self.get_db_connection()
        cur = conn.cursor()

        q = sql.SQL("select find_srid({},{},{})").format(
            sql.Literal(schema),
            sql.Literal(table),
            sql.Literal(geom)
        )

        if self.debug:
            print(q.as_string(conn))

        cur.execute(q)
        srid = cur.next()[0]

        if self.verbose:
            print("SRID: %i" % srid)

        return srid


    def get_column_type(self,table,column,schema=None):
        """
        Returns the data type of the column

        args
        table -- the table name
        column -- the column name
        schema -- the schema (inferred if not given)

        returns
        string
        """
        conn = self.get_db_connection()
        cur = conn.cursor()

        if schema is not None:
            full_table = schema + "." + table
        else:
            full_table = table

        q = sql.SQL(" \
            SELECT pg_catalog.format_type(a.atttypid,a.atttypmod) \
            FROM   pg_catalog.pg_attribute a \
            WHERE  a.attnum>0 \
            AND NOT a.attisdropped \
            AND a.attrelid = {}::regclass \
            AND a.attname = {} \
        ").format(
            sql.Literal(full_table),
            sql.Literal(column)
        )
        cur.execute(q)

        if cur.rowcount == 0:
            raise Error("Column %s not found in table %s" % (column,table))

        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0]
