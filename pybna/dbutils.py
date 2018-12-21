###################################################################
# This is a class that provides utilities for working with the
# database
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
import geopandas as gpd
import numpy as np
from binascii import hexlify
from string import upper
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
            raise ValueError("No primary key defined on table %s" % table)

        row = cur.fetchone()
        if self.verbose:
            print("   Table %s  ID: %s" % (table,row[0]))
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


    def get_srid(self,table,geom="geom",schema=None):
        if schema is None:
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
            raise ValueError("Column %s not found in table %s" % (column,table))

        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0]


    def table_exists(self,table,schema=None):
        """
        Checks whether the given table exists in the db

        args
        table -- the table name
        schema -- the schema name

        returns
        boolean -- true if exists, false if not
        """
        if schema is None:
            full_table = table
        else:
            full_table = schema + "." + table
        conn = self.get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL("select {}::regclass").format(sql.Literal(full_table)))
            cur.close()
            conn.close()
            return True
        except psycopg2.ProgrammingError:
            conn.close()
            return False


    def split_sql_for_tqdm(self,sql):
        """
        reads in an input sql script with comments representing progress updates.
        splits statements into a list.
        expects comments intended for progress reporting to be terminated by a
        semicolon. also expects sql statements to NOT begin with comments (i.e.
        don't lead off with a comment or the whole statement will be interpreted
        as a progress update)

        args
        sql -- the raw sql text

        returns
        tqdm object composed of a list of dictionaries where each entry has
        two values, the query and a progress update
        """
        statements = [s for s in sql.split(";") if len(s.strip()) > 1]

        parsed = []
        running_entry = {
            "update": None,
            "query": " "
        }
        for statement in statements:
            if statement.strip()[:2] == '--':
                running_entry["update"] = statement.strip()[2:]
            else:
                running_entry["query"] = statement
                parsed.append(dict(running_entry))
                # running_entry = {
                #     "update": " ",
                #     "query": None
                # }

        return tqdm(parsed)


    def read_sql_from_file(self,path):
        """
        Reads the SQL file at the path and returns it as plain text

        args:
        path -- file path

        returns:
        string
        """
        f = open(path)
        query = f.read()
        f.close()
        return query


    def drop_table(self,table,schema=None,conn=None):
        """
        Drops the given table from the database

        args:
        table -- table name
        schema -- schema name (default: inferred)
        conn -- a psycopg2 connection object (default: create new connection)
        """
        transaction = True
        if conn is None:
            transaction = False
            conn = self.get_db_connection()
        cur = conn.cursor()

        if isinstance(table, str):
            table = sql.Identifier(table)
        if schema is not None and isinstance(schema, str):
            schema = sql.Identifier(schema)

        if schema is None:
            cur.execute(sql.SQL("drop table if exists {}").format(table))
        else:
            cur.execute(sql.SQL("drop table if exists {}.{}").format(
                schema, table))

        if not transaction:
            conn.commit()


    def gdf_to_postgis(self,gdf,table,schema,columns=None,geom="geom",id="id",
                       keep_case=False,srid=None,conn=None,overwrite=False):
        """
        Saves a geopandas geodataframe to Postgis.

        args:
        gdf -- the GeoDataFrame to save
        table -- the table name
        schema -- the schema name
        columns -- a list of columns to save (if empty, save all columns)
        geom -- name to use for the geom column
        id -- name to use for the id/primary key column (created if it doesn't match anything in columns)
        keep_case -- prevents conversion of column names to lower case
        srid -- the projection to use (if none inferred from data)
        conn -- an open psycopg2 connection
        overwrite -- drops an existing table
        """
        # process inputs
        transaction = True
        if conn is None:
            transaction = False
            conn = self.get_db_connection()
        if not keep_case:
            gdf.columns = [c.lower() for c in gdf.columns]
        if columns is None:
            columns = gdf.columns
        elif not keep_case:
            columns = [c.lower() for c in columns]
        if srid is None:
            srid = int(gdf.geometry.crs["init"].split(":")[1])
        if overwrite:
            self.drop_table(table,schema,conn)

        # remove geom column and any columns that aren't in the gdf
        tmp_cols = list()
        for c in columns:
            if c in gdf.columns and c != geom:
                tmp_cols.append(c)
        columns = list(tmp_cols)
        del tmp_cols

        db_columns = list()
        types = list()
        db_columns.append(geom)
        types.append("text")
        for c in columns:
            if c == gdf.geometry.name:
                continue
            dtype = "text"
            if gdf[c].dtype in (np.int8,np.int16,np.int32,np.int64,np.uint8,np.uint16,np.uint32,np.uint64):
                dtype = "integer"
            if gdf[c].dtype in (np.float16,np.float32,np.float64):
                dtype = "float"
            if c.lower() == id.lower():
                dtype += " primary key"
            db_columns.append(c)
            types.append(dtype)
        columns_with_types = [sql.SQL(" ").join([sql.Identifier(k),sql.SQL(v)]) for k, v in zip(db_columns,types)]
        if not id in db_columns:
            columns_with_types.insert(0,sql.SQL(" ").join([sql.Identifier(id),sql.SQL("serial primary key")]))
        columns_sql = sql.SQL(",").join(columns_with_types)
        q = sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            columns_sql
        )
        cur = conn.cursor()
        cur.execute(q)

        #
        # copy data over
        #
        insert_sql = sql.SQL("INSERT INTO {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(",").join([sql.Identifier(c) for c in db_columns])
        )
        insert_sql = insert_sql.as_string(conn)
        insert_sql += " VALUES %s"

        # convert geoms to wkt
        gdf["wkbs"] = gdf.geometry.apply(lambda x: x.wkb).apply(hexlify).apply(upper)
        gdf = gdf.drop(gdf.geometry.name,axis=1)
        gdf = gdf.rename(columns={"wkbs": geom})

        execute_values(cur,insert_sql,gdf[db_columns].values)
        subs = {
            "schema": sql.Identifier(schema),
            "table": sql.Identifier(table),
            "geom": sql.Identifier(geom),
            "srid": sql.Literal(srid),
            "index": sql.Identifier("sidx_"+table)
        }
        q = sql.SQL(" \
            ALTER TABLE {schema}.{table} ALTER COLUMN {geom} TYPE geometry(multipolygon,{srid}) \
            USING ST_Multi(ST_SetSRID({geom}::geometry,{srid})); \
            CREATE INDEX {index} ON {schema}.{table} USING GIST ({geom}); \
            ANALYZE {schema}.{table};"
        ).format(**subs)
        cur.execute(q)
        cur.close()
        if not transaction:
            conn.commit()
            conn.close()
