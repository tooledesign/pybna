###################################################################
# This is a class that provides utilities for working with the
# database
###################################################################
import os
import yaml
import psycopg2
import sqlite3
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon
from binascii import hexlify
from tqdm import tqdm


class DBUtils:
    """pyBNA database utilities class"""

    def __init__(self, db_connection_string, verbose=False, debug=False):
        """Connects to the BNA database

        Parameters
        ----------
        db_connection_string : str
            fully formed connection string for connecting to database
        verbose : bool, optional
            output useful messages
        debug : bool, optional
            set to debug mode

        return: DBUtils object
        """
        self.db_connection_string = db_connection_string
        self.verbose = verbose
        self.debug = debug
        self.module_dir = os.path.dirname(os.path.abspath(__file__))


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
            print("   Table {}  ID: {}".format(table,row[0]))
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
        return cur.fetchone()[0]


    def get_default_schema(self):
        """
        Returns the name of the default schema in the database (i.e. the first
        schema in the search path)
        """
        conn = self.get_db_connection()
        cur = conn.cursor()
        cur.execute("show search_path")
        path = cur.fetchone()[0]
        schema = path.split(',')[0].strip()
        conn.close()
        return schema


    def parse_table_name(self,name):
        """
        Separates the given name into a schema and table and returns them as a
        list. If no schema is given, returns None for the schema.

        Parameters
        ----------
        name : str
            the name to parse
        """
        try:
            schema, table = name.split(".")
            return schema, table
        except:
            return None, name


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
        srid = cur.fetchone()[0]

        if self.verbose:
            print("SRID: {}".format(srid))

        return srid


    def get_column_type(self,table,column,schema=None):
        """
        Returns the data type of the column

        Parameters
        ----------
        table : str
            the table name
        column : str
            the column name
        schema : str, optional
            the schema (inferred if not given)

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

        Parameters
        ----------
        table : str
            the table name
        schema : str, optional
            the schema name

        Returns
        -------
        boolean
            True if exists, false if not.
        """
        conn = self.get_db_connection()
        cur = conn.cursor()

        if schema is None:
            schema, table = self.parse_table_name(table)

        if schema is None:
            full_table = sql.Identifier(table).as_string(conn)
        else:
            full_table = sql.Identifier(schema).as_string(conn) + "." + sql.Identifier(table).as_string(conn)

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

        Parameters
        ----------
        sql : str
            the raw sql text

        Returns
        -------
        tqdm object
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

        Parameters
        ----------
        path : str
            file path

        Returns
        -------
        str
        """
        f = open(path)
        query = f.read()
        f.close()
        return query


    def drop_table(self,table,schema=None,conn=None):
        """
        Drops the given table from the database

        Parameters
        ----------
        table : str
            table name (optionally schema-qualified)
        schema : str, optional
            schema name (incompatible with schema-qualified table name)
        conn : psycopg2 connection object, optional
            a psycopg2 connection object (default: create new connection)
        """
        transaction = True
        if conn is None:
            transaction = False
            conn = self.get_db_connection()
        cur = conn.cursor()

        if schema is None:
            schema, table = self.parse_table_name(table)

        if schema is None:
            raise ValueError("Schema must either be given explicitly or qualified in table name")

        cur.execute(
            sql.SQL("drop table if exists {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
        )

        if not transaction:
            conn.commit()


    def gdf_to_postgis(self,gdf,table,schema=None,columns=None,geom="geom",id="id",
                       multi=True,keep_case=False,srid=None,conn=None,
                       overwrite=False,no_geom=False):
        """
        Saves a geopandas geodataframe to Postgis.

        Parameters
        ----------
        gdf : geopandas GeoDataFrame object
            the GeoDataFrame to save
        table : str
            the table name
        schema : str, optional
            the schema name (not necessary if schema is qualified in table name)
        columns : list of str, optional
            a list of columns to save (if empty, save all columns)
        geom : str, optional
            name to use for the geom column
        id : str, optional
            name to use for the id/primary key column (created if it doesn't match anything in columns)
        multi : bool, optional
            convert single to multi if mixed types are found
        keep_case : bool, optional
            prevents conversion of column names to lower case
        srid : int, optional
            the projection to use (if none inferred from data)
        conn : psycopg2 connection object, optional
            an open psycopg2 connection
        overwrite : bool, optional
            drops an existing table
        no_geom : bool, optional
            copies only the table without accompany geometries (or processes a non-geo table)
        """
        # process inputs
        if schema is None:
            schema, table = self.parse_table_name(table)
        if schema is None:
            raise ValueError("Schema must either be given explicitly or qualified in table name")
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
        if srid is None and not no_geom:
            srid = int(gdf.geometry.crs["init"].split(":")[1])
        if overwrite:
            self.drop_table(table,schema,conn)

        if no_geom:
            # remove a geom column  (if there is one)
            try:
                gdf.drop(gdf.geometry.name,axis=1,inplace=True)
            except:
                pass
        else:
            # get geom column type
            shapely_type = gdf.geom_type.unique()
            if len(shapely_type) > 1:
                if len(shapely_type) > 2:
                    raise ValueError("Can't process more than one geometry type")
                elif multi:
                    g1 = shapely_type[0]
                    g2 = shapely_type[1]
                    if g1 in ["Point","MultiPoint"] and g2 in ["Point","MultiPoint"]:
                        pass
                    elif g1 in ["LineString","MultiLineString"] and g2 in ["LineString","MultiLineString"]:
                        pass
                    elif g1 in ["Polygon","MultiPolygon"] and g2 in ["Polygon","MultiPolygon"]:
                        pass
                    else:
                        raise ValueError("Can't process more than one geometry type")
                else:
                    raise ValueError("Can't process more than one geometry type")
            else:
                multi = False

            shapely_type = shapely_type[0]
            if shapely_type == "Point":
                if multi:
                    geom_type = "multipoint"
                else:
                    geom_type = "point"
            elif shapely_type == "MultiPoint":
                geom_type = "multipoint"
            elif shapely_type == "LineString":
                if multi:
                    geom_type = "multilinestring"
                else:
                    geom_type = "linestring"
            elif shapely_type == "MultiLineString":
                geom_type = "multilinestring"
            elif shapely_type == "Polygon":
                if multi:
                    geom_type = "multipolygon"
                else:
                    geom_type = "polygon"
            elif shapely_type == "MultiPolygon":
                geom_type = "multipolygon"
            else:
                raise ValueError("Incompatible geometry type".format(shapely_type))

        # remove geom column and any columns that aren't in the gdf
        tmp_cols = list()
        for c in columns:
            if c in gdf.columns and c != geom:
                tmp_cols.append(c)
        columns = list(tmp_cols)
        del tmp_cols

        db_columns = list()
        types = list()
        if not no_geom:
            db_columns.append(geom)
            types.append("text")
        for c in columns:
            if not no_geom:
                if c == gdf.geometry.name:
                    continue
            dtype = "text"
            if gdf[c].dtype in (np.int64,np.uint64):
                dtype = "bigint"
            if gdf[c].dtype in (np.int8,np.int16,np.int32,np.uint8,np.uint16,np.uint32):
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
        if not no_geom:
            gdf["wkbs"] = gdf.geometry.apply(lambda x: x.wkb).apply(hexlify).str.decode("utf-8").str.upper()
            gdf = gdf.drop(gdf.geometry.name,axis=1)
            gdf = gdf.rename(columns={"wkbs": geom})

        execute_values(cur,insert_sql,gdf[db_columns].values)
        if not no_geom:
            subs = {
                "schema": sql.Identifier(schema),
                "table": sql.Identifier(table),
                "geom": sql.Identifier(geom),
                "geom_type": sql.SQL(geom_type),
                "srid": sql.Literal(srid),
                "index": sql.Identifier("sidx_"+table)
            }
            if multi:
                q = sql.SQL(" \
                    ALTER TABLE {schema}.{table} ALTER COLUMN {geom} TYPE geometry({geom_type},{srid}) \
                    USING ST_Multi(ST_SetSRID({geom}::geometry,{srid})); \
                    CREATE INDEX {index} ON {schema}.{table} USING GIST ({geom}); \
                    ANALYZE {schema}.{table};"
                ).format(**subs)
            else:
                q = sql.SQL(" \
                    ALTER TABLE {schema}.{table} ALTER COLUMN {geom} TYPE geometry({geom_type},{srid}) \
                    USING ST_SetSRID({geom}::geometry,{srid}); \
                    CREATE INDEX {index} ON {schema}.{table} USING GIST ({geom}); \
                    ANALYZE {schema}.{table};"
                ).format(**subs)
            cur.execute(q)
            cur.close()
        if not transaction:
            conn.commit()
            conn.close()


    def _run_sql_script(self, fname, subs, dirs, ret=False, conn=None):
        """Pass substitutions into a sql script, and execute against server.

        fname : str
            name of the sql file
        subs : dict
            dict of substitutions for the SQL
        dirs : list
            list of directory tree in the submodule
        ret : bool, optional
            if true, send cursor back to calling routine for further processing
            (requires a pre-existing connection to be passed)
        conn : psycopg2 connection object, optional
            A connection object to work with. if none, create a new one and close it at the end
        """
        if conn is None:
            if ret:
                raise ValueError("Return option requires a pre-existing connection")
            close_conn = True
            conn = self.get_db_connection()
        else:
            close_conn = False

        # process fname
        dirs.insert(0,self.module_dir)
        dirs.append(fname)
        fpath = os.path.join(*dirs)

        # Read SQL script
        raw = self.read_sql_from_file(fpath)

        q = sql.SQL(raw).format(**subs)
        cur = conn.cursor()
        try:
            cur.execute(q)
        except Exception as e:
            if conn.closed == 0:
                conn.rollback()
                conn.close()
            raise e
        if ret:
            result = cur.fetchall()
            cur.close()
            return result
        else:
            cur.close()

        if close_conn:
            conn.commit()
            conn.close()


    def _run_sql(self, statement, subs=None, ret=False, dry=None, conn=None):
        """Pass substitutions into a sql script, and execute against server.

        statement : str
            sql to run
        subs : dict, optional
            dict of substitutions for the SQL
        ret : bool, optional
            if true, send cursor back to calling routine for further processing
            (requires a pre-existing connection to be passed)
        dry : str, optional
            a path to save SQL statements to instead of executing in DB
        conn : psycopg2 connection object, optional
            A connection object to work with. if none, create a new one and close it at the end
        """
        if conn is None:
            if ret:
                raise ValueError("Return option requires a pre-existing connection")
            close_conn = True
            conn = self.get_db_connection()
        else:
            close_conn = False

        if subs is None:
            subs = dict()
        q = sql.SQL(statement).format(**subs)
        if dry is None:
            cur = conn.cursor()
            try:
                cur.execute(q)
            except Exception as e:
                if conn.closed == 0:
                    conn.rollback()
                    conn.close()
                raise e
            if ret:
                result = cur.fetchall()
                cur.close()
                return result
            else:
                cur.close()
        else:
            append = 'w'
            if os.path.isfile(dry):
                append = 'a'
            with open(dry,append) as f:
                f.write(q.as_string(conn))
                f.write("\n")

        if close_conn:
            conn.commit()
            conn.close()


    def _add_column(self,table,name,datatype,schema=None,conn=None):
        """
        Adds a column to the given table
        """
        if schema is None:
            schema, table = self.parse_table_name(table)
        if schema is None:
            schema = self.get_schema(table)

        if not self.table_exists(table,schema):
            raise ValueError("Table {}.{} does not exist".format(schema,table))

        subs = {
            "schema": sql.Identifier(schema),
            "table": sql.Identifier(table),
            "name": sql.Identifier(name),
        }
        # handle possible SQL object as type
        if type(datatype) is sql.SQL:
            subs["type"] = datatype
        else:
            subs["type"] = sql.SQL(datatype)

        self._run_sql(
            "alter table {schema}.{table} add column if not exists {name} {type}",
            subs=subs,
            conn=conn
        )


    def export_table(self,table,fpath,layer=None,geom="geom",pkey=None,nonspatial=False):
        """
        Exports the given table to a geopackage at the given path. Overwrites
        any pre-existing tables so use with caution!

        Parameters
        ----------
        table : text
            the table in the database to export
        fpath : text
            the path to the geopackage file
        geom : text
            name of the geometry column
        pkey : text, optional
            the primary key column
        nonspatial : bool, optional
            if true, processes the table without spatial information
        """
        base, ext = os.path.splitext(fpath)
        if not ext == ".gpkg":
            raise ValueError("Output file should be a geopackage (.gpkg)")

        if pkey is None:
            pkey = self.get_pkid_col(table)

        schema, table = self.parse_table_name(table)
        if schema is None:
            schema = self.get_schema(table)

        if layer is None:
            layer = table

        # set up check for list columns
        def is_iterable(ds):
            if isinstance(
                            ds.iloc[0],
                            (list,tuple,dict)
                         ):
                return True
            else:
                return False

        # load and export
        conn = self.get_db_connection()
        if nonspatial:
            t = pd.read_sql(
                sql.SQL("select * from {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table)
                ).as_string(conn),
                conn,
                index_col=pkey
            )
            for col in t.columns:
                if is_iterable(t[col]):
                    t[col] = t[col].astype("str")
            sqlite_conn = sqlite3.connect(fpath)
            t.to_sql(layer,sqlite_conn)
        else:
            t = gpd.read_postgis(
                sql.SQL("select * from {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table)
                ).as_string(conn),
                conn,
                geom_col=geom,
                index_col=pkey
            )
            for col in t.columns:
                if is_iterable(t[col]):
                    t[col] = t[col].astype("str")
            t.to_file(fpath,layer=layer,driver="GPKG")
