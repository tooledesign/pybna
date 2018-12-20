import yaml
from urllib import urlretrieve
import tempfile
import os
from zipfile import ZipFile
import fnmatch
import geopandas as gpd
import numpy as np
from binascii import hexlify
from string import upper
from psycopg2 import sql
from psycopg2.extras import execute_values

from dbutils import DBUtils


class Importer(DBUtils):
    """Standalone class to import pyBNA datasets"""

    def __init__(self, config="config.yaml", verbose=False, debug=False,
                 host=None, db_name=None, user=None, password=None):
        """
        Reads the config file and sets up a connection to the database

        args
        config -- path to the config file
        verbose -- output useful messages
        debug -- set to debug mode
        host -- hostname or address (overrides the config file if given)
        db -- name of database on server (overrides the config file if given)
        user -- username to connect to database (overrides the config file if given)
        password -- password to connect to database (overrides the config file if given)
        """
        self.verbose = verbose
        self.debug = debug
        self.config = yaml.safe_load(open(config))
        print("Connecting to database")
        if host is None:
            host = self.config["db"]["host"]
        if db_name is None:
            db_name = self.config["db"]["dbname"]
        if user is None:
            user = self.config["db"]["user"]
        if password is None:
            password = self.config["db"]["password"]
        db_connection_string = " ".join([
            "dbname=" + db_name,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        if self.debug:
            print("DB connection: %s" % db_connection_string)
        DBUtils.__init__(self,db_connection_string,self.verbose,self.debug)


    def import_boundary(self,fpath,srid=None):
        """
        Takes a shapefile input and saves it to the DB (reprojecting if srid is given)

        args
        fpath -- path to the shapefile
        srid -- projection to use (if not given uses srid defined in config)
        """
        pass


    def import_census_blocks(self,fips=None,url=None,table=None,schema=None,
                             id=None,geom=None,pop="pop10",srid=None,
                             boundary_shp=None,overwrite=False):
        """
        Retrieves census block features and saves them to the
        designated blocks table in the DB. Can take a FIPS code to download
        directly from the US Census, or can take a URL to a zipped shapefile.

        args
        fips -- the two digit fips code that identifies the state
        url -- url to download a zipped shapefile from
        table -- the table name to save blocks to (if none use config)
        schema -- the schema to save blocks to (if none use config)
        id -- name for the id/primary key column (if none use config)
        geom -- name for the geometry column (if none use config)
        pop -- name for the population column
        srid -- projection to use (if not given uses srid defined in config)
        boundary_shp -- path to the boundary shapefile (if not given reads it from the DB as defined in config)
        overwrite -- deletes an existing table
        """
        # check inputs
        if fips is None and url is None:
            raise ValueError("Either FIPS code or URL must be given")
        if fips is not None and url is not None:
            raise ValueError("Can't accept a FIPS code _and_ a URL")
        if fips is not None:
            if isinstance(fips, (int, long)):
                fips = '{0:02d}'.format(fips)
        if table is None:
            if "table" in self.config["bna"]["blocks"]:
                table = self.config["bna"]["blocks"]["table"]
            else:
                raise ValueError("No table given. Must be specified as an arg or in config file.")
        if schema is None:
            if "schema" in self.config["bna"]["blocks"]:
                schema = self.config["bna"]["blocks"]["schema"]
            else:
                raise ValueError("No schema given. Must be specified as an arg or in config file.")
        if not overwrite and self.table_exists(table,schema):
            raise ValueError("Table %s.%s already exists" % (schema,table))
        if id is None:
            if "uid" in self.config["bna"]["blocks"]:
                id = self.config["bna"]["blocks"]["uid"]
            else:
                raise ValueError("No ID column name given. Must be specified as an arg or in config file.")
        if geom is None:
            if "geom" in self.config["bna"]["blocks"]:
                geom = self.config["bna"]["blocks"]["geom"]
            else:
                raise ValueError("No geom column name given. Must be specified as an arg or in config file.")
        if srid is None:
            if "srid" in self.config:
                srid = self.config["srid"]
            else:
                raise ValueError("SRID must be specified as an arg or in the config file")

        # download shapefile to temporary directory and load into geopandas
        if url is None:
            url = "http://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/tabblock2010_" + fips + "_pophu.zip"
        temp_dir = tempfile.mkdtemp()
        fpath = os.path.join(temp_dir,"blocks.zip")
        print("Downloading to %s from %s" % (fpath,url))
        urlretrieve(url,fpath)
        with ZipFile(fpath, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        shp = ""
        for root, dirs, files in os.walk(temp_dir):
            for _file in files:
                if fnmatch.fnmatch(_file, '*.shp'):
                    shp = os.path.join(root, _file)
        blocks = gpd.read_file(shp)
        epsg = "epsg:%i" % srid
        blocks = blocks.to_crs({'init': epsg})
        blocks.columns = [c.lower() for c in blocks.columns]

        # load the boundary into geopandas
        print("Loading boundary")
        if boundary_shp is None:
            if "geom" in self.config["bna"]["boundary"]:
                boundary_geom = self.config["bna"]["boundary"]["geom"]
            else:
                boundary_geom = "geom"
            if "schema" in self.config["bna"]["boundary"]:
                boundary_schema = self.config["bna"]["boundary"]["schema"]
            else:
                boundary_schema = self.get_schema(self.config["bna"]["boundary"]["table"])
            conn = self.get_db_connection()
            q = sql.SQL("select * from {}.{}").format(
                sql.Identifier(boundary_schema),
                sql.Identifier(self.config["bna"]["boundary"]["table"])
            ).as_string(conn)
            boundary = gpd.GeoDataFrame.from_postgis(
                sql=q,
                con=conn,
                geom_col=boundary_geom
            )
            conn.close()
        else:
            boundary = gpd.read_file(boundary_shp)
        boundary = boundary.to_crs({'init': epsg})

        # buffer the boundary by the maximum travel distance
        boundary.geometry = boundary.buffer(self.config["bna"]["connectivity"]["max_distance"])

        # filter to blocks within the boundary
        print("Filtering blocks to boundary")
        blocks = blocks[blocks.intersects(boundary.unary_union)]

        #
        # write the dataframe to the db
        #
        conn = self.get_db_connection()
        cur = conn.cursor()

        # drop old table
        if overwrite:
            self.drop_table(table,schema,conn)

        # build table creation statement
        if self.verbose:
            print("Creating table")
        columns = list()
        types = list()
        columns.append(geom)
        types.append("text")
        for c in blocks.columns:
            if c == blocks.geometry.name:
                continue
            dtype = "text"
            if blocks[c].dtype in (np.int8,np.int16,np.int32,np.int64,np.uint8,np.uint16,np.uint32,np.uint64):
                dtype = "integer"
            if blocks[c].dtype in (np.float16,np.float32,np.float64):
                dtype = "float"
            if c.lower() == id.lower():
                dtype += " primary key"
            columns.append(c)
            types.append(dtype)
        columns_with_types = [sql.SQL(" ").join([sql.Identifier(k),sql.SQL(v)]) for k, v in zip(columns,types)]
        if not id in columns:
            columns_with_types.insert(0,sql.SQL(" ").join([sql.Identifier(id),sql.SQL("serial primary key")]))
        columns_sql = sql.SQL(",").join(columns_with_types)
        q = sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            columns_sql
        )
        cur.execute(q)

        #
        # copy data over
        #
        print("Copying blocks to database")
        insert_sql = sql.SQL("INSERT INTO {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(",").join([sql.Identifier(c) for c in columns])
        )
        insert = insert_sql.as_string(conn)
        insert += " VALUES %s"

        # convert geoms to wkt
        blocks["wkbs"] = blocks.geometry.apply(lambda x: x.wkb).apply(hexlify).apply(upper)
        blocks = blocks.drop(blocks.geometry.name,axis=1)
        blocks = blocks.rename(columns={"wkbs": geom})

        execute_values(cur,insert,blocks[columns].values)
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
        conn.commit()
        conn.close()
