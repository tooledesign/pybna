import yaml
from urllib import urlretrieve
import tempfile
import os
from shutil import copy
import geopandas as gpd
from psycopg2 import sql
import overpass

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


    def import_census_blocks(self,fips=None,url=None,fpath=None,table=None,
                             schema=None,keep_case=False,columns=None,id=None,
                             geom=None,srid=None,boundary_file=None,overwrite=False):
        """
        Retrieves census block features and saves them to the
        designated blocks table in the DB. Can take a FIPS code to download
        directly from the US Census, or can take a URL or file path to any
        file that can be automatically opened by geopandas' read_file method
        (zipped shapefile, shapefile, geojson, etc.)

        args
        fips -- the two digit fips code that identifies the state
        url -- url to download a file from
        fpath -- path to a file
        table -- the table name to save blocks to (if none use config)
        schema -- the schema to save blocks to (if none use config)
        keep_case -- whether to prevent column names from being converted to lower case
        columns -- list of columns in the dataset to keep (if none keeps all)
        id -- name for the id/primary key column (if none use config)
        geom -- name for the geometry column (if none use config)
        srid -- projection to use (if not given uses srid defined in config)
        boundary_file -- path to the boundary file (if not given reads it from the DB as defined in config)
        overwrite -- deletes an existing table
        """
        # check inputs
        if fips is None and url is None and fpath is None:
            raise ValueError("Either FIPS code, URL, or file path must be given")
        if fips is not None and url is not None:
            raise ValueError("Can't accept a FIPS code _and_ a URL")
        if fips is not None and fpath is not None:
            raise ValueError("Can't accept a FIPS code _and_ a file name")
        if fpath is not None and url is not None:
            raise ValueError("Can't accept a file name _and_ a URL")
        if fips is not None:
            if isinstance(fips, (int, long)):
                fips = '{0:02d}'.format(fips)
        if fpath is not None:
            if not os.path.isfile(fpath):
                raise ValueError("File not found at %s" % fpath)
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
        if boundary_file is not None:
            if not os.path.isfile(boundary_file):
                raise ValueError("File not found at %s" % boundary_file)

        # copy the shapefile to temporary directory and load into geopandas
        if not fpath is None:
            src = fpath
        if not url is None:
            src = url
        if not fips is None:
            src = "http://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/tabblock2010_" + fips + "_pophu.zip"
        print("Loading data from %s" % src)
        blocks = gpd.read_file(src)
        epsg = "epsg:%i" % srid
        blocks = blocks.to_crs({'init': epsg})
        blocks.columns = [c.lower() for c in blocks.columns]

        # load the boundary into geopandas
        print("Loading boundary")
        if boundary_file is None:
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
            boundary = gpd.read_file(boundary_file)
        boundary = boundary.to_crs({'init': epsg})

        # buffer the boundary by the maximum travel distance
        boundary.geometry = boundary.buffer(self.config["bna"]["connectivity"]["max_distance"])

        # filter to blocks within the boundary
        print("Filtering blocks to boundary")
        blocks = blocks[blocks.intersects(boundary.unary_union)]

        # copy data to db
        print("Copying blocks to database")
        self.gdf_to_postgis(
            blocks,table,schema,
            geom=geom,
            id=id,
            keep_case=keep_case,
            srid=srid,
            columns=columns,
            overwrite=overwrite
        )


    def import_osm(self,boundary_file=None):
        """
        Processes OSM data and copies it into the database with attributes
        needed for LTS and destination scoring.

        args

        """
        pass


    def _osm_ways_from_overpass(self,xmax,ymax,xmin,ymin):
        """
        Submits an Overpass API query and returns a dictionary of results

        args
        xmax -- Maximum bound of the data on the X axis
        ymax -- Maximum bound of the data on the Y axis
        xmin -- Minimum bound of the data on the X axis
        ymin -- Minimum bound of the data on the Y axis
        """
        # https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL
        # https://github.com/mvexel/overpass-api-python-wrapper
        # https://gis.stackexchange.com/questions/246303/can-you-restrict-which-osm-tags-are-returned-by-overpass-api
        pass


    def _osm_destination_from_overpass(self,xmax,ymax,xmin,ymin,tags):
        """
        Submits an Overpass API query and returns a dictionary of results

        args
        xmax -- Maximum bound of the data on the X axis
        ymax -- Maximum bound of the data on the Y axis
        xmin -- Minimum bound of the data on the X axis
        ymin -- Minimum bound of the data on the Y axis
        tags -- list of osm tags to use for filtering this destination type
        """
        pass
