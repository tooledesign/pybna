import yaml
from urllib import urlretrieve
import tempfile
import os
from shutil import copy
import geopandas as gpd
from psycopg2 import sql
import overpass
import osmnx as ox
import random
import string

from conf import Conf
from dbutils import DBUtils


class Importer(DBUtils,Conf):
    """Standalone class to import pyBNA datasets"""

    def __init__(self, config=None, verbose=False, debug=False,
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
        Conf.__init__(self)
        self.verbose = verbose
        self.debug = debug
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        if config is None:
            config = os.path.join(self.module_dir,"config.yaml")
        self.config = self.parse_config(yaml.safe_load(open(config)))
        print("Connecting to database")
        if host is None:
            host = self.config.db.host
        if db_name is None:
            db_name = self.config.db.dbname
        if user is None:
            user = self.config.db.user
        if password is None:
            password = self.config.db.password
        db_connection_string = " ".join([
            "dbname=" + db_name,
            "user=" + user,
            "host=" + host,
            "password=" + password
        ])
        if self.debug:
            print("DB connection: %s" % db_connection_string)
        DBUtils.__init__(self,db_connection_string,self.verbose,self.debug)
        self.sql_subs = self.make_sql_substitutions(self.config)

        # mi/km
        if "units" in self.config:
            if self.config.units == "mi":
                self.km = False
            elif self.config.units == "km":
                self.km = True
            else:
                raise ValueError("Invalid units \"{}\" in config".format(self.config.units))
        else:
            self.km = False


    def __repr__(self):
        return "pyBNA Importer connected with {%s}" % self.db_connection_string


    def import_boundary(self,fpath,srid=None):
        """
        Takes a shapefile input and saves it to the DB (reprojecting if srid is given)

        args
        fpath -- path to the shapefile
        srid -- projection to use (if not given uses srid defined in config)
        """
        pass


    def import_census_blocks(self,fips=None,url=None,fpath=None,table=None,
                             keep_case=False,columns=None,id=None,geom=None,
                             srid=None,boundary_file=None,overwrite=False):
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
        table -- the table name to save blocks to (if none use config) (must be schema-qualified)
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
            if "table" in self.config.bna.blocks:
                schema, table = self.parse_table_name(self.config.bna.blocks.table)
            else:
                raise ValueError("No table given. Must be specified as an arg or in config file.")
        else:
            schema, table = self.parse_table_name(table)
        if schema is None:
            raise ValueError("No schema given. Must be qualified with table name.")
        if not overwrite and self.table_exists(table,schema):
            raise ValueError("Table %s.%s already exists" % (schema,table))
        if id is None:
            if "uid" in self.config.bna.blocks:
                id = self.config.bna.blocks.uid
            else:
                raise ValueError("No ID column name given. Must be specified as an arg or in config file.")
        if geom is None:
            if "geom" in self.config.bna.blocks:
                geom = self.config.bna.blocks.geom
            else:
                raise ValueError("No geom column name given. Must be specified as an arg or in config file.")
        if srid is None:
            if "srid" in self.config:
                srid = self.config.srid
            else:
                raise ValueError("SRID must be specified as an arg or in the config file")
        if boundary_file is not None:
            if not os.path.isfile(boundary_file):
                raise ValueError("File not found at %s" % boundary_file)

        # load the boundary into geopandas
        print("Loading boundary")
        boundary = self._load_boundary_as_dataframe(boundary_file,srid)

        # buffer the boundary by the maximum travel distance
        boundary.geometry = boundary.buffer(self.config.bna.connectivity.max_distance)

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


    def import_osm_network(self,roads_table=None,ints_table=None,
                           boundary_file=None,boundary_buffer=None,
                           osm_file=None,keep_holding_tables=False,srid=None,
                           overwrite=False):
        """
        Imports OSM ways/nodes and copies the data into the database with attributes
        needed for LTS scoring.

        args
        roads_table -- name of the table to save the OSM ways to (if none use config) (must be schema-qualified)
        ints_table -- name of the table to save the OSM intersections to (if none use config) (must be schema-qualified)
        boundary_file -- a boundary file path. if not given uses the boundary file specified in the config
        boundary_buffer -- distance (in units of the boundary) outside of the
            boundary to pull network features (if none use max_distance from config)
        osm_file -- an OSM XML file to use instead of pulling data from the network
        keep_holding_tables -- if true, saves the raw OSM import to the roads/ints schemas
        srid -- projection to use
        overwrite -- whether to overwrite any existing tables
        """
        if roads_table is None:
            if "table" in self.config.bna.network.roads:
                roads_schema, roads_table = self.parse_table_name(self.config.bna.network.roads.table)
            else:
                raise ValueError("No roads table given. Must be specified as an arg or in config file.")
        else:
            roads_schema, roads_table = self.parse_table_name(roads_table)
        if roads_schema is None:
            raise ValueError("No roads schema given. Must be qualified with table name.")
        if ints_table is None:
            if "table" in self.config.bna.network.intersections:
                ints_schema, ints_table = self.parse_table_name(self.config.bna.network.intersections.table)
            else:
                raise ValueError("No intersections table given. Must be specified as an arg or in config file.")
        else:
            ints_schema, ints_table = self.parse_table_name(ints_table)
        if ints_schema is None:
            raise ValueError("No intersections schema given. Must be qualified with table name.")
        if not overwrite and self.table_exists(roads_table,roads_schema):
            raise ValueError("Table %s.%s already exists" % (roads_schema,roads_table))
        if not overwrite and self.table_exists(ints_table,ints_schema):
            raise ValueError("Table %s.%s already exists" % (ints_schema,ints_table))
        if boundary_buffer is None:
            boundary_buffer = self.config.bna.connectivity.max_distance
        if srid is None:
            if "srid" in self.config:
                srid = self.config.srid
            else:
                raise ValueError("SRID must be specified as an arg or in the config file")
        crs = {"init": "epsg:%i" % srid}

        # generate table names for holding tables
        osm_ways_table = "osm_ways_"+"".join(random.choice(string.ascii_lowercase) for i in range(7))
        if keep_holding_tables:
            osm_ways_schema = roads_schema
        else:
            osm_ways_schema = "pg_temp"
        osm_nodes_table = "osm_nodes_"+"".join(random.choice(string.ascii_lowercase) for i in range(7))
        if keep_holding_tables:
            osm_nodes_schema = ints_schema
        else:
            osm_nodes_schema = "pg_temp"

        # load the boundary and process
        boundary = self._load_boundary_as_dataframe(boundary_file=boundary_file)
        boundary = boundary.buffer(boundary_buffer)
        boundary = boundary.to_crs({"init": "epsg:4326"})
        boundary = boundary.unary_union

        # load OSM
        if osm_file:
            print("Processing OSM data")
        else:
            print("Downloading OSM data")
        ways, nodes = self._osm_net_from_osmnx(boundary,osm_file)
        ways = ways.to_crs(crs)
        nodes = nodes.to_crs(crs)

        # copy to db
        print("Copying OSM ways to database")
        conn = self.get_db_connection()
        self.gdf_to_postgis(
            ways,
            osm_ways_table,
            osm_ways_schema,
            srid=srid,
            overwrite=overwrite,
            conn=conn
        )
        print("Copying OSM intersections to database")
        self.gdf_to_postgis(
            nodes,
            osm_nodes_table,
            osm_nodes_schema,
            srid=srid,
            overwrite=overwrite,
            conn=conn
        )

        conn.commit()

        self._process_osm(
            roads_table,roads_schema,ints_table,ints_schema,osm_ways_table,
            osm_ways_schema,osm_nodes_table,osm_nodes_schema,overwrite,conn
        )

        conn.commit()
        conn.close()


    def _process_osm(self,roads_table,roads_schema,ints_table,ints_schema,
                     osm_ways_table,osm_ways_schema,osm_nodes_table,
                     osm_nodes_schema,overwrite=None,conn=None):
        """
        Processes OSM import by running through the import scripts in the sql directory

        args:
        roads_table -- name of the roads table
        roads_schema -- name of the roads schema
        ints_table -- name of the intersections table
        ints_schema -- name of the intersections schema
        osm_ways_table -- name of the OSM ways table
        osm_ways_schema -- name of the OSM ways schema
        osm_nodes_table -- name of the OSM nodes table
        osm_nodes_schema -- name of the OSM nodes schema
        conn -- a connection object (if none a new connection is created)
        """
        commit = False
        if conn is None:
            conn = self.get_db_connection()
            commit = True

        subs = dict(self.sql_subs)
        subs["roads_table"] = sql.Identifier(roads_table)
        subs["roads_schema"] = sql.Identifier(roads_schema)
        subs["roads_geom_idx"] = sql.Identifier("sidx_"+roads_table)
        subs["ints_table"] = sql.Identifier(ints_table)
        subs["ints_schema"] = sql.Identifier(ints_schema)
        subs["ints_geom_idx"] = sql.Identifier("sidx_"+ints_table)
        subs["osm_ways_table"] = sql.Identifier(osm_ways_table)
        subs["osm_ways_schema"] = sql.Identifier(osm_ways_schema)
        subs["osm_nodes_table"] = sql.Identifier(osm_nodes_table)
        subs["osm_nodes_schema"] = sql.Identifier(osm_nodes_schema)
        if self.km:
            subs["km_multiplier"] = sql.Literal(1)
            subs["mi_multiplier"] = sql.Literal(1.609344)
        else:
            subs["km_multiplier"] = sql.Literal(0.6213712)
            subs["mi_multiplier"] = sql.Literal(1)

        # process things in the db
        road_queries = [os.path.join(self.module_dir,"sql","importer","roads",f) for f in os.listdir(os.path.join(self.module_dir,"sql","importer","roads"))]
        road_queries = sorted(road_queries)
        int_queries = [os.path.join(self.module_dir,"sql","importer","intersections",f) for f in os.listdir(os.path.join(self.module_dir,"sql","importer","intersections"))]
        int_queries = sorted(int_queries)
        queries = list(road_queries)
        queries.extend(int_queries)

        print("Processing OSM data in database")

        if overwrite:
            self.drop_table(roads_table,schema=roads_schema,conn=conn)
            self.drop_table(ints_table,schema=ints_schema,conn=conn)

        for fquery in queries:
            cur = conn.cursor()
            query = self.read_sql_from_file(fquery)
            q = sql.SQL(query).format(**subs)
            cur.execute(q)
            cur.close()

        if commit:
            conn.commit()
            conn.close()


    def _osm_net_from_osmnx(self,boundary,osm_file=None):
        """
        Submits an Overpass API query and returns a geodataframe of results

        args
        boundary -- shapely geometry representing the boundary for pulling the network
        osm_file -- an OSM XML file to use instead of downloading data from the network
        """
        # https://osmnx.readthedocs.io/en/stable/osmnx.html#osmnx.save_load.graph_to_gdfs
        node_tags = [
            "access",
            "amenity",
            "bicycle",
            "bridge",
            "button_operated",
            "crossing",
            "flashing_lights",
            "foot",
            "highway",
            "junction",
            "leisure",
            "motorcar",
            "name",
            "oneway",
            "oneway:bicycle",
            "operator",
            "public_transport",
            "railway",
            "segregated",
            "shop",
            "stop",
            "surface",
            "traffic_sign",
            "traffic_signals",
            "tunnel",
            "width"
        ]
        way_tags = [
            "access",
            "bridge",
            "bicycle",
            "button_operated",
            "crossing",
            "cycleway",
            "cycleway:left",
            "cycleway:right",
            "cycleway:both",
            "cycleway:buffer",
            "cycleway:left:buffer",
            "cycleway:right:buffer",
            "cycleway:both:buffer",
            "cycleway:width",
            "cycleway:left:width",
            "cycleway:right:width",
            "cycleway:both:width",
            "flashing_lights",
            "foot",
            "footway",
            "highway",
            "junction",
            "landuse",
            "lanes",
            "lanes:forward",
            "lanes:backward",
            "lanes:both_ways",
            "leisure",
            "maxspeed",
            "motorcar",
            "name",
            "oneway",
            "oneway:bicycle",
            "operator",
            "parking",
            "parking:lane",
            "parking:lane:right",
            "parking:lane:left",
            "parking:lane:both",
            "parking:lane:width",
            "parking:lane:right:width",
            "parking:lane:left:width",
            "parking:lane:both:width",
            "public_transport",
            "railway",
            "segregated",
            "service",
            "shop",
            "stop",
            "surface",
            "tracktype",
            "traffic_sign",
            "traffic_signals:direction",
            "tunnel",
            "turn:lanes",
            "turn:lanes:both_ways",
            "turn:lanes:backward",
            "turn:lanes:forward",
            "width",
            "width:lanes",
            "width:lanes:forward",
            "width:lanes:backward"
        ]

        ox.config(useful_tags_node=node_tags,useful_tags_path=way_tags)
        if osm_file:
            G = ox.graph_from_file(
                osm_file,
                simplify=True,
                retain_all=False
            )
        else:
            G = ox.graph_from_polygon(
                boundary,network_type='all',simplify=True,retain_all=False,
                truncate_by_edge=False,timeout=180,clean_periphery=True,
                custom_filter=None
            )
        G = ox.get_undirected(G)
        gdfs = ox.graph_to_gdfs(G)
        return gdfs[1], gdfs[0]


    def import_osm_destinations(self,schema,boundary_file=None,srid=None,overwrite=False):
        """
        Processes OSM destinations and copies the data into the database.

        args
        schema -- the schema to create the tables in
        boundary_file -- a boundary file path. if not given uses the boundary file specified in the config
        srid -- projection to use
        overwrite -- whether to overwrite any existing tables
        """
        if srid is None:
            if "srid" in self.config:
                srid = self.config.srid
            else:
                raise ValueError("SRID must be specified as an arg or in the config file")
        epsg = "epsg:%i" % srid

        boundary = self._load_boundary_as_dataframe(boundary_file=boundary_file)
        boundary = boundary.to_crs({"init": "epsg:4326"})
        min_lon,min_lat,max_lon,max_lat = boundary.total_bounds

        # set up a list of dictionaries with info about each destination
        destinations = [
            {"table":"schools","tags_query":""},
            {"table":"parks","tags_query":""}
        ]

        for d in destinations:
            table = d["table"]
            tags = d["tags_query"]
            gdf = self._osm_destination_from_overpass(min_lon,min_lat,max_lon,max_lat,tags)
            print("Copying %s to database" % table)
            self.gdf_to_postgis(
                gdf,table,schema,
                srid=srid,
                overwrite=overwrite
            )


    def _osm_destination_from_overpass(self,min_lon,min_lat,max_lon,max_lat,tags):
        """
        Submits an Overpass API query and returns a geodataframe of results

        args
        min_lon -- Minimum longitude
        min_lat -- Minimum latitude
        max_lon -- Maximum longitude
        max_lat -- Maximum latitude
        tags -- list of osm tags to use for filtering this destination type
        """
        # https://github.com/jwass/geopandas_osm   ????
        api = overpass.API()
        slc = api.get('node["name"="Salt Lake City"]')
        gdf = gpd.GeoDataFrame.from_features(slc)


    def _load_boundary_as_dataframe(self,boundary_file=None,srid=None):
        """
        Loads the boundary file as a geodataframe. If a file is given, uses
        the file. If not, reads the config and loads the boundary from the
        table indicated in the config.

        args
        boundary_file -- path to a file
        srid -- projection to use for the geodataframe (if none use the projection of the source data)

        returns
        geodataframe object
        """
        if not srid is None:
            epsg = "epsg:%i" % srid
        if boundary_file is None:
            if "geom" in self.config.bna.boundary:
                boundary_geom = self.config.bna.boundary.geom
            else:
                boundary_geom = "geom"
            boundary_schema, boundary_table = self.parse_table_name(self.config.bna.boundary.table)
            if boundary_schema is None:
                boundary_schema = self.get_schema(boundary_table)
            conn = self.get_db_connection()
            q = sql.SQL("select {} from {}.{}").format(
                sql.Identifier(boundary_geom),
                sql.Identifier(boundary_schema),
                sql.Identifier(boundary_table)
            ).as_string(conn)
            boundary = gpd.GeoDataFrame.from_postgis(
                sql=q,
                con=conn,
                geom_col=boundary_geom
            )
            conn.close()
        else:
            boundary = gpd.read_file(boundary_file)
        if not srid is None:
            boundary = boundary.to_crs({'init': epsg})
        return boundary
