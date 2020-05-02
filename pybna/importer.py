import yaml
from urllib.request import urlretrieve
import tempfile
import os
from shutil import copy
import geopandas as gpd
import pandas as pd
from psycopg2 import sql
import overpass
import osmnx as ox
import random
import string
from shapely.geometry import shape, box
from geojson import FeatureCollection

try:
    with_osmium = True
    from .destinationosmhandler import DestinationOSMHandler
except:
    with_osmium = False

from .conf import Conf
from .dbutils import DBUtils


class Importer(Conf):
    """Standalone class to import pyBNA datasets"""

    def __init__(self, config=None, verbose=False, debug=False,
                 host=None, db_name=None, user=None, password=None):
        """
        Reads the config file and sets up a connection to the database

        Parameters
        ----------
        config : str, optional
            path to the config file
        verbose : bool, optional
            output useful messages
        debug : bool, optional
            set to debug mode
        host : str, optional
            hostname or address (overrides the config file if given)
        db : str, optional
            name of database on server (overrides the config file if given)
        user : str, optional
            username to connect to database (overrides the config file if given)
        password : str, optional
            password to connect to database (overrides the config file if given)
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
            print("DB connection: {}".format(db_connection_string))
        DBUtils.__init__(self,db_connection_string,self.verbose,self.debug)
        self.sql_subs = self.make_bna_substitutions(self.config)

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


    def import_boundary(self,fpath,srid=None,table=None,overwrite=False):
        """
        Takes a shapefile input and saves it to the DB as the boundary file
        (reprojecting to the appropriate srid)

        Parameters
        ----------
        fpath : str
            path to the shapefile
        srid : int or string, optional
            projection to use (if not given uses srid defined in config)
        table : str, optional
            table to write to (if empty use config)
        overwrite : bool, optional
            overwrite an existing table
        """
        # process inputs
        if not os.path.isfile(fpath):
            raise ValueError("File not found at %s" % fpath)
        if srid is None:
            if "srid" in self.config:
                srid = self.config.srid
            else:
                raise ValueError("SRID must be specified as an arg or in the config file")
        if table is None:
            schema, table = self.parse_table_name(self.config.bna.boundary.table)
        else:
            schema, table = self.parse_table_name(table)
        if schema is None:
            schema = self.get_default_schema()

        boundary = self._load_boundary_as_dataframe(fpath,srid)
        pk = "id"
        i = 0
        while pk in boundary.columns:
            pk = "id_{}".format(i)
            i += 1
        print("Copying boundary to database")
        self.gdf_to_postgis(boundary,table,schema,id=pk,srid=srid,overwrite=overwrite)


    def import_census_blocks(self,fips=None,url=None,fpath=None,table=None,
                             keep_case=False,keep_water=False,
                             columns=None,id=None,geom=None,
                             srid=None,boundary_file=None,overwrite=False):
        """
        Retrieves census block features and saves them to the
        designated blocks table in the DB. Can take a FIPS code to download
        directly from the US Census, or can take a URL or file path to any
        file that can be automatically opened by geopandas' read_file method
        (zipped shapefile, shapefile, geojson, etc.)

        Parameters
        ----------
        fips
            the two digit fips code that identifies the state
        url : str
            url to download a file from
        fpath : str
            path to a file
        table : str, optional
            the table name to save blocks to (if none use config) (must be schema-qualified)
        keep_case : bool, optional
            whether to prevent column names from being converted to lower case
        keep_water : bool, optional
            whether to omit census blocks that are associated with water only areas
        columns : list, optional
            list of columns in the dataset to keep (if none keeps all)
        id : str, optional
            name for the id/primary key column (if none use config)
        geom : str, optional
            name for the geometry column (if none use config)
        srid : int or str, optional
            projection to use (if not given uses srid defined in config)
        boundary_file : str, optional
            path to the boundary file (if not given reads it from the DB as defined in config)
        overwrite : bool, optional
            deletes an existing table
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
            if isinstance(fips, int):
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
            schema = self.get_default_schema()
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
        print("Loading data from {}".format(src))
        blocks = gpd.read_file(src)
        blocks = blocks.to_crs("epsg:{:d}".format(srid))
        blocks.columns = [c.lower() for c in blocks.columns]

        # filter to blocks within the boundary
        print("Filtering blocks to boundary")
        blocks = blocks[blocks.intersects(boundary.unary_union)]

        # filter out blocks associated with water
        if keep_water is False:
            print("Filtering out water")
            blocks = blocks[blocks.blockce.str[0] != '0']

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


    def import_census_jobs(self,table,state=None,url_main=None,url_aux=None,
                           fpath_main=None,fpath_aux=None,overwrite=False):
        """
        Retrieves LEHD journey-to-work tables saves them to the
        designated jobs table in the DB. Can take a two letter state abbreviation
        to download directly from the US Census, or can take a URL or file path

        Current example URLs are
        https://lehd.ces.census.gov/data/lodes/LODES7/wy/od/wy_od_aux_JT00_2013.csv.gz
        https://lehd.ces.census.gov/data/lodes/LODES7/wy/od/wy_od_main_JT00_2014.csv.gz

        Parameters
        ----------
        table : str
            the table name to save blocks to
        state : str
            the two letter state abbreviation
        url_main : str
            url to download the "main" file from
        url_aux : str
            url to download the "aux" file from
        fpath_main : str
            path to the "main" file
        fpath_aux : str
            path to the "aux" file
        overwrite : bool, optional
            deletes an existing table
        """
        # check inputs
        if int(url_main is None) + int(url_aux is None) == 1:
            raise ValueError("A URL must be provided for both the main and aux tables")
        if int(fpath_main is None) + int(fpath_aux is None) == 1:
            raise ValueError("Files must be provided for both the main and aux tables")
        if state is None and url_main is None and fpath_main is None:
            raise ValueError("Either state abbreviation, URL, or file path must be given")
        if state is not None and url_main is not None:
            raise ValueError("Can't accept a state abbreviation _and_ a URL")
        if state is not None and fpath_main is not None:
            raise ValueError("Can't accept a state abbreviation _and_ a file name")
        if fpath_main is not None and url_main is not None:
            raise ValueError("Can't accept a file name _and_ a URL")
        if state is not None:
            state = state.lower()
        if fpath_main is not None:
            if not os.path.isfile(fpath_main):
                raise ValueError("File not found at %s" % fpath_main)
            if not os.path.isfile(fpath_aux):
                raise ValueError("File not found at %s" % fpath_aux)
        schema, table = self.parse_table_name(table)
        if schema is None:
            schema = self.get_default_schema()
        if not overwrite and self.table_exists(table,schema):
            raise ValueError("Table %s.%s already exists" % (schema,table))

        # copy the shapefile to temporary directory and load into geopandas
        if not state is None:
            print("Loading data for state {}".format(state.upper()))
            year = 2020
            success = False
            while not success:
                if year < 2010:
                    raise ValueError("Could not find jobs data for state {}".format(state))
                try:
                    src_main = "https://lehd.ces.census.gov/data/lodes/LODES7/"+state+"/od/"+state+"_od_main_JT00_"+str(year)+".csv.gz"
                    src_aux = "https://lehd.ces.census.gov/data/lodes/LODES7/"+state+"/od/"+state+"_od_aux_JT00_"+str(year)+".csv.gz"
                    jobs_main = pd.read_csv(src_main)
                    jobs_aux = pd.read_csv(src_aux)
                    success = True
                except:
                    print("No data for state {} for year {}. Checking previous year.".format(state.upper(),year))
                    year -= 1
        else:
            if not fpath_main is None:
                src_main = fpath_main
                src_aux = fpath_aux
            if not url_main is None:
                src_main = url_main
                src_aux = url_aux
            print("Loading main data from {}".format(src_main))
            jobs_main = pd.read_csv(src_main)
            print("Loading aux data from {}".format(src_aux))
            jobs_aux = pd.read_csv(src_aux)

        # copy data to db
        jobs_main = jobs_main[["w_geocode","S000"]]
        jobs_main.columns = ["blockid10","jobs"]
        jobs_aux = jobs_aux[["w_geocode","S000"]]
        jobs_aux.columns = ["blockid10","jobs"]
        jobs = jobs_main.append(jobs_aux,ignore_index=True)
        jobs = jobs.groupby(["blockid10"],as_index=False).sum()
        jobs["blockid10"] = jobs["blockid10"].astype("str").str.rjust(15,"0")

        print("Copying jobs to database")
        self.gdf_to_postgis(jobs,table,schema,overwrite=overwrite,no_geom=True)


    def import_osm_network(self,roads_table=None,ints_table=None,
                           boundary_file=None,boundary_buffer=None,
                           osm_file=None,keep_holding_tables=False,srid=None,
                           km=None,overwrite=False):
        """
        Imports OSM ways/nodes and copies the data into the database with attributes
        needed for LTS scoring.

        Parameters
        ----------
        roads_table : str, optional
            name of the table to save the OSM ways to (if none use config) (must be schema-qualified)
        ints_table : str, optional
            name of the table to save the OSM intersections to (if none use config) (must be schema-qualified)
        boundary_file : str, optional
            a boundary file path. if not given uses the boundary file specified in the config
        boundary_buffer : str, optional
            distance (in units of the boundary) outside of the
            boundary to pull network features (if none use max_distance from config)
        osm_file : str, optional
            an OSM XML file to use instead of pulling data from the network
        keep_holding_tables : bool, optional
            if true, saves the raw OSM import to the roads/ints schemas
        srid : int or str, optional
            projection to use
        km : bool, optional
            if true, units for measurements and speed limits are imported to
            metric
        overwrite : bool, optional
            whether to overwrite any existing tables
        """
        if roads_table is None:
            if "table" in self.config.bna.network.roads:
                roads_schema, roads_table = self.parse_table_name(self.config.bna.network.roads.table)
            else:
                raise ValueError("No roads table given. Must be specified as an arg or in config file.")
        else:
            roads_schema, roads_table = self.parse_table_name(roads_table)
        if roads_schema is None:
            roads_schema = self.get_default_schema()
        if ints_table is None:
            if "table" in self.config.bna.network.intersections:
                ints_schema, ints_table = self.parse_table_name(self.config.bna.network.intersections.table)
            else:
                raise ValueError("No intersections table given. Must be specified as an arg or in config file.")
        else:
            ints_schema, ints_table = self.parse_table_name(ints_table)
        if ints_schema is None:
            ints_schema = self.get_default_schema()
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
        crs = "epsg:{:d}".format(srid)
        if km is None:
            km = self.km

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
        boundary = boundary.to_crs("epsg:4326")
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
            osm_ways_schema,osm_nodes_table,osm_nodes_schema,srid,km,overwrite,conn
        )

        conn.commit()
        conn.close()


    def _process_osm(self,roads_table,roads_schema,ints_table,ints_schema,
                     osm_ways_table,osm_ways_schema,osm_nodes_table,
                     osm_nodes_schema,srid,km=None,overwrite=None,conn=None):
        """
        Processes OSM import by running through the import scripts in the sql directory

        Parameters
        ----------
        roads_table : str
            name of the roads table
        roads_schema : str
            name of the roads schema
        ints_table : str
            name of the intersections table
        ints_schema : str
            name of the intersections schema
        osm_ways_table : str
            name of the OSM ways table
        osm_ways_schema : str
            name of the OSM ways schema
        osm_nodes_table : str
            name of the OSM nodes table
        osm_nodes_schema : str
            name of the OSM nodes schema
        srid : int or str, optional
            projection to use
        km : str, optional
            if true, units for measurements and speed limits are imported to
            metric equivalents
        overwrite : bool, optional
            overwrite an existing table
        conn : psycopg2 connection object, optional
            a connection object (if none a new connection is created)
        """
        commit = False
        if conn is None:
            conn = self.get_db_connection()
            commit = True
        if km is None:
            km = self.km

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
        subs["srid"] = sql.Literal(srid)
        if km:
            subs["km"] = sql.Literal(True)
            subs["km_multiplier"] = sql.Literal(1)
            subs["m_multiplier"] = sql.Literal(1)
            subs["mi_multiplier"] = sql.Literal(1.609344)
            subs["ft_multiplier"] = sql.Literal(0.3048)
        else:
            subs["km"] = sql.Literal(False)
            subs["km_multiplier"] = sql.Literal(0.6213712)
            subs["m_multiplier"] = sql.Literal(3.28084)
            subs["mi_multiplier"] = sql.Literal(1)
            subs["ft_multiplier"] = sql.Literal(1)

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

        Parameters
        ----------
        boundary : shapely geometry object
            shapely geometry representing the boundary for pulling the network
        osm_file : str, optional
            an OSM XML file to use instead of downloading data from the network
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


    def import_osm_destinations(self,osm_file=None,schema=None,boundary_file=None,
                                srid=None,destination_tags=None,overwrite=False,
                                keep_intermediates=False):
        """
        Processes OSM destinations and copies the data into the database.

        Parameters
        ----------
        osm_file : str, optional
            an OSM XML file to use instead of downloading data from the network
        schema : str, optional
            the schema to create the tables in (if not given, uses the DB default).
            only used if the table is not schema-qualified in the config file
        boundary_file : str, optional
            a boundary file path. if not given uses the boundary specified in the config
        srid : int or str, optional
            projection to use
        destination_tags : list, optional
            list of destination tags to be used instead of the default
        overwrite : bool, optional
            whether to overwrite any existing tables
        keep_intermediates : bool, optional
            saves the intermediate tables used to generate the final tables
        """
        if osm_file and not with_osmium:
            raise ValueError("Importing destinations from an OSM extract requires the osmium library")

        if schema is None:
            schema = self.get_default_schema()

        if srid is None:
            if "srid" in self.config:
                srid = self.config.srid
            else:
                raise ValueError("SRID must be specified as an arg or in the config file")

        boundary = self._load_boundary_as_dataframe(boundary_file=boundary_file)
        boundary = boundary.to_crs("epsg:4326")
        min_lon,min_lat,max_lon,max_lat = boundary.total_bounds

        table_prefix = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))

        # set up a list of dictionaries with info about each destination
        if destination_tags is None:
            destination_tags = self.get_destination_tags()

        conn = self.get_db_connection()
        for d in destination_tags:
            output_schema, table = self.parse_table_name(d["table"])
            if output_schema is None:
                output_schema = schema
            if not overwrite and self.table_exists(table,output_schema):
                conn.rollback()
                conn.close()
                raise ValueError("Table %s.%s already exists" % (output_schema,table))
            tags = d["tags_query"]
            print("Copying {} to database".format(table))
            if osm_file is None:
                ways, nodes = self._osm_destinations_from_overpass(min_lon,min_lat,max_lon,max_lat,tags)
                areas = dict()
                areas["features"] = list()
            else:
                areas, nodes = self._osm_destinations_from_file(min_lon,min_lat,max_lon,max_lat,osm_file,tags)
                ways = dict()
                ways["features"] = list()

            # set attributes
            attributes = set()
            for feature in areas["features"]:
                attributes = attributes.union(set(feature["properties"].keys()))
            for feature in ways["features"]:
                attributes = attributes.union(set(feature["properties"].keys()))
            for feature in nodes["features"]:
                attributes = attributes.union(set(feature["properties"].keys()))
            attributes = list(attributes)
            attributes.sort()
            attributes_sql = [sql.Identifier(a) for a in attributes]
            query_attributes = sql.SQL(" text,").join(attributes_sql)
            if len(attributes) > 0:
                query_attributes = sql.SQL(",") + query_attributes + sql.SQL(" text")

            # make tables
            query_make_table_areas = sql.SQL(
                "create table {}.{} (geom text,osmid bigint{})"
            ).format(
                sql.Identifier(output_schema),
                sql.Identifier(table_prefix+"_"+table+"_areas"),
                query_attributes
            )
            query_make_table_ways = sql.SQL(
                "create table {}.{} (geom text,osmid bigint{})"
            ).format(
                sql.Identifier(output_schema),
                sql.Identifier(table_prefix+"_"+table+"_ways"),
                query_attributes
            )
            query_make_table_nodes = sql.SQL(
                "create table {}.{} (geom text,osmid bigint{})"
            ).format(
                sql.Identifier(output_schema),
                sql.Identifier(table_prefix+"_"+table+"_nodes"),
                query_attributes
            )
            try:
                cur = conn.cursor()
                cur.execute(query_make_table_areas)
                cur.execute(query_make_table_ways)
                cur.execute(query_make_table_nodes)
                cur.close()
            except Exception as e:
                conn.rollback()
                conn.close()
                raise e

            # insert data
            ids_already_processed = set()
            for feature in areas["features"]:
                if feature["id"] in ids_already_processed:
                    # short circuit insertion if we've already seen this feature.
                    # for some reason duplicates are imported from osm
                    continue
                else:
                    ids_already_processed.add(feature["id"])
                    self._osm_destinations_table_insert(conn,attributes,feature,output_schema,table_prefix+"_"+table+"_areas")
            ids_already_processed = set()
            for feature in ways["features"]:
                if feature["id"] in ids_already_processed:
                    # short circuit insertion if we've already seen this feature.
                    # for some reason duplicates are imported from osm
                    continue
                else:
                    ids_already_processed.add(feature["id"])
                    self._osm_destinations_table_insert(conn,attributes,feature,output_schema,table_prefix+"_"+table+"_ways")
            ids_already_processed = set()
            for feature in nodes["features"]:
                if feature["id"] in ids_already_processed:
                    # short circuit insertion if we've already seen this feature.
                    # for some reason duplicates are imported from osm
                    continue
                else:
                    ids_already_processed.add(feature["id"])
                    self._osm_destinations_table_insert(conn,attributes,feature,output_schema,table_prefix+"_"+table+"_nodes")

            # process in the db
            subs = {
                "schema": sql.Identifier(output_schema),
                "final_table": sql.Identifier(table),
                "areas_table": sql.Identifier(table_prefix+"_"+table+"_areas"),
                "ways_table": sql.Identifier(table_prefix+"_"+table+"_ways"),
                "nodes_table": sql.Identifier(table_prefix+"_"+table+"_nodes"),
                "srid": sql.Literal(srid),
                "sidx": sql.Identifier("sidx_"+table),
                "pkey": sql.Identifier("id")
            }
            qpath = os.path.join(self.module_dir,"sql","importer","process_destinations.sql")
            if overwrite:
                self.drop_table(table,schema=output_schema,conn=conn)
            query = self.read_sql_from_file(qpath)
            q = sql.SQL(query).format(**subs)
            try:
                cur = conn.cursor()
                cur.execute(q)
                cur.close()
            except Exception as e:
                cur.close()
                conn.rollback()
                conn.close()
                raise e

            if not keep_intermediates:
                self.drop_table(table_prefix+"_"+table+"_areas",schema=output_schema,conn=conn)
                self.drop_table(table_prefix+"_"+table+"_ways",schema=output_schema,conn=conn)
                self.drop_table(table_prefix+"_"+table+"_nodes",schema=output_schema,conn=conn)

        conn.commit()
        conn.close()


    def _osm_destinations_table_insert(self,conn,attributes,feature,schema,table):
        """
        Copies features from the OSM GeoJSON into the database
        """
        geom = shape(feature["geometry"]).wkt

        values = []
        values.append(sql.Literal(geom))
        values.append(sql.Literal(feature["id"]))
        for a in attributes:
            if a in list(feature["properties"].keys()):
                values.append(sql.Literal(feature["properties"][a]))
            else:
                values.append(sql.SQL("NULL"))

        query_insert = sql.SQL("insert into {}.{} values({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(",").join(values)
        )
        try:
            cur = conn.cursor()
            cur.execute(query_insert)
            cur.close()
        except Exception as e:
            conn.rollback()
            conn.close()
            raise e


    def _osm_destinations_from_overpass(self,min_lon,min_lat,max_lon,max_lat,tags):
        """
        Submits an Overpass API query and returns a geojson of results

        Parameters
        ----------
        min_lon : int or float
            Minimum longitude
        min_lat : int or float
            Minimum latitude
        max_lon : int or float
            Maximum longitude
        max_lat : int or float
            Maximum latitude
        tags : list
            list of osm tags to use for filtering this destination type

        returns
        geojson of ways, geojson of nodes
        """
        query_root = ["({},{},{},{}){}".format(min_lat,min_lon,max_lat,max_lon,tag) for tag in tags]
        way_query = "way" + ";way".join(query_root) + ";"
        node_query = "node" + ";node".join(query_root) + ";"

        api = overpass.API(timeout=600)
        ways = api.get(way_query,verbosity="geom")
        nodes = api.get(node_query,verbosity="geom")

        return ways, nodes


    def _osm_destinations_from_file(self,min_lon,min_lat,max_lon,max_lat,osm_file,tags):
        """
        Extracts destinations from an OSM file and returns a geojson of results

        Parameters
        ----------
        min_lon : int or float
            Minimum longitude
        min_lat : int or float
            Minimum latitude
        max_lon : int or float
            Maximum longitude
        max_lat : int or float
            Maximum latitude
        osm_file : str
            an OSM XML file to use instead of downloading data from the network
        tags : list
            list of osm tags to use for filtering this destination type

        returns
        geojson of areas, geojson of nodes
        """
        handler = DestinationOSMHandler(tags)
        handler.apply_file(osm_file)
        nodes = FeatureCollection(handler.nodes_json)
        areas = FeatureCollection(handler.areas_json)

        return areas, nodes


    def _load_boundary_as_dataframe(self,boundary_file=None,srid=None):
        """
        Loads the boundary file as a geodataframe. If a file is given, uses
        the file. If not, reads the config and loads the boundary from the
        table indicated in the config.

        Parameters
        ----------
        boundary_file : str, optional
            path to a file
        srid : int or str, optional
            projection to use for the geodataframe (if none use the projection of the source data)

        returns
        geodataframe object
        """
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
            boundary = boundary.to_crs("epsg:{:d}".format(srid))
        return boundary
