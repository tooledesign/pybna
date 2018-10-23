###################################################################
# The Scenario class stores a BNA scenario for use in pyBNA.
# A scenario includes two graphs: one for high stress, one for low stress
###################################################################
import sys, os, StringIO, random, string
# import collections
# from graph_tool.all import *
import psycopg2
from psycopg2 import sql
# from tempfile import mkstemp
# import numpy as np
# from scipy.sparse import coo_matrix
# import pandas as pd
# import geopandas as gpd
from tqdm import tqdm
import time

from dbutils import DBUtils


class Connectivity(DBUtils):
    """pyBNA Connectivity class"""

    def __init__(self):
        DBUtils.__init__(self,"")
        self.config = None
        self.net_config = None
        self.verbose = None
        self.debug = None
        self.srid = None
        self.blocks = None  # reference to Blocks class
        self.tiles = None
        self.net_blocks = None
        self.module_dir = None
        self.db_connectivity_table = None
        self.tiles_pkid = None
        self.db_connection_string = None

        # register pandas apply with tqdm for progress bar
        # tqdm.pandas(desc="Evaluating connectivity")


    def _get_block_nodes(self):
        # set up substitutions
        subs = {
            "blocks_schema": sql.Identifier(self.blocks.schema),
            "blocks": sql.Identifier(self.blocks.table),
            "block_id": sql.Identifier(self.blocks.id_column),
            "block_geom": sql.Identifier(self.blocks.geom),
            "roads_schema": sql.Identifier(self.get_schema(self.net_config["roads"]["table"])),
            "roads": sql.Identifier(self.net_config["roads"]["table"]),
            "road_id": sql.Identifier(self.net_config["roads"]["uid"]),
            "road_geom": sql.Identifier(self.net_config["roads"]["geom"]),
            "nodes": sql.Identifier(self.net_config["nodes"]["table"]),
            "node_id": sql.Identifier(self.net_config["nodes"]["id_column"]),
            "distance": sql.Literal(self.config["bna"]["blocks"]["roads_tolerance"]),
            "min_length": sql.Literal(self.config["bna"]["blocks"]["min_road_length"])
        }

        # read in the raw query language
        f = open(os.path.join(self.module_dir,"sql","block_nodes.sql"))
        raw = f.read()
        f.close()

        conn = self.get_db_connection()
        q = sql.SQL(raw).format(**subs).as_string(conn)

        if self.debug:
            print(q)

        return pd.read_sql_query(
            q,
            conn
        )

        conn.close()


    def build_network(self,dry=False):
        """
        Builds the network in the DB using details from the BNA config file.

        args:
        dry -- dry run only, don't execute the query in the DB (for debugging)
        """
        if self.verbose:
            print("Building network in database")

        # set up substitutions
        net_subs = {
            "srid": sql.Literal(self.srid),
            "roads_schema": sql.Identifier(self.get_schema(self.net_config["roads"]["table"])),
            "roads_table": sql.Identifier(self.net_config["roads"]["table"]),
            "roads_id_col": sql.Identifier(self.net_config["roads"]["uid"]),
            "roads_geom_col": sql.Identifier(self.net_config["roads"]["geom"]),
            "road_source": sql.Identifier(self.net_config["roads"]["source_column"]),
            "road_target": sql.Identifier(self.net_config["roads"]["target_column"]),
            "one_way": sql.Identifier(self.net_config["roads"]["oneway"]["name"]),
            "forward": sql.Literal(self.net_config["roads"]["oneway"]["forward"]),
            "backward": sql.Literal(self.net_config["roads"]["oneway"]["backward"]),
            "intersections": sql.Identifier(self.net_config["intersections"]["table"]),
            "int_id": sql.Identifier(self.net_config["intersections"]["uid"]),
            "nodes": sql.Identifier(self.net_config["nodes"]["table"]),
            "node_id": sql.Identifier(self.net_config["nodes"]["id_column"]),
            "node_index": sql.Identifier("sidx_"+self.net_config["nodes"]["table"]),
            "edges": sql.Identifier(self.net_config["edges"]["table"]),
            "edge_id": sql.Identifier(self.net_config["edges"]["id_column"]),
            "edge_index": sql.Identifier("sidx_"+self.net_config["edges"]["table"]),
            "ft_seg_stress": sql.Identifier(self.net_config["roads"]["stress"]["segment"]["forward"]),
            "tf_seg_stress": sql.Identifier(self.net_config["roads"]["stress"]["segment"]["backward"]),
            "ft_int_stress": sql.Identifier(self.net_config["roads"]["stress"]["crossing"]["forward"]),
            "tf_int_stress": sql.Identifier(self.net_config["roads"]["stress"]["crossing"]["backward"]),
            "blocks_schema": sql.Identifier(self.blocks.schema),
            "blocks_table": sql.Identifier(self.blocks.table),
            "block_id_col": sql.Identifier(self.blocks.id_column),
            "block_geom_col": sql.Identifier(self.blocks.geom),
            "roads_tolerance": sql.Literal(self.config["bna"]["blocks"]["roads_tolerance"]),
            "min_road_length": sql.Literal(self.config["bna"]["blocks"]["min_road_length"])
        }

        # read in the raw query language
        f = open(os.path.join(self.module_dir,"sql","build_network","create_tables.sql"))
        create_query = f.read()
        f.close()
        f = open(os.path.join(self.module_dir,"sql","build_network","insert_nodes.sql"))
        nodes_query = f.read()
        f.close()
        f = open(os.path.join(self.module_dir,"sql","build_network","insert_edges.sql"))
        edges_query = f.read()
        f.close()
        f = open(os.path.join(self.module_dir,"sql","build_network","cleanup.sql"))
        cleanup_query = f.read()
        f.close()
        f = open(os.path.join(self.module_dir,"sql","build_network","associate_roads_with_blocks.sql"))
        associate_query = f.read()
        f.close()

        conn = self.get_db_connection()
        cur = conn.cursor()

        # create
        print("Creating network tables")
        q = sql.SQL(create_query).format(**net_subs)
        if dry:
            print(q.as_string(conn))
        else:
            cur.execute(q)

        # nodes
        print("Adding network nodes")
        q = sql.SQL(nodes_query).format(**net_subs)
        if dry:
            print(q.as_string(conn))
        else:
            cur.execute(q)

        # edges
        print("Adding network edges")
        statements = [s for s in edges_query.split(";") if len(s.strip()) > 1]
        prog_statements = tqdm(statements)
        for statement in prog_statements:
            # handle progress updates
            if statement.strip()[:2] == '--':
                prog_statements.set_description(statement.strip()[2:])
            else:
                # compose the query
                q = sql.SQL(statement).format(**net_subs)

                if dry:
                    print(q.as_string(conn))
                else:
                    cur.execute(q)

        # cleanup
        print("Finishing up network")
        q = sql.SQL(cleanup_query).format(**net_subs)
        if dry:
            print(q.as_string(conn))
        else:
            cur.execute(q)

        # associate_roads_with_blocks
        print("Associating roads with blocks")
        statements = [s for s in associate_query.split(";") if len(s.strip()) > 1]
        prog_statements = tqdm(statements)
        for statement in prog_statements:
            # handle progress updates
            if statement.strip()[:2] == '--':
                prog_statements.set_description(statement.strip()[2:])
            else:
                # compose the query
                q = sql.SQL(statement).format(**net_subs)

                if dry:
                    print(q.as_string(conn))
                else:
                    cur.execute(q)

        conn.commit()
        cur.close()
        conn.close()


    def check_network(self):
        """
        Checks for the db network tables identified in the config file.

        returns True if they exist, False if they don't
        """
        conn = self.get_db_connection()
        for table in [self.net_config["edges"]["table"],self.net_config["nodes"]["table"]]:
            if self.verbose:
                print("Checking for %s in database" % table)
            if not self.table_exists(table):
                return False
        #
        #     try:
        #         cur = conn.cursor()
        #         cur.execute(
        #             sql.SQL(
        #                 "select * from {} limit 1"
        #             ).format(
        #                 sql.Identifier(table)
        #             )
        #         )
        #         cur.fetchone()
        #         cur.close()
        #     except psycopg2.ProgrammingError:
        #         conn.close()
        #         return False
        #
        # # no errors = tables found
        # conn.close
        return True


    def _connectivity_table_create(self,overwrite=False):
        """
        Creates the connectivity table in the database
        """
        conn = self.get_db_connection()
        cur = conn.cursor()
        if overwrite:
            cur.execute(sql.SQL('drop table if exists {}').format(sql.Identifier(self.db_connectivity_table)))
        try:
            cur.execute(sql.SQL(
                'create table {}.{} ( \
                    id serial primary key, \
                    {} varchar(15), \
                    {} varchar(15), \
                    high_stress BOOLEAN, \
                    low_stress BOOLEAN \
                )'
            ).format(
                sql.Identifier(self.blocks.schema),
                sql.Identifier(self.db_connectivity_table),
                sql.Identifier(self.config["bna"]["connectivity"]["source_column"]),
                sql.Identifier(self.config["bna"]["connectivity"]["target_column"])
            ))
        except psycopg2.ProgrammingError:
            conn.rollback()
            conn.close()
            raise ValueError("Table %s already exists" % self.db_connectivity_table)
        cur.close()
        conn.commit()
        conn.close()


    def _connectivity_table_drop_index(self):
        # adapted from https://stackoverflow.com/questions/34010401/how-can-i-drop-all-indexes-of-a-table-in-postgres
        """
        Drops indexes on the connectivity table
        """
        conn = self.get_db_connection()
        cur = conn.cursor()
        cur.execute("\
            SELECT indexrelid::regclass::text \
            FROM   pg_index  i \
                LEFT   JOIN pg_depend d ON d.objid = i.indexrelid \
                AND d.deptype = 'i' \
            WHERE  i.indrelid = {}::regclass \
            AND    d.objid IS NULL \
        ").format(sql.Literal(self.db_connectivity_table))
        for row in cur:
            if row[0] is None:
                pass
            else:
                q = "drop index " + row[0] + ";"
                cur2 = conn.cursor()
                cur2.execute(q)
                cur2.close()
        cur.close()
        conn.commit()
        conn.close()


    def _connectivity_table_create_index(self,overwrite=False):
        """
        Creates index on the connectivity table
        """
        source = self.config["bna"]["connectivity"]["source_column"]
        target = self.config["bna"]["connectivity"]["target_column"]

        conn = self.get_db_connection()
        cur = conn.cursor()
        if overwrite:
            self._connectivity_table_drop_index()

        cur.execute(sql.SQL(" \
            CREATE INDEX {} ON {} ({},{}) WHERE low_stress \
        ").format(
            sql.Identifier("idx_" + self.db_connectivity_table + "_low_stress"),
            sql.Identifier(self.db_connectivity_table),
            sql.Identifier(source),
            sql.Identifier(target)
        ))
        conn.commit()
        cur.execute(sql.SQL("analyze {}").format(sql.Identifier(self.db_connectivity_table)));


    def calculate_connectivity(self,tiles=None,network_filter=None,append=False,dry=False):
        """
        Prepares and executes queries to do connectivity analysis within the
        database. Operates on tiles and adds results as each tile completes.

        args
        tiles -- list of tile IDs to operate on. if empty use all tiles
        network_filter -- filter to be applied to the road network when routing
        append -- append to existing db table instead of creating a new one
        dry -- only prepare the query language but don't execute in the database
        """
        # check tiles
        if tiles is None:
            conn = self.get_db_connection()
            cur = conn.cursor()
            cur.execute(sql.SQL("select {} from {}").format(
                sql.Identifier(self.tiles_pkid),
                sql.Identifier(self.config["bna"]["tiles"]["table"])
            ))
            tiles = []
            for row in cur:
                tiles.append(row[0])
        elif not type(tiles) == list and not type(tiles) == tuple:
            raise ValueError("Tile IDs must be given as an iterable")

        # check zones
        zones_table = None
        zones_schema = None
        zones_uid = None
        zones_geom = None
        if "zones" in self.config["bna"]["connectivity"]:
            zones_table = self.config["bna"]["connectivity"]["zones"]["table"]
            if "schema" in self.config["bna"]["connectivity"]["zones"]:
                zones_schema = self.config["bna"]["connectivity"]["zones"]["schema"]
            if "uid" in self.config["bna"]["connectivity"]["zones"]:
                zones_uid = self.config["bna"]["connectivity"]["zones"]["uid"]
            if "geom" in self.config["bna"]["connectivity"]["zones"]:
                zones_geom = self.config["bna"]["connectivity"]["zones"]["geom"]

            if self.table_exists(zones_table):
                if zones_schema is None:
                    zones_schema = self.get_schema(zones_table)
                if zones_uid is None:
                    zones_uid = self.get_pkid_col(zones_table,zones_schema)
                if zones_geom is None:
                    zones_geom = "geom"
            else:
                zones_schema = self.blocks.schema
                zones_uid = "id"
                zones_geom = "geom"

        # drop db table or check existence if append mode set
        if not append and not dry:
            self._connectivity_table_create(overwrite=False)
        elif not self.table_exists(self.db_connectivity_table) and not dry:
            raise ValueError("table %s not found" % self.db_connectivity_table)

        tile_progress = tqdm(tiles)
        failed_tiles = list()

        for tile_id in tile_progress:
            failure = False
            tile_progress.set_description("Tile id: "+str(tile_id))
            hs_link_query = self._build_link_query(tile_id,filter=network_filter)
            ls_link_query = self._build_link_query(tile_id,max_stress=self.config["bna"]["connectivity"]["max_stress"],filter=network_filter)

            subs = {
                "blocks_table": sql.Identifier(self.config["bna"]["blocks"]["table"]),
                "block_id_col": sql.Identifier(self.config["bna"]["blocks"]["id_column"]),
                "block_geom_col": sql.Identifier(self.config["bna"]["blocks"]["geom"]),
                "tiles_table": sql.Identifier(self.config["bna"]["tiles"]["table"]),
                "tile_id_col": sql.Identifier(self.tiles_pkid),
                "tile_geom_col": sql.Identifier(self.config["bna"]["tiles"]["geom"]),
                "tile_id": sql.Literal(tile_id),
                "intersections": sql.Identifier(self.net_config["intersections"]["table"]),
                "int_id": sql.Identifier(self.net_config["intersections"]["uid"]),
                "vert_table": sql.Identifier(self.net_config["nodes"]["table"]),
                "vert_id_col": sql.Identifier(self.net_config["nodes"]["id_column"]),
                "roads_schema": sql.Identifier(self.get_schema(self.net_config["roads"]["table"])),
                "roads_table": sql.Identifier(self.net_config["roads"]["table"]),
                "roads_id_col": sql.Identifier(self.net_config["roads"]["uid"]),
                "ft_seg_stress": sql.Identifier(self.net_config["roads"]["stress"]["segment"]["forward"]),
                "tf_seg_stress": sql.Identifier(self.net_config["roads"]["stress"]["segment"]["backward"]),
                "ft_int_stress": sql.Identifier(self.net_config["roads"]["stress"]["crossing"]["forward"]),
                "tf_int_stress": sql.Identifier(self.net_config["roads"]["stress"]["crossing"]["backward"]),
                "road_source": sql.Identifier(self.net_config["roads"]["source_column"]),
                "road_target": sql.Identifier(self.net_config["roads"]["target_column"]),
                "connectivity_table": sql.Identifier(self.db_connectivity_table),
                "conn_source_col": sql.Identifier(self.config["bna"]["connectivity"]["source_column"]),
                "conn_target_col": sql.Identifier(self.config["bna"]["connectivity"]["target_column"]),
                "max_trip_distance": sql.Literal(self.config["bna"]["connectivity"]["max_distance"]),
                "max_detour": sql.Literal(self.config["bna"]["connectivity"]["max_detour"]),
                "max_stress": self.config["bna"]["connectivity"]["max_stress"],
                "hs_link_query": sql.Literal(hs_link_query),
                "ls_link_query": sql.Literal(ls_link_query)
            }

            f = open(os.path.join(self.module_dir,"sql","connectivity","tile_based_connectivity.sql"))
            raw = f.read()
            f.close()

            conn = self.get_db_connection()

            statements = self.split_sql_for_tqdm(raw)

            for statement in statements:
                statements.set_description(statement["update"])
                q = sql.SQL(statement["query"]).format(**subs)

                if dry:
                    print(q.as_string(conn))
                else:
                    try:
                        cur = conn.cursor()
                        cur.execute(q)
                        cur.close()
                    except psycopg2.OperationalError:
                        print("Tile %s failed" % str(tile_id))
                        failed_tiles.append(tile_id)
                        failure = True
                        time.sleep(60)
                        break

            if not failure:
                conn.commit()
            conn.close()

        print("\n\n------------------------------------")
        print("Process completed with %i failed tiles" % len(failed_tiles))
        if len(failed_tiles) > 0:
            print(failed_tiles)
        print("------------------------------------\n")

        if not dry and not append:
            self._connectivity_table_create_index();


    def _build_link_query(self,tile_id,max_stress=99,filter=None):
        """
        Prepares the query of road network features passed to pgrouting for the
        routing analysis.
        """

        if filter is None:
            filter = "TRUE"

        conn = self.get_db_connection()
        cur = conn.cursor()

        subs = {
            "link_table": sql.Identifier(self.net_config["edges"]["table"]),
            "link_id_col": sql.Identifier(self.net_config["edges"]["id_column"]),
            "link_cost_col": sql.Identifier(self.net_config["edges"]["cost_column"]),
            "link_stress_col": sql.Identifier(self.net_config["edges"]["stress_column"]),
            "tiles_table": sql.Identifier(self.config["bna"]["tiles"]["table"]),
            "tile_id_col": sql.Identifier(self.tiles_pkid),
            "tile_geom_col": sql.Identifier(self.config["bna"]["tiles"]["geom"]),
            "tile_id": sql.Literal(tile_id),
            "max_trip_distance": sql.Literal(self.config["bna"]["connectivity"]["max_distance"]),
            "max_stress": sql.Literal(max_stress),
            "filter": sql.SQL(filter)
        }

        f = open(os.path.join(self.module_dir,"sql","connectivity","link_query.sql"))
        raw = f.read()
        f.close()

        q = sql.SQL(raw).format(**subs).as_string(conn)
        conn.close()

        return q


    def make_zones(table,schema=None,uid="id",geom="geom"):
        """
        Creates analysis zones that aggregate blocks into logical groupings
        based on islands of 100% low stress connectivity

        args
        table -- table name
        schema -- schema name
        uid -- uid column name
        geom -- geom column name
        """
