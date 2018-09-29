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


class Connectivity:
    """pyBNA Connectivity class"""

    config = None
    net_config = None
    verbose = None
    debug = None
    db = None  # reference to DBUtils class
    srid = None
    blocks = None  # reference to Blocks class
    tiles = None
    net_blocks = None
    hs_graph = None
    ls_graph = None
    module_dir = None
    db_connectivity_table = None
    tiles_pkid = None

    # register pandas apply with tqdm for progress bar
    tqdm.pandas(desc="Evaluating connectivity")


    def _get_block_nodes(self):
        # set up substitutions
        subs = {
            "blocks_schema": sql.Identifier(self.blocks.schema),
            "blocks": sql.Identifier(self.blocks.table),
            "block_id": sql.Identifier(self.blocks.id_column),
            "block_geom": sql.Identifier(self.blocks.geom),
            "roads_schema": sql.Identifier(self.db.get_schema(self.net_config["roads"]["table"])),
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

        conn = self.db.get_db_connection()
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
            "schema": sql.Identifier(self.db.get_schema(self.net_config["roads"]["table"])),
            "roads": sql.Identifier(self.net_config["roads"]["table"]),
            "road_id": sql.Identifier(self.net_config["roads"]["uid"]),
            "roads_geom": sql.Identifier(self.net_config["roads"]["geom"]),
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
            "tf_int_stress": sql.Identifier(self.net_config["roads"]["stress"]["crossing"]["backward"])
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

        conn = self.db.get_db_connection()
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

        conn.commit()
        cur.close()
        conn.close()


    def check_network(self):
        """
        Checks for the db network tables identified in the config file.

        returns True if they exist, False if they don't
        """
        conn = self.db.get_db_connection()
        for table in [self.net_config["edges"]["table"],self.net_config["nodes"]["table"]]:
            if self.verbose:
                print("Checking for %s in database" % table)
            if not self.db.table_exists(table):
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
        conn = self.db.get_db_connection()
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
        conn = self.db.get_db_connection()
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

        conn = self.db.get_db_connection()
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
            conn = self.db.get_db_connection()
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

        # drop db table or check existence if append mode set
        if not append and not dry:
            self._connectivity_table_create(overwrite=False)
        elif not self.db.table_exists(self.db_connectivity_table) and not dry:
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
                "vert_table": sql.Identifier(self.net_config["nodes"]["table"]),
                "vert_id_col": sql.Identifier(self.net_config["nodes"]["id_column"]),
                "road_id": sql.Identifier("road_id"),
                "connectivity_table": sql.Identifier(self.db_connectivity_table),
                "conn_source_col": sql.Identifier(self.config["bna"]["connectivity"]["source_column"]),
                "conn_target_col": sql.Identifier(self.config["bna"]["connectivity"]["target_column"]),
                "max_trip_distance": sql.Literal(self.config["bna"]["connectivity"]["max_distance"]),
                "max_detour": sql.Literal(self.config["bna"]["connectivity"]["max_detour"]),
                "hs_link_query": sql.Literal(hs_link_query),
                "ls_link_query": sql.Literal(ls_link_query)
            }

            f = open(os.path.join(self.module_dir,"sql","connectivity","tile_based_connectivity.sql"))
            raw = f.read()
            f.close()

            conn = self.db.get_db_connection()

            statements = self.db.split_sql_for_tqdm(raw)

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

        conn = self.db.get_db_connection()
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
