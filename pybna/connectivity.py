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


    def build_network(self,dry=False):
        """
        Builds the network in the DB using details from the BNA config file.

        args:
        dry -- dry run only, don't execute the query in the DB (for debugging)
        """
        if self.verbose:
            print("Building network in database")

        # set up substitutions
        subs = dict(self.sql_subs)
        subs["nodes_index"] = sql.Identifier("sidx_"+self.config.bna.network.nodes.table)
        subs["edges_index"] = sql.Identifier("sidx_"+self.config.bna.network.edges.table)

        # read in the raw query language
        create_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","build_network","create_tables.sql"))
        nodes_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","build_network","insert_nodes.sql"))
        edges_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","build_network","insert_edges.sql"))
        cleanup_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","build_network","cleanup.sql"))
        associate_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","build_network","associate_roads_with_blocks.sql"))

        conn = self.get_db_connection()
        cur = conn.cursor()

        # create
        print("Creating network tables")
        q = sql.SQL(create_query).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            cur.execute(q)

        # nodes
        print("Adding network nodes")
        q = sql.SQL(nodes_query).format(**subs)
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
                q = sql.SQL(statement).format(**subs)

                if dry:
                    print(q.as_string(conn))
                else:
                    cur.execute(q)

        # cleanup
        print("Finishing up network")
        q = sql.SQL(cleanup_query).format(**subs)
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
                q = sql.SQL(statement).format(**subs)

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
        return True


    def _connectivity_table_create(self,overwrite=False):
        """
        Creates the connectivity table in the database
        """
        conn = self.get_db_connection()
        cur = conn.cursor()
        if overwrite:
            cur.execute(
                sql.SQL(
                    'DROP TABLE IF EXISTS {connectivity_schema}.{connectivity_table}'
                ).format(**self.sql_subs)
            )
        try:
            raw = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","create_table.sql"))
            q = sql.SQL(raw).format(**self.sql_subs)
            cur.execute(q)
        except psycopg2.ProgrammingError:
            conn.rollback()
            conn.close()
            raise ValueError("Table %s already exists" % self.config.bna.connectivity.table)
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
        ").format(
            sql.Literal(
                "'" +
                self.config.bna.connectivity.schema +
                "'.'" +
                self.config.bna.connectivity.table +
                "'"
            )
        )
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
        # make a copy of sql substitutes
        subs = dict(self.sql_subs)
        subs["connectivity_index"] = sql.Identifier("idx_" + self.config.bna.connectivity.table + "_low_stress")

        conn = self.get_db_connection()
        cur = conn.cursor()
        if overwrite:
            self._connectivity_table_drop_index()

        cur.execute(sql.SQL(" \
            CREATE INDEX {connectivity_index} \
            ON {connectivity_schema}.{connectivity_table} ({connectivity_source_col},{connectivity_target_col}) \
            WHERE low_stress \
        ").format(**subs))
        conn.commit()
        cur.execute(sql.SQL("analyze {connectivity_schema}.{connectivity_table}").format(**subs));


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
        if network_filter is None:
            network_filter = "TRUE"

        # make a copy of sql substitutes
        subs = dict(self.sql_subs)
        subs["network_filter"] = sql.SQL(network_filter)

        # check tiles
        if tiles is None:
            conn = self.get_db_connection()
            cur = conn.cursor()
            cur.execute(
                sql.SQL("select {tiles_id_col} from {tiles_schema}.{tiles_table}").format(**subs)
            )
            tiles = []
            for row in cur:
                tiles.append(row[0])
        elif not type(tiles) == list and not type(tiles) == tuple:
            raise ValueError("Tile IDs must be given as an iterable")

        # create db table or check existence if append mode set
        if not append and not dry:
            self._connectivity_table_create(overwrite=False)
        if append and not dry:
            if not self.table_exists(self.db_connectivity_table):
                raise ValueError("table %s not found" % self.db_connectivity_table)

        # get raw queries
        q_filter_zones_to_tile = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","filter_zones_to_tile.sql"))
        q_zone_nodes = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","zone_nodes.sql"))
        q_network_subset = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","network_subset.sql"))
        q_distance_table = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","distance_table.sql"))
        q_cost_to_zones = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","cost_to_zones.sql"))
        q_combine = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","combine_cost_matrices.sql"))

        tile_progress = tqdm(tiles)
        failed_zones = list()

        for tile_id in tile_progress:
            tile_progress.set_description("Tile id: "+str(tile_id))
            subs["tile_id"] = sql.Literal(tile_id)

            conn = self.get_db_connection()
            cur = conn.cursor()

            # filter zones to tile
            q = sql.SQL(q_filter_zones_to_tile).format(**subs)
            if dry:
                print(q.as_string(conn))
            else:
                cur.execute(q)

            # flatten zones and nodes
            q = sql.SQL(q_zone_nodes).format(**subs)
            if dry:
                print(q.as_string(conn))
            else:
                cur.execute(q)

            # subset hs network
            subs["max_stress"] = sql.Literal(99)
            subs["net_table"] = sql.Identifier("tmp_hs_net")
            q = sql.SQL(q_network_subset).format(**subs)
            if dry:
                print(q.as_string(conn))
            else:
                cur.execute(q)

            # subset ls network
            subs["max_stress"] = sql.Literal(self.config.bna.connectivity.max_stress)
            subs["net_table"] = sql.Identifier("tmp_ls_net")
            q = sql.SQL(q_network_subset).format(**subs)
            if dry:
                print(q.as_string(conn))
            else:
                cur.execute(q)

            # retrieve zones and loop through
            if dry:
                zones = [(-100,[-200])]
            else:
                cur.execute("select id, node_ids from pg_temp.tmp_tilezones")
                zones = cur.fetchall()

            zone_progress = tqdm(zones)
            for zone in zone_progress:
                failure = False
                zone_id = zone[0]
                node_ids = zone[1]
                subs["zone_id"] = sql.Literal(zone_id)
                subs["node_ids"] = sql.Literal(node_ids)

                # get hs zone costs
                subs["net_table"] = sql.Identifier("tmp_hs_net")
                subs["distance_table"] = sql.Identifier("tmp_hs_distance")
                subs["cost_to_zones"] = sql.Identifier("tmp_hs_cost_to_zones")

                q = sql.SQL(q_distance_table).format(**subs)
                if dry:
                    print(q.as_string(conn))
                else:
                    try:
                        cur2 = conn.cursor()
                        cur2.execute(q)
                        cur2.close()
                    except:
                        failure = True
                        failed_zones.append(zone_id)
                        time.sleep(5)
                        continue

                q = sql.SQL(q_cost_to_zones).format(**subs)
                if dry:
                    print(q.as_string(conn))
                else:
                    try:
                        cur2 = conn.cursor()
                        cur2.execute(q)
                        cur2.close()
                    except:
                        failure = True
                        failed_zones.append(zone_id)
                        time.sleep(5)
                        continue

                # get ls zone costs
                subs["net_table"] = sql.Identifier("tmp_ls_net")
                subs["distance_table"] = sql.Identifier("tmp_ls_distance")
                subs["cost_to_zones"] = sql.Identifier("tmp_ls_cost_to_zones")
                q = sql.SQL(q_distance_table).format(**subs)
                if dry:
                    print(q.as_string(conn))
                else:
                    try:
                        cur2 = conn.cursor()
                        cur2.execute(q)
                        cur2.close()
                    except:
                        failure = True
                        failed_zones.append(zone_id)
                        time.sleep(5)
                        continue
                q = sql.SQL(q_cost_to_zones).format(**subs)
                if dry:
                    print(q.as_string(conn))
                else:
                    try:
                        cur2 = conn.cursor()
                        cur2.execute(q)
                        cur2.close()
                    except:
                        failure = True
                        failed_zones.append(zone_id)
                        time.sleep(5)
                        continue

                # build combined cost table and write to connectivity table
                q = sql.SQL(q_combine).format(**subs)
                if dry:
                    print(q.as_string(conn))
                else:
                    try:
                        cur2 = conn.cursor()
                        cur2.execute(q)
                        cur2.close()
                    except:
                        failure = True
                        failed_zones.append(zone_id)
                        time.sleep(5)
                        continue

                # if dry, break after one go-round so we don't overload the output
                if dry:
                    zone_progress.close()
                    break

            if not dry:
                conn.commit()
            conn.close()

        print("\n\n------------------------------------")
        print("Process completed with %i failed tiles" % len(failed_zones))
        if len(failed_zones) > 0:
            print(failed_zones)
        print("------------------------------------\n")

        if not dry and not append:
            self._connectivity_table_create_index();
