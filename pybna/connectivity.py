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
        self.net_blocks = None
        self.module_dir = None
        self.db_connectivity_table = None
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
        # associate_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","build_network","associate_roads_with_blocks.sql"))

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
        # print("Associating roads with blocks")
        # statements = [s for s in associate_query.split(";") if len(s.strip()) > 1]
        # prog_statements = tqdm(statements)
        # for statement in prog_statements:
        #     # handle progress updates
        #     if statement.strip()[:2] == '--':
        #         prog_statements.set_description(statement.strip()[2:])
        #     else:
        #         # compose the query
        #         q = sql.SQL(statement).format(**subs)
        #
        #         if dry:
        #             print(q.as_string(conn))
        #         else:
        #             cur.execute(q)

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


    def _get_block_ids(self):
        """
        Returns a list of all block IDs from the database
        """
        conn = self.get_db_connection()
        cur = conn.cursor()
        cur.execute(
            sql.SQL("select {blocks_id_col} from {blocks_schema}.{blocks_table}").format(**self.sql_subs)
        )
        blocks = []
        for row in cur:
            blocks.append(row[0])
        return blocks


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


    def calculate_connectivity(self,blocks=None,network_filter=None,append=False,dry=False):
        """
        Organizes and calls SQL scripts for calculating connectivity.

        args
        blocks -- list of block IDs to use as origins. if empty use all blocks.
        network_filter -- filter to be applied to the road network when routing
        append -- append to existing db table instead of creating a new one
        dry -- only prepare the query language but don't execute in the database
        """
        subs = dict(self.sql_subs)

        if network_filter is None:
            network_filter = "TRUE"
        subs["network_filter"] = sql.SQL(network_filter)

        # check blocks
        if blocks is None:
            blocks = self._get_block_ids()
        elif not type(blocks) == list and not type(blocks) == tuple:
            raise ValueError("Block IDs must be given as an iterable")

        # create db table or check existence if append mode set
        if not append and not dry:
            self._connectivity_table_create(overwrite=False)
        if append and not dry:
            if not self.table_exists(self.db_connectivity_table):
                raise ValueError("table %s not found" % self.db_connectivity_table)

        # get raw queries
        q_filter_this_block = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","calculation","10_filter_this_block.sql"))
        q_assign_nodes_to_blocks = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","calculation","20_assign_nodes_to_blocks.sql"))
        q_network_subset = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","calculation","30_network_subset.sql"))
        q_this_block_nodes = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","calculation","35_this_block_nodes.sql"))
        q_distance_table = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","calculation","40_distance_table.sql"))
        q_cost_to_blocks = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","calculation","60_cost_to_blocks.sql"))

        q_combine = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","calculation","70_combine_cost_matrices.sql"))

        block_progress = tqdm(blocks)
        failed_blocks = list()

        for block_id in block_progress:
            failure = False
            block_progress.set_description("Block id: "+str(block_id))
            subs["block_id"] = sql.Literal(block_id)

            conn = self.get_db_connection()
            cur = conn.cursor()

            # filter blocks
            q = sql.SQL(q_filter_this_block).format(**subs)
            if dry:
                print(q.as_string(conn))
            else:
                cur.execute(q)

            # associate blocks and nodes
            q = sql.SQL(q_assign_nodes_to_blocks).format(**subs)
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

            # get hs nodes
            if dry:
                hs_nodes = {-1}
            else:
                cur.execute("select distinct source from tmp_hs_net union select distinct target from tmp_hs_net")
                hs_nodes = set(n[0] for n in cur.fetchall())

            # subset ls network
            subs["max_stress"] = sql.Literal(self.config.bna.connectivity.max_stress)
            subs["net_table"] = sql.Identifier("tmp_ls_net")
            q = sql.SQL(q_network_subset).format(**subs)
            if dry:
                print(q.as_string(conn))
            else:
                cur.execute(q)

            # get ls nodes
            if dry:
                ls_nodes = {-1}
            else:
                cur.execute("select distinct source from tmp_ls_net union select distinct target from tmp_ls_net")
                ls_nodes = set(n[0] for n in cur.fetchall())

            # retrieve nodes for this block and loop through
            if dry:
                node_ids = {-1}
            else:
                q = sql.SQL(q_this_block_nodes).format(**subs)
                cur.execute(q)
                if cur.rowcount >= 0:
                    node_ids = set()
                else:
                    node_ids = set(cur.fetchone()[0])
            hs_node_ids = list(node_ids & hs_nodes)
            ls_node_ids = list(node_ids & ls_nodes)

            # get hs block costs
            subs["node_ids"] = sql.Literal(hs_node_ids)
            subs["net_table"] = sql.Identifier("tmp_hs_net")
            subs["distance_table"] = sql.Identifier("tmp_hs_distance")
            subs["cost_to_blocks"] = sql.Identifier("tmp_hs_cost_to_blocks")

            if len(hs_node_ids) == 0:
                cur2 = conn.cursor()
                cur2.execute("create temp table tmp_hs_cost_to_blocks (id int, agg_cost float)")
                cur2.close()
            else:
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
                        failed_blocks.append(block_id)
                        time.sleep(2)
                        continue

                q = sql.SQL(q_cost_to_blocks).format(**subs)
                if dry:
                    print(q.as_string(conn))
                else:
                    try:
                        cur2 = conn.cursor()
                        cur2.execute(q)
                        cur2.close()
                    except:
                        failure = True
                        failed_blocks.append(block_id)
                        time.sleep(2)
                        continue

            # get ls block costs
            subs["node_ids"] = sql.Literal(ls_node_ids)
            subs["net_table"] = sql.Identifier("tmp_ls_net")
            subs["distance_table"] = sql.Identifier("tmp_ls_distance")
            subs["cost_to_blocks"] = sql.Identifier("tmp_ls_cost_to_blocks")

            if len(ls_node_ids) == 0:
                cur2 = conn.cursor()
                cur2.execute("create temp table tmp_ls_cost_to_blocks (id int, agg_cost float)")
                cur2.close()
            else:
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
                        failed_blocks.append(block_id)
                        time.sleep(2)
                        continue
                q = sql.SQL(q_cost_to_blocks).format(**subs)
                if dry:
                    print(q.as_string(conn))
                else:
                    try:
                        cur2 = conn.cursor()
                        cur2.execute(q)
                        cur2.close()
                    except:
                        failure = True
                        failed_blocks.append(block_id)
                        time.sleep(2)
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
                    failed_blocks.append(block_id)
                    time.sleep(2)
                    continue

            # if dry, break after one go-round so we don't overload the output
            if dry:
                break

            if not dry:
                conn.commit()
            conn.close()

        print("\n\n------------------------------------")
        print("Process completed with %i failed units" % len(failed_blocks))
        if len(failed_blocks) > 0:
            print(failed_blocks)
        print("------------------------------------\n")

        if not dry and not append:
            self._connectivity_table_create_index();
