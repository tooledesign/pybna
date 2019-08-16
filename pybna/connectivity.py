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


    def build_network(self,dry=None):
        """
        Builds the network in the DB using details from the BNA config file.

        args:
        dry -- a path to save SQL statements to instead of executing in DB
        """
        if self.verbose:
            print("Building network in database")

        # set up substitutions
        subs = dict(self.sql_subs)
        subs["nodes_index"] = sql.Identifier("sidx_"+self.config.bna.network.nodes.table)
        subs["edges_index"] = sql.Identifier("sidx_"+self.config.bna.network.edges.table)

        # run scripts
        conn = self.get_db_connection()
        print("Creating network tables")
        self._run_sql_script("create_tables.sql",subs,["sql","build_network"],dry=dry,conn=conn)
        print("Adding network nodes")
        self._run_sql_script("insert_nodes.sql",subs,["sql","build_network"],dry=dry,conn=conn)
        print("Adding network edges")
        self._run_sql_script("insert_edges.sql",subs,["sql","build_network"],dry=dry,conn=conn)
        print("Finishing up network")
        self._run_sql_script("cleanup.sql",subs,["sql","build_network"],dry=dry,conn=conn)

        conn.commit()
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
        ").format(sql.Literal(self.config.bna.connectivity.table))
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
        s,t = self.parse_table_name(self.config.bna.connectivity.table)
        idx = "idx_" + t + "_low_stress"
        subs["connectivity_index"] = sql.Identifier(idx)

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


    def calculate_connectivity(self,blocks=None,network_filter=None,append=False,dry=None):
        """
        Organizes and calls SQL scripts for calculating connectivity.

        args
        blocks -- list of block IDs to use as origins. if empty use all blocks.
        network_filter -- filter to be applied to the road network when routing
        append -- append to existing db table instead of creating a new one
        dry -- a path to save SQL statements to instead of executing in DB
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
        if not append and dry is None:
            self._connectivity_table_create(overwrite=False)
        if append and dry is None:
            if not self.table_exists(self.db_connectivity_table):
                raise ValueError("table %s not found" % self.db_connectivity_table)

        # get raw queries
        q_this_block_nodes = self.read_sql_from_file(os.path.join(self.module_dir,"sql","connectivity","calculation","35_this_block_nodes.sql"))

        block_progress = tqdm(blocks)
        failed_blocks = list()

        for block_id in block_progress:
            failure = False
            block_progress.set_description("Block id: "+str(block_id))
            subs["block_id"] = sql.Literal(block_id)

            conn = self.get_db_connection()
            cur = conn.cursor()

            # filter blocks
            self._run_sql_script("10_filter_this_block.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
            self._run_sql_script("20_assign_nodes_to_blocks.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)

            # subset hs network
            subs["max_stress"] = sql.Literal(99)
            subs["net_table"] = sql.Identifier("tmp_hs_net")
            self._run_sql_script("30_network_subset.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)

            # get hs nodes
            ret = self._run_sql("select distinct source from tmp_hs_net union select distinct target from tmp_hs_net",ret=True,dry=dry,conn=conn)
            if dry is None:
                hs_nodes = set(n[0] for n in ret)
            else:
                hs_nodes = {-1}

            # subset ls network
            subs["max_stress"] = sql.Literal(self.config.bna.connectivity.max_stress)
            subs["net_table"] = sql.Identifier("tmp_ls_net")
            self._run_sql_script("30_network_subset.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)

            # get ls nodes
            ret = self._run_sql("select distinct source from tmp_ls_net union select distinct target from tmp_ls_net",ret=True,dry=dry,conn=conn)
            if dry is None:
                ls_nodes = set(n[0] for n in ret)
            else:
                ls_nodes = {-1}

            # retrieve nodes for this block and loop through
            ret = self._run_sql_script("35_this_block_nodes.sql",subs,["sql","connectivity","calculation"],ret=True,dry=dry,conn=conn)
            if dry is not None:
                ret = set()

            if len(ret) <= 0:
                node_ids = set()
            else:
                node_ids = ret[0][0]
                if node_ids is None:
                    node_ids = set()
                else:
                    node_ids = set(node_ids)
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
                try:
                    self._run_sql_script("40_distance_table.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
                except:
                    failure = True
                    failed_blocks.append(block_id)
                    time.sleep(2)
                    continue

                try:
                    self._run_sql_script("60_cost_to_blocks.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
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
                try:
                    self._run_sql_script("40_distance_table.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
                except:
                    failure = True
                    failed_blocks.append(block_id)
                    time.sleep(2)
                    continue

                try:
                    self._run_sql_script("60_cost_to_blocks.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
                except:
                    failure = True
                    failed_blocks.append(block_id)
                    time.sleep(2)
                    continue

            # build combined cost table and write to connectivity table
            try:
                self._run_sql_script("70_combine_cost_matrices.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
            except:
                failure = True
                failed_blocks.append(block_id)
                time.sleep(2)
                continue

            cur.close()
            conn.commit()
            conn.close()

        print("\n\n------------------------------------")
        print("Process completed with {} failed units".format(len(failed_blocks)))
        if len(failed_blocks) > 0:
            print(failed_blocks)
        print("------------------------------------\n")

        if dry is None and not append:
            self._connectivity_table_create_index();
