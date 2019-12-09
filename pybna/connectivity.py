import os, string
import psycopg2
from psycopg2 import sql
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
            self.drop_table(self.db_connectivity_table,conn=conn)
        try:
            self._run_sql_script("create_table.sql",self.sql_subs,["connectivity"],conn=conn)
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


    def _calculate_connectivity(self,scenario_id=None,blocks=None,restrict_block_destinations=False,
                               network_filter=None,road_ids=None,append=False,
                               subtract=False,dry=None):
        """
        Organizes and calls SQL scripts for calculating connectivity.

        args
        scenario_id -- the id of the scenario for which connectivity is calculated
            (none means the scores represent the base condition)
        blocks -- list of block IDs to use as origins. if empty use all blocks.
        restrict_block_destinations -- if true, limit the destinations to the same
            list of blocks given in the blocks arg (i.e. blocks represents total
            universe of origins and destinations)
        network_filter -- filter to be applied to the road network when routing
        road_ids -- list of road_ids to be flipped to low stress (requires scenario_id)
        append -- append to existing db table instead of creating a new one
        subtract -- (requires scenario_id) if true the calculated scores for
            the project represent a subtraction of that project from the
            finished network
        dry -- a path to save SQL statements to instead of executing in DB
        """
        if blocks is None and restrict_block_destinations:
            raise ValueError("List of blocks is required for restrict_block_destinations")
        subs = dict(self.sql_subs)
        if scenario_id:
            subs["scenario_id"] = sql.Literal(scenario_id)
        else:
            subs["scenario_id"] = sql.SQL("NULL")

        if subtract:
            subs["project_subtract"] = sql.Literal(subtract)
        else:
            subs["project_subtract"] = sql.SQL("NULL")

        if network_filter is None:
            network_filter = "TRUE"
        subs["network_filter"] = sql.SQL(network_filter)

        if road_ids is None:
            subs["low_stress_road_ids"] = sql.SQL("NULL")
        else:
            subs["low_stress_road_ids"] = sql.Literal(road_ids)

        # check blocks
        if blocks is None:
            blocks = self._get_block_ids()
            subs["destination_blocks_filter"] = sql.SQL("TRUE")
        elif not type(blocks) == list and not type(blocks) == tuple:
            raise ValueError("Block IDs must be given as an iterable")
        else:
            if restrict_block_destinations:
                subs["destination_block_ids"] = sql.Literal(blocks)
                destination_id_filter = sql.SQL("blocks.{blocks_id_col} = ANY({destination_block_ids})")
                subs["destination_blocks_filter"] = destination_id_filter.format(**subs)
            else:
                subs["destination_blocks_filter"] = sql.SQL("TRUE")

        # create db table or check existence if append mode set, drop index if append
        if not append and dry is None:
            self._connectivity_table_create(overwrite=False)
        if append and dry is None:
            if not self.table_exists(self.db_connectivity_table):
                raise ValueError("table %s not found" % self.db_connectivity_table)
            self._connectivity_table_drop_index()

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
            self._run_sql_script("15_filter_other_blocks.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
            if scenario_id is not None:
                self._run_sql_script("17_remove_ls_connections_for_project.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
            self._run_sql_script("20_assign_nodes_to_blocks.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
            if road_ids is not None:
                self._run_sql_script("25_flip_low_stress.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)


            #
            # We can skip the entire high-stress routine but i need to figure
            # out how to make that work with minimal change to existing method.
            # Maybe we need two version of 70_combine_cost_matrices?
            #


            # subset hs network
            subs["max_stress"] = sql.Literal(99)
            subs["net_table"] = sql.Identifier("tmp_hs_net")
            if scenario_id is None:
                self._run_sql_script("30_network_subset.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)

                # get hs nodes
                ret = self._run_sql("select distinct source from tmp_hs_net union select distinct target from tmp_hs_net",ret=True,dry=dry,conn=conn)
                if dry is None:
                    hs_nodes = set(n[0] for n in ret)
                else:
                    hs_nodes = {-1}
            else:
                hs_nodes = set()

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

            if len(hs_node_ids) == 0 or scenario_id is not None:
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
                if scenario_id is None:
                    self._run_sql_script("80_insert.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
                else:
                    self._run_sql_script("80_insert_with_project.sql",subs,["sql","connectivity","calculation"],dry=dry,conn=conn)
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


    def calculate_project_connectivity(self,scenario_column,scenario_ids=None,
                                       datatype=None,blocks=None,
                                       network_filter=None,subtract=False,dry=None):
        """
        Wrapper for connectivity calculations on a given project, only to be
        used once the base scenario has been run.

        args
        scenario_ids -- list of projects for which connectivity is calculated
            (if none calculate for all projects)
        datatype -- the column type to use for creating the scenario column in the db
        blocks -- list of block IDs to use as origins. if empty use all blocks.
        network_filter -- filter to be applied to the road network when routing
        subtract -- if true the calculated scores for the project represent
            a subtraction of that project from the finished network
        dry -- a path to save SQL statements to instead of executing in DB
        """
        if not self.table_exists(self.db_connectivity_table):
            raise ValueError("Connectivity table {} for the base scenario not found".format(self.db_connectivity_table))
        subs = dict(self.sql_subs)

        # add column
        if datatype is None:
            datatype = self.get_column_type(self.db_connectivity_table,scenario_column)
        self._add_column(self.db_connectivity_table,"scenario",datatype)

        # add subs
        subs["roads_scenario_col"] = sql.Identifier(scenario_column)

        # iterate projects
        if scenario_ids is None:
            ret = self._run_sql(
                " \
                    select distinct {roads_scenario_col} \
                    from {roads_schema}.{roads_table} \
                    where {roads_scenario_col} is not null \
                ",
                subs=subs,
                ret=True
            )
            scenario_ids = [row[0] for row in ret]

        for scenario_id in scenario_ids:
            subs["scenario_id"] = sql.Literal(scenario_id)
            # get list of affected blocks
            if blocks is None:
                ret = self._run_sql_script("get_affected_block_ids",subs,["connectivity","scenarios"],ret=True)
                blocks = [row[0] for row in ret]

            # get list of road_ids that should be flipped to low stress
            ret = self._run_sql_script("get_affected_block_ids",subs,["connectivity","scenarios"],ret=True)
            road_ids = [row[0] for row in ret]

            # pass on to main _calculate_connectivity
            self._calculate_connectivity(
                scenario_id=scenario_id,
                blocks=blocks,
                restrict_block_destinations=True,
                network_filter=network_filter,
                road_ids=road_ids,
                append=True
            )


    def calculate_connectivity(self,blocks=None,network_filter=None,
                               append=False,dry=None):
        """
        Wrapper for connectivity calculations on the base scenario

        args
        blocks -- list of block IDs to use as origins. if empty use all blocks.
        network_filter -- filter to be applied to the road network when routing
        append -- append to existing db table instead of creating a new one
        dry -- a path to save SQL statements to instead of executing in DB
        """
        self._calculate_connectivity(
            blocks=blocks,
            network_filter=network_filter,
            append=append,
            dry=dry
        )
