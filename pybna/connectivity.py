import os, string, warnings
import psycopg2
from psycopg2 import sql
from tqdm import tqdm
import time

from .dbutils import DBUtils


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


    def build_network(self,skip_check=False,throw_error=False):
        """
        Builds the network in the DB using details from the BNA config file.

        Parameters
        ----------
        skip_check : bool, optional
            skips check for network issues
        throw_error : bool, optional
            throws an error rather than raising a warning if checks uncover issues
        """
        if self.verbose:
            print("Building network in database")

        # run checks
        if not skip_check:
            print("Checking for network issues")
            self.check_road_features(throw_error)

        # set up substitutions
        subs = dict(self.sql_subs)
        subs["nodes_index"] = sql.Identifier("sidx_"+self.config.bna.network.nodes.table)
        subs["edges_index"] = sql.Identifier("sidx_"+self.config.bna.network.edges.table)

        # run scripts
        conn = self.get_db_connection()
        print("Creating network tables")
        self._run_sql_script("create_tables.sql",subs,["sql","build_network"],conn=conn)
        print("Adding network nodes")
        self._run_sql_script("insert_nodes.sql",subs,["sql","build_network"],conn=conn)
        print("Adding network edges")
        self._run_sql_script("insert_edges.sql",subs,["sql","build_network"],conn=conn)
        print("Finishing up network")
        self._run_sql_script("cleanup.sql",subs,["sql","build_network"],conn=conn)

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
                print("Checking for {} in database".format(table))
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
            sql.SQL(" \
                select {blocks_id_col} from {blocks_schema}.{blocks_table} blocks \
                where exists ( \
                    select 1 from {boundary_schema}.{boundary_table} bound \
                    where st_dwithin(bound.{boundary_geom_col},blocks.{blocks_geom_col},{connectivity_max_distance}) \
                ) \
            ").format(**self.sql_subs)
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
            self._run_sql_script("create_table.sql",self.sql_subs,["sql","connectivity"],conn=conn)
        except psycopg2.ProgrammingError:
            if conn.closed == 0:
                conn.rollback()
                conn.close()
            raise ValueError("Table %s already exists" % self.config.bna.connectivity.table)
        if conn.closed == 0:
            if not cur.closed:
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
        cur.execute(sql.SQL("\
            SELECT indexrelid::regclass::text \
            FROM   pg_index  i \
                LEFT   JOIN pg_depend d ON d.objid = i.indexrelid \
                AND d.deptype = 'i' \
            WHERE  i.indrelid = {}::regclass \
            AND    d.objid IS NULL \
        ").format(sql.Literal(self.config.bna.connectivity.table)))
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


    def _calculate_connectivity(self,scenario_id=None,origin_blocks=None,
                                destination_blocks=None,network_filter=None,
                                road_ids=None,append=False,subtract=False,
                                dry=None):
        """
        Organizes and calls SQL scripts for calculating connectivity.

        Parameters
        ----------
        scenario_id
            the id of the scenario for which connectivity is calculated
            (none means the scores represent the base condition)
        origin_blocks : list, optional
            list of block IDs to use as origins. if empty use all blocks.
        destination_blocks : list, optional
            list of block IDs to use as destinations. if empty use all blocks.
        network_filter : str, optional
            filter to be applied to the road network when routing
        road_ids : list, optional
            list of road_ids to be flipped to low stress (requires scenario_id)
        append : bool
            append to existing db table instead of creating a new one
        subtract : bool
            (requires scenario_id) if true the calculated scores for
            the scenario are flagged as a subtraction of that scenario from the
            finished network
        dry : str
            a path to save SQL statements to instead of executing in DB
        """
        if scenario_id is None and subtract:
            raise ValueError("Subtract flag can only be used with a scenario")
        subs = dict(self.sql_subs)
        if scenario_id:
            subs["scenario_id"] = sql.Literal(scenario_id)
        else:
            subs["scenario_id"] = sql.SQL("NULL")

        if subtract:
            subs["scenario_subtract"] = sql.Literal(subtract)
        else:
            subs["scenario_subtract"] = sql.SQL("NULL")

        if network_filter is None:
            network_filter = "TRUE"
        subs["network_filter"] = sql.SQL(network_filter)

        if road_ids is None:
            subs["low_stress_road_ids"] = sql.SQL("NULL")
        else:
            subs["low_stress_road_ids"] = sql.Literal(road_ids)

        # check blocks
        if origin_blocks is None:
            origin_blocks = self._get_block_ids()
        elif not hasattr(origin_blocks,"__iter__"):
            raise ValueError("Origin block IDs must be given as an iterable")
        if destination_blocks is None:
            subs["destination_blocks_filter"] = sql.SQL("TRUE")
        elif not hasattr(destination_blocks,"__iter__"):
            raise ValueError("Destination block IDs must be given as an iterable")
        else:
            subs["destination_block_ids"] = sql.Literal(destination_blocks)
            destination_id_filter = sql.SQL("blocks.{blocks_id_col} = ANY({destination_block_ids})")
            subs["destination_blocks_filter"] = destination_id_filter.format(**subs)

        # create db table or check existence if append mode set, drop index if append
        if not append and dry is None:
            self._connectivity_table_create(overwrite=False)
        if append and dry is None:
            if not self.table_exists(self.db_connectivity_table):
                raise ValueError("table %s not found" % self.db_connectivity_table)
            self._connectivity_table_drop_index()

        block_progress = tqdm(origin_blocks,smoothing=0.1)
        failed_blocks = list()

        for block_id in block_progress:
            failure = False
            block_progress.set_description("Block id: "+str(block_id))
            subs["block_id"] = sql.Literal(block_id)

            conn = self.get_db_connection()
            cur = conn.cursor()

            # filter blocks
            self._run_sql_script("10_filter_this_block.sql",subs,["sql","connectivity","calculation"],conn=conn)
            self._run_sql_script("15_filter_other_blocks.sql",subs,["sql","connectivity","calculation"],conn=conn)
            if scenario_id is not None:
                self._run_sql_script("17_remove_ls_connections_for_scenario.sql",subs,["sql","connectivity","calculation"],conn=conn)
            self._run_sql_script("20_assign_nodes_to_blocks.sql",subs,["sql","connectivity","calculation"],conn=conn)
            self._run_sql_script("25_flip_low_stress.sql",subs,["sql","connectivity","calculation"],conn=conn)

            # subset hs network
            subs["max_stress"] = sql.Literal(99)
            subs["net_table"] = sql.Identifier("tmp_hs_net")
            if scenario_id is None:
                self._run_sql_script("30_network_subset.sql",subs,["sql","connectivity","calculation"],conn=conn)

                # get hs nodes
                ret = self._run_sql("select distinct source from tmp_hs_net union select distinct target from tmp_hs_net",ret=True,conn=conn)
                if dry is None:
                    hs_nodes = set(n[0] for n in ret)
                else:
                    hs_nodes = {-1}
            else:
                hs_nodes = set()

            # subset ls network
            subs["max_stress"] = sql.Literal(self.config.bna.connectivity.max_stress)
            subs["net_table"] = sql.Identifier("tmp_ls_net")
            self._run_sql_script("30_network_subset.sql",subs,["sql","connectivity","calculation"],conn=conn)

            # get ls nodes
            ret = self._run_sql("select distinct source from tmp_ls_net union select distinct target from tmp_ls_net",ret=True,conn=conn)
            if dry is None:
                ls_nodes = set(n[0] for n in ret)
            else:
                ls_nodes = {-1}

            # retrieve nodes for this block and loop through
            ret = self._run_sql_script("35_this_block_nodes.sql",subs,["sql","connectivity","calculation"],ret=True,conn=conn)
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
                    self._run_sql_script("40_distance_table.sql",subs,["sql","connectivity","calculation"],conn=conn)
                except:
                    failure = True
                    failed_blocks.append(block_id)
                    time.sleep(2)
                    continue

                try:
                    self._run_sql_script("60_cost_to_blocks.sql",subs,["sql","connectivity","calculation"],conn=conn)
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
                    self._run_sql_script("40_distance_table.sql",subs,["sql","connectivity","calculation"],conn=conn)
                except:
                    failure = True
                    failed_blocks.append(block_id)
                    time.sleep(2)
                    continue

                try:
                    self._run_sql_script("60_cost_to_blocks.sql",subs,["sql","connectivity","calculation"],conn=conn)
                except:
                    failure = True
                    failed_blocks.append(block_id)
                    time.sleep(2)
                    continue

            # build combined cost table and write to connectivity table
            try:
                self._run_sql_script("70_combine_cost_matrices.sql",subs,["sql","connectivity","calculation"],conn=conn)
                if scenario_id is None:
                    self._run_sql_script("80_insert.sql",subs,["sql","connectivity","calculation"],conn=conn)
                else:
                    self._run_sql_script("80_insert_with_scenario.sql",subs,["sql","connectivity","calculation"],conn=conn)
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


    def drop_scenario(self,scenario_ids=None,conn=None):
        """
        Removes the scenario(s) from the connectivity table. If no scenario_id
        is given, remove all scenarios.

        Parameters
        ----------
        scenario_ids : list, optional
            list of scenarios to delete
            (if none calculate for all scenarios)
        conn : psycopg2 connection object
            a DB connection
        """
        close_conn = False
        if conn is None:
            close_conn = True
            conn = self.get_db_connection()

        subs = dict(self.sql_subs)
        if scenario_ids is None:
            self._run_sql(
                """
                    delete from {connectivity_schema}.{connectivity_table}
                    where scenario is not null
                """,
                subs=subs,
                conn=conn
            )
        else:
            if not hasattr(scenario_ids,"__iter__"):
                scenario_ids = [scenario_ids]

            # iterate scenarios
            for scenario_id in scenario_ids:
                subs["scenario_id"] = sql.Literal(scenario_id)
                self._run_sql(
                    """
                        delete from {connectivity_schema}.{connectivity_table}
                        where scenario = {scenario_id}
                    """,
                    subs=subs,
                    conn=conn
                )

        if close_conn:
            conn.commit()
            conn.close()


    def calculate_scenario_connectivity(self,scenario_column,scenario_ids=None,
                                        datatype=None,origin_blocks=None,
                                        destination_blocks=None,network_filter=None,
                                        subtract=False,dry=None):
        """
        Wrapper for connectivity calculations on a given scenario, only to be
        used once the base scenario has been run.

        Parameters
        ----------
        scenario_column : str
            the column in the roads table indicating a scenario
        scenario_ids : list, optional
            list of scenario for which connectivity is calculated
            (if none calculate for all scenarios)
        datatype : str, optional
            the column type to use for creating the scenario column in the db
        origin_blocks : list, optional
            list of block IDs to use as origins. if empty use all blocks.
        destination_blocks : list, optional
            list of block IDs to use as destinations. if empty use all blocks.
        network_filter : str, optional
            filter to be applied to the road network when routing
        subtract : bool, optional
            if true the calculated scores for the scenario represent
            a subtraction of that scenario from all other scenarios
        dry : str, optional
            a path to save SQL statements to instead of executing in DB
        """
        if not self.table_exists(self.db_connectivity_table):
            raise ValueError("Connectivity table {} for the base scenario not found".format(self.db_connectivity_table))
        subs = dict(self.sql_subs)

        # add column
        if datatype is None:
            datatype = self.get_column_type(self.config.bna.network.roads.table,scenario_column)
        self._add_column(self.db_connectivity_table,"scenario",datatype)
        self._add_column(self.db_connectivity_table,"subtract","boolean")

        # check if scenario already exists in table


        # add subs
        subs["roads_scenario_col"] = sql.Identifier(scenario_column)

        # iterate scenarios
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
        if not hasattr(scenario_ids,"__iter__"):
            scenario_ids = [scenario_ids]

        for scenario_id in scenario_ids:
            subs["scenario_id"] = sql.Literal(scenario_id)
            conn = self.get_db_connection()

            # get list of affected blocks
            if origin_blocks is None or destination_blocks is None:
                ret = self._run_sql_script("get_affected_block_ids.sql",subs,["sql","connectivity","scenarios"],ret=True,conn=conn)
                affected_blocks = [row[0] for row in ret]
                if origin_blocks is None:
                    origin_blocks = affected_blocks
                if destination_blocks is None:
                    destination_blocks = affected_blocks

            # get list of road_ids that should be flipped to low stress
            if subtract:
                ret = self._run_sql_script("get_affected_road_ids_subtract.sql",subs,["sql","connectivity","scenarios"],ret=True,conn=conn)
            else:
                ret = self._run_sql_script("get_affected_road_ids.sql",subs,["sql","connectivity","scenarios"],ret=True,conn=conn)
            road_ids = [row[0] for row in ret]

            conn.close()

            self.drop_scenario([scenario_id])

            # pass on to main _calculate_connectivity
            self._calculate_connectivity(
                scenario_id=scenario_id,
                origin_blocks=origin_blocks,
                destination_blocks=destination_blocks,
                network_filter=network_filter,
                road_ids=road_ids,
                append=True
            )


    def calculate_connectivity(self,blocks=None,network_filter=None,
                               append=False,dry=None):
        """
        Wrapper for connectivity calculations on the base scenario

        Parameters
        ----------
        blocks : list, optional
            list of block IDs to use as origins. if empty use all blocks.
        network_filter : str, optional
            filter to be applied to the road network when routing
        append : bool, optional
            append to existing db table instead of creating a new one
        dry : str, optional
            a path to save SQL statements to instead of executing in DB
        """
        self._calculate_connectivity(
            origin_blocks=blocks,
            network_filter=network_filter,
            append=append
        )


    def check_road_features(self,throw_error=False):
        """
        Checks road features for the following issues:
            1) Empty geometries
            2) Extremely small geometries
            3) Source/target values not in intersections table
            4) Empty source/target values
            5) unusually high number of intersection legs (shared from/to values)

        Parameters
        ----------
        throw_error : bool, optional
            throws an error rather than raising a warning
        """
        conn = self.get_db_connection()

        # null geoms
        cur = self._run_sql(
            """
                SELECT COUNT(*)
                FROM {roads_schema}.{roads_table}
                WHERE {roads_geom_col} IS NULL
            """,
            subs=self.sql_subs,
            ret=True,
            conn=conn
        )
        if cur[0][0] > 0:
            if throw_error:
                raise ValueError("Null geometries found in roads table")
            else:
                warnings.warn("Null geometries found in roads table")

        # short geoms
        cur = self._run_sql(
            """
                SELECT COUNT(*)
                FROM {roads_schema}.{roads_table}
                WHERE ST_Length({roads_geom_col}) <= 1
            """,
            subs=self.sql_subs,
            ret=True,
            conn=conn
        )
        if cur[0][0] > 0:
            if throw_error:
                raise ValueError("Extremely short roads found in roads table")
            else:
                warnings.warn("Extremely short roads found in roads table")

        # source/target not in ints table
        cur = self._run_sql(
            """
                SELECT COUNT(*)
                FROM {roads_schema}.{roads_table} r
                WHERE
                    (
                        {roads_source_col} IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1
                            FROM {ints_schema}.{ints_table} i
                            WHERE i.{ints_id_col} = r.{roads_source_col}
                        )
                    )
                    OR
                    (
                        {roads_target_col} IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1
                            FROM {ints_schema}.{ints_table} i
                            WHERE i.{ints_id_col} = r.{roads_target_col}
                        )
                    )
            """,
            subs=self.sql_subs,
            ret=True,
            conn=conn
        )
        if cur[0][0] > 0:
            if throw_error:
                raise ValueError("Source/target value not found in intersection table")
            else:
                warnings.warn("Source/target value not found in intersection table")

        # empty source/target
        cur = self._run_sql(
            """
                SELECT COUNT(*)
                FROM {roads_schema}.{roads_table}
                WHERE
                    {roads_source_col} IS NULL
                    OR {roads_target_col} IS NULL
            """,
            subs=self.sql_subs,
            ret=True,
            conn=conn
        )
        if cur[0][0] > 0:
            if throw_error:
                raise ValueError("Empty source or target value found")
            else:
                warnings.warn("Empty source or target value found")

        # high number of legs
        cur = self._run_sql(
            """
                SELECT i.{ints_id_col}
                FROM
                    {ints_schema}.{ints_table} i,
                    {roads_schema}.{roads_table} r
                WHERE
                    i.{ints_id_col} IN (r.{roads_source_col},r.{roads_target_col})
                GROUP BY i.{ints_id_col}
                HAVING COUNT(*) > 8
            """,
            subs=self.sql_subs,
            ret=True,
            conn=conn
        )
        if len(cur) > 0:
            if throw_error:
                raise ValueError("Unusually high number of intersection legs found")
            else:
                warnings.warn("Unusually high number of intersection legs found")
