###################################################################
# This is the base class for the pyBNA object and handles most of
# the objects and methods associated with it.
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
from tqdm import tqdm
from .dbutils import DBUtils

FORWARD_DIRECTION = "forward"
BACKWARD_DIRECTION = "backward"

class Core(DBUtils):
    """pyBNA Core class"""

    def __init__(self):
        DBUtils.__init__(self,"")
        self.config = None
        self.verbose = None
        self.debug = None
        self.srid = None
        self.sql_subs = None


    def travel_sheds(self,block_ids,out_table,composite=True,scenario_id=None,
                     subtract=False,overwrite=False):
        """
        Creates a new DB table showing the high- and low-stress travel sheds
        for the block(s) identified by block_ids. If more than one block is
        passed to block_ids the table will have multiple travel sheds that need
        to be filtered by a user. If no scenario is indicated the base scenario
        is used.

        Parameters
        ----------
        block_ids : list
            the ids to use building travel sheds
        out_table : str
            the table to save travel sheds to
        composite : bool, optional
            whether to save the output as a composite of all blocks or as individual sheds for each block
        scenario_id : text, optional
            if given, the travel shed represents the given scenario. if not given,
            the base scenario is used.
        subtract : bool, optional
            if true the calculated scores for the scenario represent
            a subtraction of that scenario from all other scenarios
        overwrite : bool, optional
            whether to overwrite an existing table
        """
        conn = self.get_db_connection()

        schema, out_table = self.parse_table_name(out_table)
        if schema is None:
            schema = self.get_default_schema()

        if overwrite:
            self.drop_table(out_table,conn=conn,schema=schema)

        # set global sql vars
        subs = dict(self.sql_subs)
        subs["table"] = sql.Identifier(out_table)
        subs["schema"] = sql.Identifier(schema)
        subs["block_ids"] = sql.Literal(block_ids)
        subs["sidx"] = sql.Identifier("sidx_" + out_table + "_geom")
        subs["idx"] = sql.Identifier(out_table + "_source_blockid")
        if scenario_id:
            subs["scenario_id"] = sql.Literal(scenario_id)
        else:
            subs["scenario_id"] = sql.SQL("NULL")

        # create temporary filtered connectivity table
        if scenario_id is None:
            try:
                self.get_column_type(self.db_connectivity_table,"scenario")
                subs["scenario_where"] = sql.SQL("WHERE scenario IS NULL")
            except:
                subs["scenario_where"] = sql.SQL("")
            self._run_sql_script("01_connectivity_table.sql",subs,["sql","scenarios"],conn=conn)
        elif subtract:
            self._run_sql_script("01_connectivity_table_scenario_subtract.sql",subs,["sql","scenarios"],conn=conn)
        else:
            self._run_sql_script("01_connectivity_table_scenario.sql",subs,["sql","scenarios"],conn=conn)

        # make sheds
        if composite:
            self._run_sql_script("travel_shed_composite.sql",subs,["sql"],conn=conn)
        else:
            self._run_sql_script("travel_shed.sql",subs,["sql"],conn=conn)

        conn.commit()
        conn.close()


    def export(self,fpath):
        """
        Exports BNA tables to a geopackage. Overwrites any pre-existing tables
        so use with caution!

        Parameters
        ----------
        fpath : text
            the path to the geopackage file
        """
        base, ext = os.path.splitext(fpath)
        if not ext == ".gpkg":
            raise ValueError("Output file should be a geopackage (.gpkg)")

        # check for tables
        for t in [
                    self.config.bna.boundary,
                    self.config.bna.blocks,
                    self.config.bna.network.roads,
                    self.config.bna.connectivity
                 ]:
            if not self.table_exists(t.table):
                raise ValueError("No table at {}".format(t.table))
        for d in self.destinations:
            if "table" in d:
                if not self.table_exists(d.table):
                    raise ValueError("No table at {}".format(t.table))

        # export
        for t in [
                    self.config.bna.boundary,
                    self.config.bna.blocks,
                    self.config.bna.network.roads
                 ]:
            schema, table = self.parse_table_name(t.table)
            if "geom" in t:
                self.export_table(t.table,fpath,geom=t.geom)
            else:
                self.export_table(t.table,fpath)

        for d in self.destinations:
            if "table" in d:
                if "geom" in d:
                    self.export_table(d.table,fpath,geom=t.geom)
                else:
                    self.export_table(d.table,fpath)

        self.export_table(self.config.bna.connectivity.table,fpath,nonspatial=True)
