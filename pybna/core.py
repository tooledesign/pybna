###################################################################
# This is the base class for the pyBNA object and handles most of
# the objects and methods associated with it.
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
from tqdm import tqdm
from blocks import Blocks
from dbutils import DBUtils

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


    def set_destinations(self):
        """Retrieve the destinations identified in the config file and register them."""
        if self.verbose:
            print('Adding destinations')

        conn = self.get_db_connection()
        cur = conn.cursor()

        for v in self.config["bna"]["destinations"]:
            if "table" in v:
                self.destinations[v["name"]] = DestinationCategory(
                    v["name"], conn, v["table"], v["uid"], verbose=self.verbose
                )
                # add all the census blocks containing a destination from this category
                # to the pyBNA index of all blocks containing a destination of any type
                self.destination_blocks.update(
                    self.destinations[v["name"]].destination_blocks)
            if "subcats" in v:
                for sub in v["subcats"]:
                    self.destinations[sub["name"]] = Destinations(
                        sub["name"],
                        conn,
                        sub["table"],
                        sub["uid"],
                        verbose=self.verbose
                    )
                    self.destination_blocks.update(
                        self.destinations[sub["name"]].destination_blocks)


        if self.verbose:
            print("%i census blocks are part of at least one destination" %
                  len(self.destination_blocks))


    def score(self):
        """Calculate network score."""
        pass


    def travel_sheds(self,block_ids,out_table,composite=True,
                     overwrite=False,dry=None):
        """
        Creates a new DB table showing the high- and low-stress travel sheds
        for the block(s) identified by block_ids. If more than one block is
        passed to block_ids the table will have multiple travel sheds that need
        to be filtered by a user.

        args
        block_ids -- the ids to use building travel sheds
        out_table -- the table to save travel sheds to
        composite -- whether to save the output as a composite of all blocks or as individual sheds for each block
        overwrite -- whether to overwrite an existing table
        dry -- a path to save SQL statements to instead of executing in DB
        """
        conn = self.get_db_connection()

        schema, out_table = self.parse_table_name(out_table)
        if schema is None:
            schema = self.get_default_schema()

        if overwrite and dry is None:
            self.drop_table(out_table,conn=conn,schema=schema)

        # set global sql vars
        subs = dict(self.sql_subs)
        subs["table"] = sql.Identifier(out_table)
        subs["schema"] = sql.Identifier(schema)
        subs["block_ids"] = sql.Literal(block_ids)
        subs["sidx"] = sql.Identifier("sidx_" + out_table + "_geom")
        subs["idx"] = sql.Identifier(out_table + "_source_blockid")

        if composite:
            self._run_sql_script("travel_shed_composite.sql",subs,["sql"],dry=dry,conn=conn)
        else:
            self._run_sql_script("travel_shed.sql",subs,["sql"],dry=dry,conn=conn)

        conn.commit()
        conn.close()
