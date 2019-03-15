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

# from scenario import Scenario
# from destinations import Destinations


class Core(DBUtils):
    """pyBNA Core class"""

    def __init__(self):
        DBUtils.__init__(self,"")
        self.config = None
        self.verbose = None
        self.debug = None
        self.srid = None
        self.sql_subs = None
        self.default_schema = None


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


    def travel_sheds(self,block_ids,out_table,schema=None,composite=True,
                     overwrite=False,dry=False):
        """
        Creates a new DB table showing the high- and low-stress travel sheds
        for the block(s) identified by block_ids. If more than one block is
        passed to block_ids the table will have multiple travel sheds that need
        to be filtered by a user.

        args
        block_ids -- the ids to use building travel sheds
        out_table -- the table to save travel sheds to
        schema -- the db schema to save the table to (defaults to where census blocks are stored)
        composite -- whether to save the output as a composite of all blocks or as individual sheds for each block
        overwrite -- whether to overwrite an existing table
        """
        conn = self.get_db_connection()

        if schema is None:
            schema = self.default_schema


        if overwrite and not dry:
            self.drop_table(out_table,conn=conn,schema=schema)

        # read in the raw query language
        if composite:
            query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","travel_shed_composite.sql"))
        else:
            query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","travel_shed.sql"))

        # set global sql vars
        subs = dict(self.sql_subs)
        subs["table"] = sql.Identifier(out_table)
        subs["schema"] = sql.Identifier(schema)
        subs["block_ids"] = sql.Literal(block_ids)
        subs["sidx"] = sql.Identifier("sidx_" + out_table + "_geom")
        subs["idx"] = sql.Identifier(out_table + "_source_blockid")

        q = sql.SQL(query).format(**subs)

        if dry:
            print(q.as_string(conn))
        else:
            cur = conn.cursor()
            cur.execute(q)
            cur.close()

        conn.commit()
        conn.close()
