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
        self.tiles = None
        self.tiles_pkid = None
        self.sql_subs = None
        self.default_schema = None


    def make_tiles(self,table=None,max_blocks=5000,schema=None,id=None,geom=None,overwrite=False):
        """
        Creates a new tile table using the config parameters. Automatically adjusts
        tile size so no tile contains more than max_blocks number of blocks. This
        is accomplished by starting with one tile that covers all blocks and
        recursively splitting prospective tiles into four equal parts until
        max_blocks is satisfied.

        Where applicable, defaults to whatever is given in the config file unless
        another value is explicitly passed in.

        args
        table -- the table name to use
        max_blocks -- maximum allowable number of blocks for a tile
        schema -- schema for the tiles table. if none looks for a schema in the
                    config. if none in config uses schema of the blocks.
        id -- the name to use for the identifier column (primary key)
        geom -- the name to use for the geom column
        overwrite -- whether to overwrite an existing table
        """
        # make a copy of sql substitutes
        subs = dict(self.sql_subs)

        if not table is None:
            subs["tiles_table"] = sql.Identifier(table)
        if not schema is None:
            subs["tiles_schema"] = sql.Identifier(schema)
        if not id is None:
            subs["tiles_id_col"] = sql.Identifier(id)
        if not geom is None:
            subs["tiles_geom_col"] = sql.Identifier(geom)

        conn = self.get_db_connection()
        cur = conn.cursor()

        if overwrite:
            self.drop_table(
                table=subs["tiles_table"],
                schema=subs["tiles_schema"],
                conn=conn
            )

        # create table
        cur.execute(sql.SQL(" \
            create table {tiles_schema}.{tiles_table} ( \
                {tiles_id_col} serial primary key, \
                {tiles_geom_col} geometry(polygon,{srid}) \
            ) \
        ").format(**subs))

        # get dimensions
        cur.execute(sql.SQL(" \
            SELECT \
                MIN(ST_XMin({blocks_geom_col})) AS xmin, \
                MIN(ST_YMin({blocks_geom_col})) AS ymin, \
                MAX(ST_XMax({blocks_geom_col})) AS xmax, \
                MAX(ST_YMax({blocks_geom_col})) AS ymax \
            FROM {blocks_schema}.{blocks_table} \
        ").format(**subs))

        row = cur.fetchone()
        xmin = row[0]
        ymin = row[1]
        xmax = row[2]
        ymax = row[3]

        self._split_tiles(conn,subs,max_blocks,xmin,ymin,xmax,ymax)
        conn.commit()
        cur.close()
        conn.close()


    def _split_tiles(self,conn,subs,max_blocks,xmin,ymin,xmax,ymax):
        """
        Recursive method that tests the input bounds for how many blocks are contained.
        If less than max, write the bounds to the DB as a tile. If not, split
        the tile into four equal parts and submit each as a recursion. in cases where
        a tile results in zero blocks, the tile is ignored.

        args
        conn -- database connection object from parent method
        subs -- dictionary of substitions (usually a copy of self.sql_subs)
        max_blocks -- maximum allowable number of blocks for a tile
        xmin -- minimum x bound
        ymin -- minimum y bound
        xmax -- maximum x bound
        ymax -- maximum y bound
        """
        sql_envelope = sql.SQL("st_makeenvelope({},{},{},{},{})").format(
            sql.Literal(xmin),
            sql.Literal(ymin),
            sql.Literal(xmax),
            sql.Literal(ymax),
            subs["srid"]
        )

        subs["envelope"] = sql_envelope

        # test for contained blocks
        cur = conn.cursor()
        cur.execute(sql.SQL(" \
            select count({blocks_id_col}) from {blocks_schema}.{blocks_table} blocks \
            where \
                st_intersects( \
                    blocks.{blocks_geom_col}, \
                    {envelope} \
                ) \
        ").format(**subs))
        block_count = cur.fetchone()[0]
        if max_blocks < block_count:
            cur.close()
            xmid = (xmin + xmax)/2
            ymid = (ymin + ymax)/2
            self._split_tiles(conn,table,schema,geom,max_blocks,xmin,ymin,xmid,ymid) # bottom left
            self._split_tiles(conn,table,schema,geom,max_blocks,xmid,ymin,xmax,ymid) # bottom right
            self._split_tiles(conn,table,schema,geom,max_blocks,xmin,ymid,xmid,ymax) # upper left
            self._split_tiles(conn,table,schema,geom,max_blocks,xmid,ymid,xmax,ymax) # upper right
        elif block_count == 0:
            cur.close()
        else:
            cur.execute(sql.SQL(" \
                insert into {tiles_schema}.{tiles_table} ({tiles_geom_col}) \
                select {envelope} \
            ").format(**subs))


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
