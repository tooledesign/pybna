###################################################################
# This is the base class for the pyBNA object and handles most of
# the objects and methods associated with it.
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
import pandas as pd
import geopandas as gpd
import pickle
from tqdm import tqdm
from blocks import Blocks

# from scenario import Scenario
# from destinations import Destinations


class Core():
    """pyBNA Core class"""
    config = None
    verbose = None
    debug = None
    db = None  # reference to DBUtils class
    srid = None
    blocks = None  # reference to Blocks class
    tiles = None
    tiles_pkid = None


    def get_tiles(self,table_name,geom_col,add_columns=list()):
        """
        Returns a GeoDataFrame of tiles from the database

        args:
        table_name -- the name of the tiles table
        geom_col -- the name of the geometry column
        add_columns -- a list of additional columns to include in the dataframe (the primary key is already retrieved)
        """

        print("Fetching tiles from DB")
        self.tiles_pkid = self.db.get_pkid_col(table_name)
        conn = self.db.get_db_connection()

        # handle additional columns
        cols = " "
        for c in add_columns:
            if c == self.tiles_pkid:   # we already grab the primary key column
                continue
            cols = cols + sql.SQL(",{}").format(sql.Identifier(c)).as_string(conn)

        # query
        q = sql.SQL("select {} as id, {} as pkid, {} as geom %s from {};" % cols).format(
            sql.Identifier(self.tiles_pkid),
            sql.Identifier(self.tiles_pkid),
            sql.Identifier(geom_col),
            sql.Identifier(table_name)
        )

        if self.debug:
            print(q.as_string(conn))

        return gpd.GeoDataFrame.from_postgis(
            q,
            conn,
            geom_col="geom",
            index_col="id"
        )


    def make_tiles(self,max_blocks=5000,schema=None,overwrite=False):
        """
        Creates a new tile table using the config parameters. Automatically adjusts
        tile size so no tile contains more than max_blocks number of blocks. This
        is accomplished by starting with one tile that covers all blocks and
        recursively splitting prospective tiles into four equal parts until
        max_blocks is satisfied.

        args
        max_blocks -- maximum allowable number of blocks for a tile
        schema -- schema for the tiles table. if none looks for a schema in the
                    config. if none in config uses schema of the blocks.
        overwrite -- whether to overwrite an existing table
        """
        if schema is None:
            if "schema" in self.config["bna"]["tiles"]:
                schema = self.config["bna"]["tiles"]["schema"]
            else:
                schema = self.db.get_schema(self.config["bna"]["blocks"]["table"])

        conn = self.db.get_db_connection()
        cur = conn.cursor()

        if overwrite:
            cur.execute(sql.SQL("drop table if exists {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(self.config["bna"]["tiles"]["table"])
            ))

        # create table
        cur.execute(sql.SQL(" \
            create table {}.{} ( \
                id serial primary key, \
                {} geometry(polygon,{}) \
            ) \
        ").format(
            sql.Identifier(schema),
            sql.Identifier(self.config["bna"]["tiles"]["table"]),
            sql.Identifier(self.config["bna"]["tiles"]["geom"]),
            sql.Literal(self.srid)
        ))

        # get dimensions
        cur.execute(sql.SQL(" \
            SELECT \
                MIN(ST_XMin({})) AS xmin, \
                MIN(ST_YMin({})) AS ymin, \
                MAX(ST_XMax({})) AS xmax, \
                MAX(ST_YMax({})) AS ymax \
            FROM {}.{} \
        ").format(
            sql.Identifier(self.blocks.geom),
            sql.Identifier(self.blocks.geom),
            sql.Identifier(self.blocks.geom),
            sql.Identifier(self.blocks.geom),
            sql.Identifier(self.blocks.schema),
            sql.Identifier(self.blocks.table)
        ))

        row = cur.fetchone()
        xmin = row[0]
        ymin = row[1]
        xmax = row[2]
        ymax = row[3]

        self._split_tiles(conn,schema,max_blocks,xmin,ymin,xmax,ymax)
        conn.commit()
        cur.close()
        conn.close()


    def _split_tiles(self,conn,schema,max_blocks,xmin,ymin,xmax,ymax):
        """
        Recursive method that tests the input bounds for how many blocks are contained.
        If less than max, write the bounds to the DB as a tile. If not, split
        the tile into four equal parts and submit each as a recursion. in cases where
        a tile results in zero blocks, the tile is ignored.

        args
        conn -- database connection object from parent method
        schema -- schema for the tiles table
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
            sql.Literal(self.srid)
        )

        # test for contained blocks
        cur = conn.cursor()
        cur.execute(sql.SQL(" \
            select count({}) from {}.{} blocks \
            where \
                st_intersects( \
                    blocks.{}, \
                    {} \
                ) \
        ").format(
            sql.Identifier(self.blocks.id_column),
            sql.Identifier(self.blocks.schema),
            sql.Identifier(self.blocks.table),
            sql.Identifier(self.blocks.geom),
            sql_envelope
        ))
        block_count = cur.fetchone()[0]
        if max_blocks < block_count:
            cur.close()
            xmid = (xmin + xmax)/2
            ymid = (ymin + ymax)/2
            self._split_tiles(conn,schema,max_blocks,xmin,ymin,xmid,ymid) # bottom left
            self._split_tiles(conn,schema,max_blocks,xmid,ymin,xmax,ymid) # bottom right
            self._split_tiles(conn,schema,max_blocks,xmin,ymid,xmid,ymax) # upper left
            self._split_tiles(conn,schema,max_blocks,xmid,ymid,xmax,ymax) # upper right
        elif block_count == 0:
            cur.close()
        else:
            cur.execute(sql.SQL(" \
                insert into {}.{} ({}) \
                select {} \
            ").format(
                sql.Identifier(schema),
                sql.Identifier(self.config["bna"]["tiles"]["table"]),
                sql.Identifier(self.config["bna"]["tiles"]["geom"]),
                sql_envelope
            ))


    def set_blocks(self):
        """
        Set pybna's blocks from database
        """
        blocks_table = self.config["bna"]["blocks"]["table"]
        boundary_table = self.config["bna"]["boundary"]["table"]
        boundary_geom = self.config["bna"]["boundary"]["geom"]
        pop = self.config["bna"]["blocks"]["population"]
        geom = self.config["bna"]["blocks"]["geom"]
        if "schema" in self.config["bna"]["blocks"]:
            blocks_schema = self.config["bna"]["blocks"]["schema"]
        else:
            blocks_schema = self.db.get_schema(blocks_table)
        if "id_column" in self.config["bna"]["blocks"]:
            block_id_col = self.config["bna"]["blocks"]["id_column"]
        else:
            block_id_col = get_pkid_col(blocks_table,schema=blocks_schema)

        # subs = {
        #     "block_geom": sql.Identifier(geom),
        #     "block_id": sql.Identifier(block_id_col),
        #     "pop": sql.Identifier(pop),
        #     "blocks_schema": sql.Identifier(blocks_schema),
        #     "blocks_table": sql.Identifier(blocks_table),
        #     "boundary_table": sql.Identifier(boundary_table),
        #     "boundary_geom": sql.Identifier(boundary_geom)
        # }
        #
        # if self.verbose:
        #     print("Getting census blocks from %s.%s" % (blocks_schema,blocks_table))
        #
        # conn = self.db.get_db_connection()
        # q = sql.SQL(" \
        #     select b.{block_geom} as geom, b.{block_id} as blockid, b.{pop} as pop \
        #     from {blocks_schema}.{blocks_table} b\
        #     where exists ( \
        #         select 1 from {boundary_table} bound \
        #         where st_intersects(b.{block_geom},bound.{boundary_geom}) \
        #     );"
        # ).format(**subs)
        #
        # if self.debug:
        #     print(q.as_string(conn))
        #
        self.blocks = Blocks()
        # self.blocks.blocks = gpd.GeoDataFrame.from_postgis(
        #     q,
        #     conn,
        #     geom_col=geom
        # )
        # conn.close()

        self.blocks.table = blocks_table
        self.blocks.schema = blocks_schema
        self.blocks.id_column = block_id_col
        self.blocks.id_type = self.db.get_column_type(blocks_table,block_id_col,schema=blocks_schema)
        self.blocks.geom = geom
        self.blocks.pop_column = pop



    def set_destinations(self):
        """Retrieve the destinations identified in the config file and register them."""
        if self.verbose:
            print('Adding destinations')

        conn = self.db.get_db_connection()
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


    def travel_sheds(self,block_ids,out_table,schema=None,overwrite=False,dry=False):
        """
        Creates a new DB table showing the high- and low-stress travel sheds
        for the block(s) identified by block_ids. If more than one block is
        passed to block_ids the table will have multiple travel sheds that need
        to be filtered by a user.

        args
        block_ids -- the ids to use building travel sheds
        out_table -- the table to save travel sheds to
        overwrite -- whether to overwrite an existing table
        """
        conn = self.db.get_db_connection()

        if schema is None:
            schema = self.blocks.schema

        cur = conn.cursor()

        if overwrite and not dry:
            cur.execute(sql.SQL('drop table if exists {}.{}').format(
                sql.Identifier(schema),
                sql.Identifier(out_table)
            ))

        if not dry:
            cur.execute(
                sql.SQL(
                    "create table {}.{} ( \
                        id serial primary key, \
                        geom geometry(multipolygon,{}), \
                        source_blockid text, \
                        target_blockid text, \
                        low_stress boolean, \
                        high_stress boolean \
                    )"
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(out_table),
                    sql.Literal(self.srid)
                )
            )

        # read in the raw query language
        f = open(os.path.join(self.module_dir,"sql","travel_shed.sql"))
        raw = f.read()
        f.close()

        # set global sql vars
        sidx = "sidx_" + out_table + "_geom"
        idx = "idx_" + out_table + "_source_blockid"

        for block in tqdm(block_ids):
            # compose the query
            subs = {
                "schema": sql.Identifier(schema),
                "table": sql.Identifier(out_table),
                "geom": sql.Identifier(self.config["bna"]["blocks"]["geom"]),
                "blocks_schema": sql.Identifier(self.blocks.schema),
                "blocks": sql.Identifier(self.blocks.table),
                "connectivity": sql.Identifier(self.config["bna"]["connectivity"]["table"]),
                "block_id_col": sql.Identifier(self.config["bna"]["blocks"]["id_column"]),
                "source_blockid": sql.Identifier("source_blockid10"),
                "target_blockid": sql.Identifier("target_blockid10"),
                "block_id": sql.Literal(block),
                "sidx": sql.Identifier(sidx),
                "idx": sql.Identifier(idx)
            }

            q = sql.SQL(raw).format(**subs)

            if dry:
                print(q.as_string(conn))
            else:
                cur.execute(q)

        conn.commit()
        del cur