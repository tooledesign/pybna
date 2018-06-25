###################################################################
# This is the class that manages destinations for the pyBNA object
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
import pandas as pd
import geopandas as gpd
import pickle
from tqdm import tqdm
import random, string

from destinationcategory import DestinationCategory


class Destinations():
    """pyBNA Destinations class"""
    config = None
    verbose = None
    debug = None
    db = None              # reference to DBUtils class
    srid = None
    blocks = None


    def score_destinations(self,output_table,schema=None,overwrite=False,dry=False):
        """
        Creates a new db table of scores for each block

        args:
        output_table -- table to create
        schema -- schema for the table. default is the schema where the census block table is stored.
        overwrite -- overwrite a pre-existing table
        dry -- print the assembled query instead of executing in the database
        """
        if schema is None:
            schema = self.blocks.schema

        conn = self.db.get_db_connection()
        cur = conn.cursor()

        if overwrite:
            cur.execute("drop table if exists {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(output_table)
            )
        # try:
        #     self._create_destination_table(conn,schema,output_table,dry=dry)
        # except:
        #     conn.rollback()

        # for destination in tqdm(self.destinations):





        # combine all the temporary tables into the final output
        columns = sql.SQL("")
        tables = sql.SQL("")
        for cat in self.config["bna"]["destinations"]:
            cat_cols, tab_cols = self._concat_dests(conn,cat,dry)
        columns += cat_cols
        tables += tab_cols

        subs = {
            "blocks_schema": sql.Identifier(self.blocks.schema),
            "blocks_table": sql.Identifier(self.blocks.table),
            "block_id_col": sql.Identifier(self.blocks.id_column),
            "schema": sql.Identifier(schema),
            "table": sql.Identifier(output_table),
            "columns": columns,
            "tables": tables
        }

        q = sql.SQL(" \
            SELECT \
                blocks.{block_id_col} \
                {columns} \
            INTO {schema}.{table} \
            FROM \
                {blocks_schema}.{blocks_table} blocks \
                {tables} \
        ").format(**subs)

        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Compiling destination data for all sources into output table")
            cur.execute(q)
            conn.commit()
        cur.close()
        conn.close()


    def _concat_dests(self,conn,node,dry=False):
        """
        Concatenates the various temporary destination result columns and tables
        together to plug into the query that creates the final score table. Operates
        recursively through the destinations listed in the config file.

        args
        conn -- psycopg2 connection object from the parent method
        node -- current node in the config file
        """
        columns = sql.SQL("")
        tables = sql.SQL("")

        if "subcats" in node:
            for subcat in node["subcats"]:
                subcolumn, subtable = self._concat_dests(conn,subcat,dry)
                columns += subcolumn
                tables += subtable
            col_name = sql.Identifier(node["name"] + "_score")
            columns += sql.SQL(",NULL::float as {}").format(col_name)

        if "blocks" in node:

            # set up destination object
            destination = DestinationCategory(self, node, os.path.join(self.module_dir,"sql","destinations"), self.verbose, self.debug)

            # set up temporary tables for results
            tbl = ''.join(random.choice(string.ascii_lowercase) for _ in range(7))
            if self.verbose:
                print("Calculating for %s" % node["name"])
            tbl_hs = tbl + "_hs"
            tbl_ls = tbl + "_ls"

            hs_subs = {
                "tmp_table": sql.Identifier(tbl_hs),
                "index": sql.Identifier("tidx_"+tbl_hs+"_block_id"),
                "connection_true": sql.Literal(True)
            }
            ls_subs = {
                "tmp_table": sql.Identifier(tbl_ls),
                "index": sql.Identifier("tidx_"+tbl_ls+"_block_id"),
                "connection_true": sql.SQL("low_stress")
            }
            hs_query = destination.query.format(**hs_subs)
            ls_query = destination.query.format(**ls_subs)

            if dry:
                print(hs_query.as_string(conn))
                print(ls_query.as_string(conn))
            else:
                try:
                    cur = conn.cursor()
                    cur.execute(hs_query)
                    cur.execute(ls_query)
                    cur.close()
                except psycopg2.Error as error:
                    conn.rollback()
                    raise error


            hs_tmptable = sql.Identifier(tbl_hs)
            ls_tmptable = sql.Identifier(tbl_ls)
            hs_col_name = sql.Identifier(node["name"] + "_hs")
            ls_col_name = sql.Identifier(node["name"] + "_ls")
            score_col_name = sql.Identifier(node["name"] + "_score")

            columns += sql.SQL(",coalesce({}.total,0)::int as {}").format(hs_tmptable,hs_col_name)
            columns += sql.SQL(",coalesce({}.total,0)::int as {}").format(ls_tmptable,ls_col_name)
            columns += sql.SQL(",NULL::float as {}").format(score_col_name)

            tables += sql.SQL(" LEFT JOIN pg_temp.{} ON blocks.{} = {}.block_id ").format(
                hs_tmptable,
                sql.Identifier(self.blocks.id_column),
                hs_tmptable
            )
            tables += sql.SQL(" LEFT JOIN pg_temp.{} ON blocks.{} = {}.block_id ").format(
                ls_tmptable,
                sql.Identifier(self.blocks.id_column),
                ls_tmptable
            )

        return columns, tables

# b._concat_dests(b.config["bna"]["destinations"][1])
