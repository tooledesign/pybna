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

        if not dry:
            if overwrite:
                cur.execute(sql.SQL("drop table if exists {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(output_table)
                ))
            elif self.db.table_exists(output_table,schema):
                raise psycopg2.ProgrammingError("Table %s.%s already exists" % (schema,output_table))

        # combine all the temporary tables into the final output
        columns = sql.SQL("")
        tables = sql.SQL("")
        print("Calculating high and low stress destination access")
        for cat in self.config["bna"]["destinations"]:
            cat_cols, tab_cols = self._concat_dests(conn,cat,dry)
            columns += cat_cols
            tables += tab_cols

        if "schema" in self.config["bna"]["boundary"]:
            boundary_schema = self.config["bna"]["boundary"]["schema"]
        else:
            boundary_schema = self.db.get_schema(self.config["bna"]["boundary"]["table"])

        subs = {
            "blocks_schema": sql.Identifier(self.blocks.schema),
            "blocks_table": sql.Identifier(self.blocks.table),
            "block_id_col": sql.Identifier(self.blocks.id_column),
            "block_geom": sql.Identifier(self.blocks.geom),
            "boundary_schema": sql.Identifier(boundary_schema),
            "boundary_table": sql.Identifier(self.config["bna"]["boundary"]["table"]),
            "boundary_geom": sql.Identifier(self.config["bna"]["boundary"]["geom"]),
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
            WHERE EXISTS ( \
                select 1 \
                from {boundary_schema}.{boundary_table} bound \
                where st_intersects(blocks.{block_geom},bound.{boundary_geom}) \
            ) \
        ").format(**subs)

        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Compiling destination data for all sources into output table")
            cur.execute(q)

        # now use the results to calculate scores
        print("Calculating scores")
        cases = sql.SQL("")
        for cat in self.config["bna"]["destinations"]:
            cat_case = self._concat_scores(conn,cat)
            cases += cat_case

        cases = sql.SQL(cases.as_string(conn)[1:])
        subs["cases"] = cases
        q = sql.SQL(" \
            UPDATE {schema}.{table} \
            SET {cases} \
        ").format(**subs)

        if dry:
            print(q.as_string(conn))
        else:
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
        dry -- prints sql commands instead of executing them in the db
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
                print("   ... "+node["name"])
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


    def _concat_scores(self,conn,node):
        """
        Concatenates the update logic for all fields together

        args
        conn -- psycopg2 connection object from the parent method
        node -- current node in the config file
        """
        columns = sql.SQL("")

        if "subcats" in node:
            for subcat in node["subcats"]:
                subcolumn = self._concat_scores(conn,subcat)
                columns += subcolumn
            col_name = sql.Identifier(node["name"] + "_score")
            columns += sql.SQL(",NULL::float as {}").format(col_name)

        if "blocks" in node:
            # set up destination object
            destination = DestinationCategory(self, node, os.path.join(self.module_dir,"sql","destinations"), self.verbose, self.debug)
            if self.verbose:
                print("   ... "+node["name"])
            if "breaks" not in node:
                node["breaks"] = {}
            columns += sql.SQL(",{} = ").format(sql.Identifier(destination.score_column_name))
            columns += self._concat_case(
                destination.hs_column_name,
                destination.ls_column_name,
                node["breaks"],
                node["maxpoints"]
            )

        return columns


    def _concat_case(self,hs_column,ls_column,breaks,maxpoints):
        """
        Builds a case statement for comparing high stress and low stress destination
        counts using defined break points

        args
        hs_column -- the name of the column with high stress destination counts
        ls_column -- the name of the column with low stress destination counts
        breaks -- a dictionary of break points

        returns
        a composed psycopg2 SQL object representing a full CASE ... END statement
        """
        subs = {
            "hs_column": sql.Identifier(hs_column),
            "ls_column": sql.Identifier(ls_column),
            "maxpoints": sql.Literal(maxpoints),
            "break": sql.Literal(0)
        }

        case = sql.SQL(" \
            CASE \
            WHEN COALESCE({hs_column}) = 0 THEN NULL \
            WHEN {hs_column} = {ls_column} THEN {maxpoints} \
            WHEN {ls_column} = 0 THEN 0 \
        ").format(**subs)

        cumul_score = 0
        prev_score = 0
        for b in sorted(breaks.items()):
            subs["break"] = sql.Literal(b[0])
            subs["score"] = sql.Literal(b[1])
            subs["cumul_score"] = sql.Literal(cumul_score)
            subs["prev_score"] = sql.Literal(prev_score)

            case += sql.SQL(" \
                WHEN {ls_column} = {break} THEN {score} + {cumul_score} \
                WHEN {ls_column} < {break} \
                    THEN ({ls_column}::FLOAT/{break}) * ({score} - {cumul_score}) \
            ").format(**subs)

            cumul_score += b[1]
            prev_score = b[1]

        if maxpoints == cumul_score:
            case += sql.SQL("WHEN {ls_column} > {break} THEN {maxpoints}").format(**subs)
        if maxpoints > cumul_score:
            case += sql.SQL("ELSE {maxpoints} + (({ls_column} - {break})/({hs_column} - {break})) * ({maxpoints} - {cumul_score})").format(**subs)
        case += sql.SQL(" END")

        return case


# b._concat_dests(b.config["bna"]["destinations"][1])
