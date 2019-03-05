###################################################################
# This is the class that manages destinations for the pyBNA object
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
import numpy as np
from tqdm import tqdm
import random, string

from dbutils import DBUtils
from destinationcategory import DestinationCategory


class Destinations(DBUtils):
    """pyBNA Destinations class"""

    def __init__(self):
        DBUtils.__init__(self,"")
        self.config = None
        self.verbose = None
        self.debug = None
        self.srid = None


    def score_destinations(self,output_table,schema=None,with_geoms=False,overwrite=False,dry=False):
        """
        Creates a new db table of scores for each block

        args:
        output_table -- table to create
        schema -- schema for the table. default is the schema where the census block table is stored.
        overwrite -- overwrite a pre-existing table
        dry -- print the assembled query instead of executing in the database
        """
        # make a copy of sql substitutes
        subs = dict(self.sql_subs)

        subs["scores_table"] = sql.Identifier(output_table)
        if schema is not None:
            subs["scores_schema"] = sql.Identifier(schema)
        else:
            subs["scores_schema"] = subs["blocks_schema"]

        conn = self.get_db_connection()
        cur = conn.cursor()

        if not dry:
            if overwrite:
                self.drop_table(
                    table=subs["scores_table"],
                    schema=subs["scores_schema"],
                    conn=conn
                )
            elif self.table_exists(output_table,subs["scores_schema"].as_string(conn)):
                raise psycopg2.ProgrammingError("Table %s.%s already exists" % (subs["scores_schema"].as_string(conn),output_table))

        # combine all the temporary tables into the final output
        columns = sql.SQL("")
        tables = sql.SQL("")
        print("Calculating high and low stress destination access")
        for cat in self.config["bna"]["destinations"]:
            cat_cols, tab_cols = self._concat_dests(conn,cat,dry)
            columns += cat_cols
            tables += tab_cols

        subs["columns"] = columns
        subs["tables"] = tables

        q = sql.SQL(" \
            SELECT \
                blocks.{blocks_id_col} \
                {columns}, \
                NULL::FLOAT AS overall_score \
            INTO {scores_schema}.{scores_table} \
            FROM \
                {blocks_schema}.{blocks_table} blocks \
                {tables} \
            WHERE EXISTS ( \
                select 1 \
                from {boundary_schema}.{boundary_table} bound \
                where st_intersects(blocks.{blocks_geom_col},bound.{boundary_geom_col}) \
            ); \
            ALTER TABLE {scores_schema}.{scores_table} ADD PRIMARY KEY ({blocks_id_col}); \
        ").format(**subs)

        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Compiling destination data for all sources into output table")
            cur.execute(q)

        # now use the results to calculate scores
        print("Calculating destination scores")
        cases = sql.SQL("")
        for cat in self.config["bna"]["destinations"]:
            cat_case = self._concat_scores(conn,cat)
            cases += cat_case

        cases = sql.SQL(cases.as_string(conn)[1:])
        subs["cases"] = cases
        q = sql.SQL(" \
            UPDATE {scores_schema}.{scores_table} \
            SET {cases} \
        ").format(**subs)

        if dry:
            print(q.as_string(conn))
        else:
            cur.execute(q)
            cur.close()

        # finally set any category scores
        print("Calculating category scores")

        # set up the overall score by copying the destinations configuration
        # and adding an "overall" category with all main categories underneath it
        overall_config = {"name": "overall"}
        overall_config["subcats"] = self.config["bna"]["destinations"]
        self._category_scores(conn,overall_config,subs,dry)

        if with_geoms:
            self._copy_block_geoms(conn,subs,dry)

        conn.commit()
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
                self.sql_subs["blocks_id_col"],
                hs_tmptable
            )
            tables += sql.SQL(" LEFT JOIN pg_temp.{} ON blocks.{} = {}.block_id ").format(
                ls_tmptable,
                self.sql_subs["blocks_id_col"],
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
                node["maxpoints"],
                node["method"]
            )

        return columns


    def _concat_case(self,hs_column,ls_column,breaks,maxpoints,method):
        """
        Builds a case statement for comparing high stress and low stress destination
        counts using defined break points

        args
        hs_column -- the name of the column with high stress destination counts
        ls_column -- the name of the column with low stress destination counts
        breaks -- a dictionary of break points
        method -- the method to use for calculation (count, percentage)

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
        prev_break = 0
        for b in sorted(breaks.items()):
            subs["break"] = sql.Literal(b[0])
            subs["prev_break"] = sql.Literal(prev_break)
            subs["score"] = sql.Literal(b[1])
            subs["cumul_score"] = sql.Literal(cumul_score)
            if method == "count":
                subs["val"] = sql.SQL("{ls_column}").format(**subs)
            elif method == "percentage":
                subs["val"] = sql.SQL("({ls_column}::FLOAT/{hs_column})").format(**subs)

            case += sql.SQL(" \
                WHEN {val} = {break} THEN {score} + {cumul_score} \
                WHEN {val} < {break} \
                    THEN {cumul_score} + (({val} - {prev_break})::FLOAT/({break} - {prev_break})) * ({score} - {cumul_score}) \
            ").format(**subs)

            prev_break = b[0]
            if method == "count":       # parks score is calculating wrong on 311090002021011
                cumul_score += b[1]
            elif method == "percentage":
                cumul_score = b[1]

        subs["cumul_score"] = sql.Literal(cumul_score)
        if np.isclose(maxpoints,cumul_score):
            case += sql.SQL("WHEN {val} > {break} THEN {maxpoints}").format(**subs)
        if maxpoints > cumul_score:
            if method == "count":
                case += sql.SQL("ELSE {cumul_score} + (({ls_column} - {break})::FLOAT/({hs_column} - {break})) * ({maxpoints} - {cumul_score})").format(**subs)
            elif method == "percentage":
                case += sql.SQL("ELSE {cumul_score} + (({ls_column}::FLOAT/{hs_column}) - {break})::FLOAT * ({maxpoints} - {cumul_score})").format(**subs)

        case += sql.SQL(" END")

        return case


    def _category_scores(self,conn,node,subs,dry=False):
        """
        Iteratively calculates category scores from all component subcategories
        using the weights defined in the config file.
        Will first calculate any subcategories which themselves have subcategories.

        args
        conn -- psycopg2 connection object from the parent method
        node -- current node in the destination tree
        subs -- list of SQL substitutions from the parent method
        dry -- outputs all SQL commands to stdout instead of executing in the DB
        """
        if "subcats" in node:
            for subcat in node["subcats"]:
                self._category_scores(conn,subcat,subs,dry)

            if self.verbose:
                print("   ... %s" % node["name"])

            if "maxpoints" not in node:
                node["maxpoints"] = self._get_maxpoints(subcat)

            num = []
            den = []
            check_zero = []
            check_null = []
            for subcat in node["subcats"]:
                if "maxpoints" not in subcat:
                    subcat["maxpoints"] = self._get_maxpoints(subcat)

                num.append(sql.SQL("{}*coalesce({},0)::float/{}").format(
                    sql.Literal(subcat["weight"]),
                    sql.Identifier(subcat["name"]+"_score"),
                    sql.Literal(subcat["maxpoints"])
                ))
                den.append(sql.SQL("case when {} is null then 0 else {} end").format(
                    sql.Identifier(subcat["name"]+"_score"),
                    sql.Literal(subcat["weight"])
                ))
                check_zero.append(sql.SQL("coalesce({},0) = 0").format(
                    sql.Identifier(subcat["name"]+"_score")
                ))
                check_null.append(sql.SQL("{} is null").format(
                    sql.Identifier(subcat["name"]+"_score")
                ))

            subs["this_column"] = sql.Identifier(node["name"] + "_score")
            subs["check_null"] = sql.SQL(" and ").join(check_null)
            subs["check_zero"] = sql.SQL(" and ").join(check_zero)
            subs["numerator"] = sql.SQL(" + ").join(num)
            subs["denominator"] = sql.SQL(" + ").join(den)
            subs["maxpoints"] = sql.Literal(node["maxpoints"])
            q = sql.SQL(" \
                update {scores_schema}.{scores_table} \
                set \
                    {this_column} = \
                        case \
                            when {check_null} then null \
                            when {check_zero} then 0 \
                            else {maxpoints} * ({numerator})::FLOAT/({denominator}) \
                            end \
            ").format(**subs)

            if dry:
                print(q.as_string(conn))
            else:
                cur = conn.cursor()
                cur.execute(q)
                cur.close()


    def _get_maxpoints(self,node):
        """
        calculates a maximum score for main categories composed of subcategories
        using the weights assigned to the subcategories.

        args
        node -- current branch of the destination tree
        """
        if "maxpoints" in node:
            return node["maxpoints"]
        elif "subcats" in node:
            for subcat in node["subcats"]:
                maxpoints += self._get_maxpoints(subcat)
        else:
            return node["weight"]


    def _copy_block_geoms(self,conn,subs,dry=False):
        """
        Copies the geometries from the block table to the output table of destination
        scores.

        args
        conn -- psycopg2 connection object from the parent method
        subs -- list of SQL substitutions from the parent method
        """
        # get geometry type from block table
        subs["type"] = sql.SQL(
            self.get_column_type(
                subs["blocks_table"].as_string(conn),
                subs["blocks_geom_col"].string,
                subs["blocks_schema"].as_string(conn)
            )
        )
        subs["sidx_name"] = sql.Identifier("sidx_")+subs["scores_table"]

        f = open(os.path.join(self.module_dir,"sql","destinations","add_geoms.sql"))
        raw = f.read()
        f.close()

        q = sql.SQL(raw).format(**subs)

        if dry:
            print(q.as_string(conn))
        else:
            cur = conn.cursor()
            cur.execute(q)
            cur.close()
