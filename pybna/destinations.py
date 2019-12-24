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
        self.db_connectivity_table = None
        self.destinations = None


    def register_destinations(self,category=None,workspace_schema=None,destinations=None):
        """
        Retrieve the destinations identified in the config file and register them.

        args
        category -- a destination category to register. None -> re-register all destinations
        workspace_schema -- schema to save interim working tables to
        destinations -- a list of destinations (if none, use the config file)
        """
        if category is None and destinations is None:
            if self.verbose:
                print('Adding destinations')
            self.destinations = dict()

        if destinations is None:
            destinations = self.config.bna.destinations

        for v in destinations:
            config = self.parse_config(v)
            if "table" in config:
                if category is None or config.name == category:
                    self.destinations[config.name] = DestinationCategory(
                        config,
                        self.db_connection_string,
                        workspace_schema=workspace_schema
                    )

            if "subcats" in config:
                self.register_destinations(
                    category=category,
                    workspace_schema=workspace_schema,
                    destinations=config.subcats
                )


    def score_destinations(self,output_table,scenario_id=None,subtract=False,with_geoms=False,overwrite=False,dry=None):
        """
        Creates a new db table of scores for each block

        args:
        output_table -- table to create (optionally schema-qualified)
        scenario_id -- the id of the scenario for which scores are calculated
            (none means the scores represent the base condition)
        subtract -- if true the calculated scores for the scenario represent
            a subtraction of that scenario from all other scenarios
        overwrite -- overwrite a pre-existing table
        dry -- a path to save SQL statements to instead of executing in DB
        """
        # make a copy of sql substitutes
        subs = dict(self.sql_subs)

        # check if a scenarios column exists
        if scenario_id is None:
            try:
                self.get_column_type(self.db_connectivity_table,"scenario")
                subs["scenario_where"] = sql.SQL("WHERE scenario IS NULL")
            except:
                subs["scenario_where"] = sql.SQL("")
        else:
            subs["scenario_id"] = sql.Literal(scenario_id)

        schema, output_table = self.parse_table_name(output_table)

        subs["scores_table"] = sql.Identifier(output_table)
        if schema is None:
            schema = self.get_default_schema()
        subs["scores_schema"] = sql.Identifier(schema)

        conn = self.get_db_connection()
        cur = conn.cursor()

        if dry is None:
            if overwrite:
                self.drop_table(
                    table=output_table,
                    schema=schema,
                    conn=conn
                )
            elif self.table_exists(output_table,subs["scores_schema"].as_string(conn)):
                raise psycopg2.ProgrammingError("Table {}.{} already exists".format(subs["scores_schema"].as_string(conn),output_table))

        # create temporary filtered connectivity table
        if scenario_id is None:
            self._run_sql_script("01_connectivity_table.sql",subs,["sql","destinations"],conn=conn)
        elif subtract:
            self._run_sql_script("01_connectivity_table_scenario_subtract.sql",subs,["sql","destinations"],conn=conn)
        else:
            self._run_sql_script("01_connectivity_table_scenario.sql",subs,["sql","destinations"],conn=conn)

        # generate high and low stress counts for all categories
        for name, destination in self.destinations.iteritems():
            destination.count_connections(subs,conn=conn)

        # combine all the temporary tables into the final output
        columns = sql.SQL("")
        tables = sql.SQL("")
        for name, destination in self.destinations.iteritems():
            columns += sql.SQL(",coalesce({}.total,0)::int as {}").format(
                sql.Identifier(destination.high_stress_name),
                sql.Identifier(destination.high_stress_name)
            )
            columns += sql.SQL(",coalesce({}.total,0)::int as {}").format(
                sql.Identifier(destination.low_stress_name),
                sql.Identifier(destination.low_stress_name)
            )
            columns += sql.SQL(",NULL::float as {}").format(
                sql.Identifier(destination.config.name + "_score")
            )
            tables += sql.SQL(" LEFT JOIN {}.{} ON blocks.{} = {}.block_id ").format(
                sql.Identifier(destination.workspace_schema),
                sql.Identifier(destination.high_stress_name),
                self.sql_subs["blocks_id_col"],
                sql.Identifier(destination.high_stress_name)
            )
            tables += sql.SQL(" LEFT JOIN {}.{} ON blocks.{} = {}.block_id ").format(
                sql.Identifier(destination.workspace_schema),
                sql.Identifier(destination.low_stress_name),
                self.sql_subs["blocks_id_col"],
                sql.Identifier(destination.low_stress_name)
            )

        subs["columns"] = columns
        subs["tables"] = tables

        print("Compiling destination data for all sources into output table")
        self._run_sql_script("03_all_combined.sql",subs,["sql","destinations"],dry=dry,conn=conn)

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

        self._run_sql(q.as_string(conn),dry=dry,conn=conn)

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


    # def _concat_dests(self,conn,node,dry=None):
    #     """
    #     Concatenates the various temporary destination result columns and tables
    #     together to plug into the query that creates the final score table. Operates
    #     recursively through the destinations listed in the config file.
    #
    #     args
    #     conn -- psycopg2 connection object from the parent method
    #     node -- current node in the config file
    #     dry -- a path to save SQL statements to instead of executing in DB
    #     """
    #     columns = sql.SQL("")
    #     tables = sql.SQL("")
    #
    #     if "subcats" in node:
    #         for subcat in node["subcats"]:
    #             subcolumn, subtable = self._concat_dests(conn,subcat,dry)
    #             columns += subcolumn
    #             tables += subtable
    #         col_name = sql.Identifier(node["name"] + "_score")
    #         columns += sql.SQL(",NULL::float as {}").format(col_name)
    #
    #     if "table" in node:
    #
    #         # set up destination object
    #         destination = DestinationCategory(node, os.path.join(self.module_dir,"sql","destinations"), self.sql_subs, self.db_connection_string)
    #
    #         # set up temporary tables for results
    #         tbl = ''.join(random.choice(string.ascii_lowercase) for _ in range(7))
    #         if self.verbose:
    #             print("   ... "+node["name"])
    #         tbl_hs = tbl + "_hs"
    #         tbl_ls = tbl + "_ls"
    #
    #         hs_subs = {
    #             "tmp_table": sql.Identifier(tbl_hs),
    #             "index": sql.Identifier("tidx_"+tbl_hs+"_block_id"),
    #             "connection_true": sql.Literal(True)
    #         }
    #         ls_subs = {
    #             "tmp_table": sql.Identifier(tbl_ls),
    #             "index": sql.Identifier("tidx_"+tbl_ls+"_block_id"),
    #             "connection_true": sql.SQL("low_stress")
    #         }
    #         hs_query = destination.query.format(**hs_subs)
    #         ls_query = destination.query.format(**ls_subs)
    #
    #         self._run_sql(hs_query.as_string(conn),dry=dry,conn=conn)
    #         self._run_sql(ls_query.as_string(conn),dry=dry,conn=conn)
    #
    #         hs_tmptable = sql.Identifier(tbl_hs)
    #         ls_tmptable = sql.Identifier(tbl_ls)
    #         hs_col_name = sql.Identifier(node["name"] + "_hs")
    #         ls_col_name = sql.Identifier(node["name"] + "_ls")
    #         score_col_name = sql.Identifier(node["name"] + "_score")
    #
    #         columns += sql.SQL(",coalesce({}.total,0)::int as {}").format(hs_tmptable,hs_col_name)
    #         columns += sql.SQL(",coalesce({}.total,0)::int as {}").format(ls_tmptable,ls_col_name)
    #         columns += sql.SQL(",NULL::float as {}").format(score_col_name)
    #
    #         tables += sql.SQL(" LEFT JOIN pg_temp.{} ON blocks.{} = {}.block_id ").format(
    #             hs_tmptable,
    #             self.sql_subs["blocks_id_col"],
    #             hs_tmptable
    #         )
    #         tables += sql.SQL(" LEFT JOIN pg_temp.{} ON blocks.{} = {}.block_id ").format(
    #             ls_tmptable,
    #             self.sql_subs["blocks_id_col"],
    #             ls_tmptable
    #         )
    #
    #     return columns, tables


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

        if "table" in node:
            # set up destination object
            destination = DestinationCategory(node, os.path.join(self.module_dir,"sql","destinations"), self.sql_subs, self.db_connection_string)
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
                case += sql.SQL(" \
                    WHEN {val} = {break} THEN {score} + {cumul_score} \
                    WHEN {val} < {break} \
                        THEN {cumul_score} + (({val} - {prev_break})::FLOAT/({break} - {prev_break})) * ({score} - {cumul_score}) \
                ").format(**subs)
            elif method == "percentage":
                subs["val"] = sql.SQL("({ls_column}::FLOAT/{hs_column})").format(**subs)
                case += sql.SQL(" \
                    WHEN {val} = {break} THEN {score} \
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


    def _category_scores(self,conn,node,subs,dry=None):
        """
        Iteratively calculates category scores from all component subcategories
        using the weights defined in the config file.
        Will first calculate any subcategories which themselves have subcategories.

        args
        conn -- psycopg2 connection object from the parent method
        node -- current node in the destination tree
        subs -- list of SQL substitutions from the parent method
        dry -- a path to save SQL statements to instead of executing in DB
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

            self._run_sql(q.as_string(conn),dry=dry,conn=conn)


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


    def _copy_block_geoms(self,conn,subs,dry=None):
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

        self._run_sql_script("04_add_geoms.sql",subs,["sql","destinations"],dry=dry,conn=conn)
