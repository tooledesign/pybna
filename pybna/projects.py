import sys, string
import psycopg2
from psycopg2 import sql

from dbutils import DBUtils


class Projects(DBUtils):
    """pyBNA Projects class"""

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

        # register pandas apply with tqdm for progress bar
        # tqdm.pandas(desc="Evaluating connectivity")


    def calculate_project(self,projects_table,project_id,projects_column="project_id"):
        """
        Calculates connectivity for a single project.

        args
        projects_table -- the table holding information about projects
        project_id -- the project id for the project being measured
        projects_column -- the name of the column holding project ids
        """
        road_ids = get_road_ids(projects_table,project_id,projects_column=projects_column)
        _calculate_project(project_id,road_ids)


    def calculate_except_project(self,projects_table,project_id,projects_column="project_id"):
        """
        Calculates connectivity for all projects _except_ the given project.
        This is a way of testing the impact of the project on the final
        network (i.e. what does connectivity look like if everything except
        this project gets built)

        args
        projects_table -- the table holding information about projects
        project_id -- the project id for the project being measured
        projects_column -- the name of the column holding project ids
        """
        road_ids = get_road_ids(projects_table,project_id,projects_column=projects_column)
        _calculate_project(project_id,road_ids)


    def _calculate_project(self,project_id,road_ids,subtract=False):
        """
        Calculates connectivity for a single project (or lack of a single
        project)

        args
        projects_table -- the table holding information about projects
        project_id -- the project id for the project being measured
        road_ids -- a list of road_ids in the base network that should be
            flipped to low stress as part of this project
        """
        # get list of affected blocks
        # call calculate_connectivity with project_id and blocks


    def get_road_ids(self,projects_table,project_id,projects_column="project_id",subtract=False):
        """
        Returns a list of road_ids associated with the given project.
        If subtract, the road_ids represent all projects _except_ the given one.

        args
        projects_table -- the table holding information about projects
        project_id -- the project_id
        projects_column -- the name of the column holding project ids
        subtract -- if true, return road_ids for all projects except those listed
        """
        projects_schema, projects_table = self.parse_table_name(projects_table)
        if projects_schema is None:
            projects_schema = self.get_schema(projects_table)

        subs = dict(self.sql_subs)
        subs["projects_schema"] = sql.Identifier(projects_schema)
        subs["projects_table"] = sql.Identifier(projects_table)
        subs["projects_col"] = sql.Identifier(projects_column)
        subs["project_id"] = sql.Literal(project_id)

        conn = self.get_db_connection()

        if subtract:
            result = self._run_sql_script("get_road_ids_subtract.sql",subs,["sql","projects"],ret=True,conn=conn)
        else:
            result = self._run_sql_script("get_road_ids.sql",subs,["sql","projects"],ret=True,conn=conn)

        return [row[0] for row in result]
