import os
import psycopg2
from psycopg2 import sql

from dbutils import DBUtils


class Zones(DBUtils):
    """pyBNA Destinations class"""

    def __init__(self):
        DBUtils.__init__(self,"")

        # these are vars that come from other classes
        self.config = None
        self.verbose = None
        self.debug = None
        self.module_dir = None
        self.sql_subs = None
        self.default_schema = None


    def make_zones(self,table,schema=None,uid="id",geom="geom",roads_filter=None,dry=False):
        """
        Creates analysis zones that aggregate blocks into logical groupings
        based on islands of 100% low stress connectivity

        args
        table -- table name
        schema -- schema name
        uid -- uid column name
        geom -- geom column name
        roads_filter -- SQL filter applied to the roads table (used e.g. to make
            sure zones don't span arterial roads)
        ints_filter -- SQL filter applied to the intersections table (used e.g.
            to make sure zones don't encompass 6-leg intersections)
        """
        print("Grouping blocks into zones")

        if schema is None:
            schema = self.default_schema

        if roads_filter is None:
            roads_filter = "TRUE"

        # build subs
        subs = dict(self.sql_subs)
        subs["roads_filter"] = sql.SQL(roads_filter)
        subs["zones_table"] = sql.Identifier(table)
        subs["zones_schema"] = sql.Identifier(schema)
        subs["zones_id_col"] = sql.Identifier(uid)
        subs["zones_geom_col"] = sql.Identifier(geom)
        subs["zones_index"] = sql.Identifier("sidx_" + table)

        # read in the raw queries
        query_01 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","agg_blocks","01_create_prelim_zones.sql"))
        query_02 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","agg_blocks","02_remove_bad_zones.sql"))
        query_03 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","agg_blocks","03_aggregate_blocks.sql"))
        query_04 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","agg_blocks","04_unnest.sql"))
        query_05 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","agg_blocks","05_set_nodes_closest_to_center.sql"))
        query_06 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","agg_blocks","06_set_nodes_furthest_apart.sql"))
        query_07 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","agg_blocks","07_clean_up.sql"))

        conn = self.get_db_connection()
        cur = conn.cursor()
        cur2 = conn.cursor()

        # create preliminary zones
        q = sql.SQL(query_01).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Making preliminary zones")
            cur.execute(q)

        # remove bad zones
        q = sql.SQL(query_02).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Removing bad prelim zones")
            cur.execute(q)

        # aggregate blocks
        q = sql.SQL(query_03).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Aggregating blocks")
            cur.execute(q)

        # unnest arrays for faster node matching
        q = sql.SQL(query_04).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Preparing for node matching")
            cur.execute(q)

        # set nodes
        if self.verbose:
            print("Setting network nodes on zones")

        q = sql.SQL(query_05).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            cur.execute(q)

        for i in [1,3,5,7,11]:
            # iterate the script that distributes nodes throughout
            # the zone, adding more nodes to zones with higher
            # numbers of constituent blocks
            subs["num_blocks"] = sql.Literal(i)
            q = sql.SQL(query_06).format(**subs)
            if dry:
                print(q.as_string(conn))
            else:
                cur.execute(q)

        # clean up
        q = sql.SQL(query_07).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Cleaning up")
            cur.execute(q)

        conn.commit()
        cur.close()
        conn.close()
