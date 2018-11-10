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


    def make_zones_from_network(self,table=None,schema=None,uid=None,geom=None,
                                roads_filter=None,overwrite=False,dry=False):
        """
        Creates analysis zones that aggregate blocks into logical groupings
        based on islands in the road network formed by high stress links and
        other road attributes as given by the roads_filter.

        args
        table -- table name (default: table name given in config)
        schema -- schema name (default: schema name given in config)
        uid -- uid column name (default: uid in config, or "id" if not in config)
        geom -- geom column name (default: geom in config, or "geom" if not in config)
        roads_filter -- SQL filter applied to the roads table (used e.g. to make
            sure zones don't span arterial roads)
        overwrite -- overwrite an existing table
        dry - output SQL statements without running anything on the DB
        """
        print("Grouping blocks into zones")

        if table is None:
            table = self.config.bna.connectivity.zones.table

        if schema is None:
            if "schema" in self.config.bna.connectivity.zones:
                schema = self.config.bna.connectivity.zones.schema
            else:
                schema = self.default_schema

        if uid is None:
            if "uid" in self.config.bna.connectivity.zones:
                uid = self.config.bna.connectivity.zones.uid
            else:
                uid = "id"

        if geom is None:
            if "geom" in self.config.bna.connectivity.zones:
                geom = self.config.bna.connectivity.zones.geom
            else:
                geom = "geom"

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
        query_01 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_network","01_create_prelim_zones.sql"))
        query_02 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_network","02_remove_bad_zones.sql"))
        query_03 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_network","03_aggregate_blocks.sql"))
        query_07 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_network","07_clean_up.sql"))

        conn = self.get_db_connection()
        cur = conn.cursor()

        if overwrite:
            self.drop_table(out_table,out_schema,conn=conn)

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

        # match nodes
        self._associate_nodes_with_zones(conn,subs,dry)

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


    def make_zones_from_lines(self,in_table,out_table=None,in_schema=None,
                              out_schema=None,uid=None,in_geom=None,
                              out_geom=None,lines_filter=None,overwrite=False,
                              dry=False):
        """
        Creates analysis zones that aggregate blocks into logical groupings based
        on islands formed with lines as provided in the input table. Lines should
        be topologically connected with no intersecting lines.

        args
        in_table -- the table of input lines
        out_table -- the table name to save zones to (default: from config)
        in_schema -- the schema of the input lines table (default: inferred)
        out_schema -- the schema of the zones table (default: same schema as the blocks table)
        uid -- uid column name (default: uid in config, or "id" if not in config)
        in_geom -- geom column name of the lines table (default: "geom")
        out_geom -- geom column name for the zones table (default: geom in config, or "geom" if not in config)
        lines_filter -- a filter to apply to the lines table
        overwrite -- overwrite an existing table
        dry - output SQL statements without running anything on the DB
        """
        if not self.table_exists(in_table):
            raise ValueError("No table found at %s" % in_table)

        if in_schema is None:
            in_schema = self.get_schema(in_table)

        in_uid = self.get_pkid_col(in_table,schema=in_schema)
        in_geom = "geom"

        if out_table is None:
            out_table = self.config.bna.connectivity.zones.table

        if out_schema is None:
            if "schema" in self.config.bna.connectivity.zones:
                out_schema = self.config.bna.connectivity.zones.schema
            else:
                out_schema = self.default_schema

        if uid is None:
            if "uid" in self.config.bna.connectivity.zones:
                uid = self.config.bna.connectivity.zones.uid
            else:
                uid = "id"

        if in_geom is None:
            in_geom = "geom"

        if out_geom is None:
            if "geom" in self.config.bna.connectivity.zones:
                out_geom = self.config.bna.connectivity.zones.geom
            else:
                out_geom = "geom"

        if lines_filter is None:
            lines_filter = "TRUE"

        # build subs
        subs = dict(self.sql_subs)
        subs["in_table"] = sql.Identifier(in_table)
        subs["in_schema"] = sql.Identifier(in_schema)
        subs["in_geom"] = sql.Identifier(in_geom)
        subs["lines_filter"] = sql.SQL(lines_filter)
        subs["zones_table"] = sql.Identifier(out_table)
        subs["zones_schema"] = sql.Identifier(out_schema)
        subs["zones_id_col"] = sql.Identifier(uid)
        subs["zones_geom_col"] = sql.Identifier(out_geom)
        subs["zones_index"] = sql.Identifier("sidx_" + out_table)

        # read in the raw query
        query_01 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_lines","01_create_prelim_zones.sql"))
        query_02 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_lines","02_remove_bad_zones.sql"))
        query_03 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_lines","03_aggregate_blocks.sql"))
        query_07 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_lines","07_clean_up.sql"))

        conn = self.get_db_connection()
        cur = conn.cursor()

        if overwrite:
            self.drop_table(out_table,out_schema,conn=conn)

        # create zones from source table
        q = sql.SQL(query_01).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Making zones")
            cur.execute(q)

        # remove bad zones
        q = sql.SQL(query_02).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Removing bad prelim zones")
            cur.execute(q)

        # associate blocks to zones
        q = sql.SQL(query_03).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Associating blocks to zones")
            cur.execute(q)

        # match nodes
        self._associate_nodes_with_zones(conn,subs,dry)

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


    def make_zones_from_table(self,in_table,in_schema=None,out_table=None,
                              out_schema=None,uid=None,geom=None,overwrite=False,
                              dry=False):
        """
        Creates analysis zones that aggregate blocks into logical groupings
        based on polygons from another table.

        args
        out_table -- table name for output zones (default: table name given in config)
        out_schema -- schema name (default: schema name given in config)
        uid -- uid column name (default: uid in config, or "id" if not in config)
        geom -- geom column name (default: geom in config, or "geom" if not in config)
        overwrite -- overwrite existing table
        dry - output SQL statements without running anything on the DB
        """
        print("Grouping blocks into zones")

        if not self.table_exists(in_table):
            raise ValueError("No table found at %s" % in_table)

        if in_schema is None:
            in_schema = self.get_schema(in_table)

        in_uid = self.get_pkid_col(in_table,schema=in_schema)
        in_geom = "geom" # future: add get_geom to dbutils

        if out_table is None:
            out_table = self.config.bna.connectivity.zones.table

        if out_schema is None:
            if "schema" in self.config.bna.connectivity.zones:
                out_schema = self.config.bna.connectivity.zones.schema
            else:
                out_schema = self.default_schema

        if uid is None:
            if "uid" in self.config.bna.connectivity.zones:
                uid = self.config.bna.connectivity.zones.uid
            else:
                uid = "id"

        if geom is None:
            if "geom" in self.config.bna.connectivity.zones:
                geom = self.config.bna.connectivity.zones.geom
            else:
                geom = "geom"

        # build subs
        subs = dict(self.sql_subs)
        subs["in_table"] = sql.Identifier(in_table)
        subs["in_schema"] = sql.Identifier(in_schema)
        subs["in_uid"] = sql.Identifier(in_uid)
        subs["in_geom"] = sql.Identifier(in_geom)
        subs["zones_table"] = sql.Identifier(out_table)
        subs["zones_schema"] = sql.Identifier(out_schema)
        subs["zones_id_col"] = sql.Identifier(uid)
        subs["zones_geom_col"] = sql.Identifier(geom)
        subs["zones_index"] = sql.Identifier("sidx_" + out_table)

        # read in the raw queries
        query_01 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_table","01_aggregate_blocks.sql"))
        query_07 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","from_table","07_clean_up.sql"))

        conn = self.get_db_connection()
        cur = conn.cursor()

        if overwrite:
            self.drop_table(out_table,out_schema,conn=conn)

        # create zones from source table
        q = sql.SQL(query_01).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Making zones")
            cur.execute(q)

        # match nodes
        self._associate_nodes_with_zones(conn,subs,dry)

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


    def make_zones_no_aggregation(self,table=None,schema=None,uid=None,
                                  geom=None,overwrite=False,dry=False):
        """
        Creates analysis zones simply as copies of blocks

        args
        table -- table name (default: table name given in config)
        schema -- schema name (default: schema name given in config)
        uid -- uid column name (default: uid in config, or "id" if not in config)
        geom -- geom column name (default: geom in config, or "geom" if not in config)
        overwrite -- overwrite existing table
        dry - output SQL statements without running anything on the DB
        """
        print("Copying blocks to zones")

        if table is None:
            table = self.config.bna.connectivity.zones.table

        if schema is None:
            if "schema" in self.config.bna.connectivity.zones:
                schema = self.config.bna.connectivity.zones.schema
            else:
                schema = self.default_schema

        if uid is None:
            if "uid" in self.config.bna.connectivity.zones:
                uid = self.config.bna.connectivity.zones.uid
            else:
                uid = "id"

        if geom is None:
            if "geom" in self.config.bna.connectivity.zones:
                geom = self.config.bna.connectivity.zones.geom
            else:
                geom = "geom"

        # build subs
        subs = dict(self.sql_subs)
        subs["zones_table"] = sql.Identifier(table)
        subs["zones_schema"] = sql.Identifier(schema)
        subs["zones_id_col"] = sql.Identifier(uid)
        subs["zones_geom_col"] = sql.Identifier(geom)
        subs["zones_index"] = sql.Identifier("sidx_" + table)

        # read in the raw queries
        query_01 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","no_agg","01_copy_blocks.sql"))
        query_07 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","no_agg","07_clean_up.sql"))

        conn = self.get_db_connection()
        cur = conn.cursor()

        if overwrite:
            self.drop_table(table,schema,conn=conn)

        # copy blocks into zones
        q = sql.SQL(query_01).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Copying blocks")
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


    def _associate_nodes_with_zones(self,conn=None,subs=None,dry=False):
        """
        Runs queries to associate network nodes with zones

        args:

        conn - a psycopg2 connection
        subs - a dictionary of SQL substitutions
        dry - output SQL statements without running anything on the DB
        """
        query_04 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","associate_nodes","04_unnest.sql"))
        query_05 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","associate_nodes","05_set_nodes_closest_to_center.sql"))
        query_06 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","associate_nodes","06_set_nodes_furthest_apart.sql"))
        query_07 = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","associate_nodes","07_uniqueify.sql"))

        newconn = False
        if conn is None:
            newconn = True
            conn = self.get_db_connection()
        if subs is None:
            subs = dict(self.sql_subs)

        cur = conn.cursor()

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

        q = sql.SQL(query_07).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            cur.execute(q)

        cur.close()

        if newconn:
            conn.commit()
            conn.close()


    def associate_blocks_with_zones(self,table=None,schema=None,uid=None,geom=None,dry=False):
        """
        Runs queries to associate blocks with zones

        args:
        table -- table name (default: table name given in config)
        schema -- schema name (default: schema name given in config)
        uid -- uid column name (default: uid in config, or primary key if not in config)
        geom -- geom column name (default: geom in config, or "geom" if not in config)
        dry - output SQL statements without running anything on the DB
        """
        if table is None:
            table = self.config.bna.connectivity.zones.table

        if schema is None:
            if "schema" in self.config.bna.connectivity.zones:
                schema = self.config.bna.connectivity.zones.schema
            else:
                schema = self.default_schema

        if uid is None:
            if "uid" in self.config.bna.connectivity.zones:
                uid = self.config.bna.connectivity.zones.uid
            else:
                uid = self.get_pkid_col(table,schema)

        if geom is None:
            if "geom" in self.config.bna.connectivity.zones:
                geom = self.config.bna.connectivity.zones.geom
            else:
                geom = "geom"

        subs = dict(self.sql_subs)
        subs["zones_table"] = sql.Identifier(table)
        subs["zones_schema"] = sql.Identifier(schema)
        subs["zones_id_col"] = sql.Identifier(uid)
        subs["zones_geom_col"] = sql.Identifier(geom)

        query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","associate_blocks","aggregate_blocks.sql"))

        conn = self.get_db_connection()
        cur = conn.cursor()

        q = sql.SQL(query).format(**subs)

        if dry:
            print(q.as_string(conn))
        else:
            cur.execute(q)
            cur.close()
            conn.commit()
