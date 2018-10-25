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


    def make_zones(self,table,schema=None,uid="id",geom="geom",roads_filter=None,ints_filter=None,dry=False):
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

        if ints_filter is None:
            ints_filter = "TRUE"

        # build subs
        subs = dict(self.sql_subs)
        subs["roads_filter"] = sql.SQL(roads_filter)
        subs["ints_filter"] = sql.SQL(ints_filter)
        subs["zones_table"] = sql.Identifier(table)
        subs["zones_schema"] = sql.Identifier(schema)
        subs["zones_id_col"] = sql.Identifier(uid)
        subs["zones_geom_col"] = sql.Identifier(geom)
        subs["zones_index"] = sql.Identifier("sidx_" + table)

        # read in the raw queries
        create_zones_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","create_zones.sql"))
        block_nodes_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","associate_nodes_with_blocks.sql"))
        ls_islands_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","isolate_ls_islands.sql"))
        missing_blocks_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","identify_missing_blocks.sql"))
        total_missing_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","total_missing_blocks.sql"))
        aggregate_blocks_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","aggregate_blocks.sql"))
        remaining_blocks_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","add_remaining_blocks.sql"))
        clean_up_query = self.read_sql_from_file(os.path.join(self.module_dir,"sql","zones","clean_up.sql"))

        conn = self.get_db_connection()
        cur = conn.cursor()
        cur2 = conn.cursor()

        # create zones
        q = sql.SQL(create_zones_query).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Creating zones table")
            cur.execute(q)

        # associate blocks with nodes
        q = sql.SQL(block_nodes_query).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Associating blocks with nodes")
            cur.execute(q)

        # set up low stress islands
        q = sql.SQL(ls_islands_query).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Identifying low stress islands")
            cur.execute(q)

        # build zones by grabbing a block that hasn't yet been
        # assigned to a zone and building a zone around it
        if self.verbose:
            print("Stitching blocks together into zones")
        mbq = sql.SQL(missing_blocks_query).format(**subs)
        tbq = sql.SQL(total_missing_query).format(**subs)
        remain = -1
        if dry:
            print(mbq.as_string(conn))
        else:
            cur.execute(mbq)
            conn.commit()
            while cur.rowcount > 0:
                cur2.execute(tbq)
                missing_blocks = cur2.fetchone()[0]
                if missing_blocks != remain:
                    remain = missing_blocks
                    row = cur.fetchone()
                    subs["source_nodes"] = sql.Literal(row[1])
                    abq = sql.SQL(aggregate_blocks_query).format(**subs)
                    cur2.execute(abq)
                    conn.commit()
                    cur.execute(mbq)
        if dry:
            subs["source_nodes"] = sql.Literal([1])
            abq = sql.SQL(aggregate_blocks_query).format(**subs)
            print(abq.as_string(conn))

        # add any remaining blocks that didn't get picked up
        if not dry:
            q = sql.SQL(remaining_blocks_query).format(**subs)
            cur.execute(q)

        # clean up
        q = sql.SQL(clean_up_query).format(**subs)
        if dry:
            print(q.as_string(conn))
        else:
            if self.verbose:
                print("Cleaning up")
            cur.execute(q)

        conn.commit()
        cur2.close()
        cur.close()
        conn.close()
