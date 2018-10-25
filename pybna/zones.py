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


    def make_zones(table,schema=None,uid="id",geom="geom",roads_filter=None,ints_filter=None,dry=False):
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

        # read in the raw query language
        f = open(os.path.join(self.module_dir,"sql","zones","create_zones.sql"))
        create_zones_query = f.read()
        f.close()
        f = open(os.path.join(self.module_dir,"sql","zones","associate_nodes_with_blocks.sql"))
        block_nodes_query = f.read()
        f.close()
        f = open(os.path.join(self.module_dir,"sql","zones","isolate_ls_islands.sql"))
        ls_islands_query = f.read()
        f.close()
        f = open(os.path.join(self.module_dir,"sql","zones","identify_missing_blocks.sql"))
        missing_blocks_query = f.read()
        f.close()
        f = open(os.path.join(self.module_dir,"sql","zones","aggregate_blocks.sql"))
        aggregate_blocks_query = f.read()
        f.close()
        f = open(os.path.join(self.module_dir,"sql","zones","clean_up.sql"))
        clean_up_query = f.read()
        f.close()

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

        # build zones by grabbing a block that hasn't net been
        # assigned to a zone and building a zone around it
        if self.verbose:
            print("Stitching blocks together into zones")
        mbq = sql.SQL(missing_blocks_query).format(**subs)
        if dry:
            print(mbq.as_string(conn))
        else:
            cur.execute(mbq)
            conn.commit()
            while cur.rowcount > 0:
                row = cur.fetchone()
                subs["source_nodes"] = sql.Literal(row[1])
                abq = sql.SQL(aggregate_blocks_query).format(**subs)
                cur2.execute(abq)
                conn.commit()
                cur.execute(mbq)
        if dry:
            subs["source_nodes"] = sql.Literal([1])

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
