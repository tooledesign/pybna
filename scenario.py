###################################################################
# The Scenario class stores a BNA scenario for use in pyBNA.
# A scenario includes two graphs: one for high stress, one for low stress
###################################################################
import sys, os, StringIO
import collections
from graph_tool.all import *
import psycopg2
from psycopg2 import sql
from tempfile import mkstemp
import numpy as np
from scipy.sparse import coo_matrix
import pandas as pd
import geopandas as gpd
import graphutils
from tqdm import tqdm


class Scenario:
    """A scenario to analyze in the BNA."""

    def __init__(self, bna, config, build_network=True):
        """
        Create new scenario from PostGIS.

        args:
        bna -- reference to the parent pybna object that contains this scenario
        config -- dictionary of configuration options for this scenario
            (comes from the config file)
        verbose -- output useful messages
        debug -- produce additional output for testing and debugging

        return: None
        """
        # register pandas apply with tqdm for progress bar
        tqdm.pandas(desc="Evaluating connectivity")

        self.bna = bna
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        self.config = config
        self._set_config_from_defaults(self.config,self.bna.config["bna"]["defaults"]["scenario"])
        self.name = self.config["name"]
        self.bna._reestablish_conn()
        self.conn = self.bna.conn
        self.config["max_detour"] = float(100 + self.config["max_detour"])/100

        if "notes" in self.config:
            self.notes = self.config["notes"]
        else:
            notes = "Scenario %s" % self.name

        self.verbose = self.bna.verbose
        self.debug = self.bna.debug

        if build_network:
            self.build_db_network()
        elif not self._check_db_network():
            print("Network tables not found in database")
            self.build_db_network()
        else:
            print("Network tables found in database")

        # build graphs
        if not self.debug:
            self.hs_graph = graphutils.build_network(
                self.conn,
                self.config["edges"],
                self.config["nodes"],
                self.verbose
            )
            self.ls_graph = graphutils.build_restricted_network(
                self.hs_graph,
                self.config["max_stress"]
            )

            # get block nodes
            self.blocks = self.bna.blocks.merge(
                self._get_block_nodes(),
                on="blockid"
            )

            # get block graph nodes
            # self.blocks["graph_v"] = self.blocks["nodes"].apply(
            #     lambda x: [int(find_vertex(self.hs_graph,self.hs_graph.vp.pkid,i)[0]) for i in x]
            # )
            self.blocks["graph_v"] = self.blocks["nodes"].apply(self._get_graph_nodes)

        # create connectivity matrix
        self.connectivity = None

        if self.debug:
            self._set_debug(True)


    def __unicode__(self):
        return u"Scenario %s  :  Max stress %i  :  Notes: %s" % (self.name, self.config["max_stress"], self.notes)


    def __repr__(self):
        return r"Scenario %s  :  Max stress %i  :  Notes: %s" % (self.name, self.config["max_stress"], self.notes)


    def _set_config_from_defaults(self, config, defaults):
        # adapted from https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
        """
        Applies defaults to the scenario settings

        args:
        config -- this scenario's settings
        defaults -- the defaults from the config file
        """
        for k, v in defaults.iteritems():
            if (k in config and isinstance(config[k], dict)
                    and isinstance(defaults[k], collections.Mapping)):
                self._set_config_from_defaults(config[k], defaults[k])
            elif k in config:
                pass
            else:
                config[k] = defaults[k]


    def _get_connectivity(self,tiles=None,orig_blockid=None,dest_blockid=None,db_table=None,append=False):
        """Create a connectivity matrix using the this class' graphs and
        census blocks. The matrix relies on bitwise math so the following
        correspondence of values to connectivity combinations is possible:
        0 = neither hs nor ls connected       (binary 00)
        1 = hs connected but not ls connected (binary 01)
        2 = ls connected but not hs connected (binary 10) (not possible under current methodology)
        3 = both hs and ls connected          (binary 11)

        kwargs:
        tiles -- a geopandas dataframe holding polygons for breaking the analysis into chunks (can't be used with blockid)
        orig_blockid -- uid of a specific block to use as an origin (can't be used with tiles)
        dest_blockid -- uid of a specific block to use as a destination (can't be used with tiles)
        db_table -- if given, writes the results to this table in the db
        append -- determines whether to append results to an existing db table
            (only applies if db_table is given)

        return: pandas sparse dataframe
        """
        if isinstance(tiles,gpd.GeoDataFrame) and (orig_blockid or dest_blockid):
            raise ValueError("Can't run connectivity using tiles and blockids")

        if self.verbose:
            print("Building connectivity matrix")

        # drop db table if given or check existence if append mode set
        if db_table and not append:
            self.create_db_connectivity_table(db_table,overwrite=True)
        elif db_table:
            cur = self.conn.cursor()
            try:
                cur.execute(sql.SQL('select 1 from {} limit 1').format(sql.Identifier(db_table)))
                cur.close()
            except psycopg2.ProgrammingError:
                raise ValueError("table %s not found" % db_table)

        # create single tile if no tiles given
        if tiles is None:
            tiles = gpd.GeoDataFrame.from_features(gpd.GeoSeries(
                self.blocks.unary_union.envelope,
                name="geom"
            ))

        cdf = pd.DataFrame(columns=["blockidfrom","nodesfrom","graph_vfrom",
            "blockidto","nodesto","graph_vto","hsls"])
        c = 1
        ctotal = len(tiles)
        for i in tiles.index:
            self.blocks["tempkey"] = 1

            if orig_blockid:
                df = self.blocks[self.blocks["blockid"]==orig_blockid]
            else:
                if self.verbose:
                    print("Processing tile %i out of %i" % (c,ctotal))
                    c += 1

                # select blocks that intersect the tile
                df = gpd.sjoin(
                    self.blocks,
                    tiles[tiles.index==i]
                )[["blockid","geom","nodes","graph_v","tempkey"]]

                # skip this tile if it's empty
                if len(df) == 0:
                    continue

                # convert to centroids for more accurate intersection
                df.geom = df.centroid

                # select blocks whose centroids intersect the tile
                df = gpd.sjoin(
                    df,
                    tiles[tiles.index==i]
                ).drop(columns=["index_right"])

                # skip this tile if it's empty
                if len(df) == 0:
                    continue

            if dest_blockid:
                dblocks = self.blocks[self.blocks["blockid"]==dest_blockid][["blockid","tempkey","nodes","graph_v","geom"]]
            else:
                dblocks = self.blocks[["blockid","tempkey","nodes","graph_v","geom"]]

            # cartesian join of subselected blocks (origins) with all census blocks (destinations)
            df = df.merge(
                dblocks,
                on="tempkey",
                suffixes=("from","to")
            ).drop(columns=["tempkey"])

            # filter out based on distances between blocks
            if self.verbose:
                print("Filtering blocks based on crow-flies distance")
            df["keep"] = df.apply(
                lambda x: x["geomfrom"].distance(x["geomto"]) < self.config["max_distance"],
                axis=1
            )
            df = df[df.keep].drop(columns=["geomfrom","geomto","keep"]) #.to_sparse()

            self.blocks = self.blocks.drop(columns=["tempkey"])

            # add stress connectivity column
            df["hsls"] = pd.SparseSeries([False] * len(df),dtype="int8",fill_value=0)

            # run connectivity for all rows
            df["hsls"] = df.progress_apply(self._is_connected,axis=1)

            if db_table:
                if self.verbose:
                    print("\nWriting tile results to database")
                f = StringIO.StringIO()
                out = df[["blockidfrom","blockidto","hsls"]].copy()
                out["high_stress"] = np.where(out["hsls"] & 0b01 == 0b01, True, False)  # bitwise t/f test for high stress
                out["low_stress"] = np.where(out["hsls"] & 0b10 == 0b10, True, False)   # bitwise t/f test for low stress
                out[["blockidfrom","blockidto","high_stress","low_stress"]].to_csv(f,index=False,header=False)
                f.seek(0)
                self._reestablishConn()
                cur = self.conn.cursor()
                cur.copy_from(f,db_table,columns=("source_blockid10","target_blockid10","high_stress","low_stress"),sep=",")
                cur.close()
                self.conn.commit()
            cdf = cdf.append(df).drop_duplicates(subset=["blockidfrom","blockidto"])

        if db_table:
            self._reestablishConn()
            cur = self.conn.cursor()
            cur.execute(sql.SQL(' \
                create index {} on {}.{} (source_blockid10,target_blockid10) where high_stress is true; \
                create index {} on {}.{} (source_blockid10,target_blockid10) where low_stress is true; \
                analyze {}.{}; \
            ').format(
                sql.Identifier("idx_"+db_table+"_blockpairs_hs"),
                sql.Identifier(self.bna.blocks_schema),
                sql.Identifier(db_table),
                sql.Identifier("idx_"+db_table+"_blockpairs_ls"),
                sql.Identifier(self.bna.blocks_schema),
                sql.Identifier(db_table),
                sql.Identifier(self.bna.blocks_schema),
                sql.Identifier(db_table)
            ))
            self.conn.commit()
        return cdf


    def _is_connected(self,row):
        hs_connected = False
        ls_connected = False

        # short circuit this pair if the origin or destination blocks have no vertices
        if len(row["graph_vfrom"]) == 0 or len(row["graph_vto"]) == 0:
            return 0

        from_block = row["blockidfrom"]
        to_block = row["blockidto"]
        if from_block == to_block:    # if same block assume connected
            return 3
        hs_dist = -1
        ls_dist = -1

        # create temporary vertices for from and to blocks
        o_vertex = self.hs_graph.add_vertex()
        d_vertex = self.hs_graph.add_vertex()
        for i in row["graph_vfrom"]:
            self.hs_graph.add_edge(o_vertex,self.hs_graph.vertex(i))
        for i in row["graph_vto"]:
            self.hs_graph.add_edge(self.hs_graph.vertex(i),d_vertex)

        # first test hs connection
        dist = shortest_distance(
                self.hs_graph,
                source=o_vertex,
                target=d_vertex,
                weights=self.hs_graph.ep.cost,
                max_dist=self.config["max_distance"]
        )
        if self.debug:
            self.block_distances.append(
                [{
                    "blockidfrom":from_block,
                    "blockidto":to_block,
                    "hs_dist":dist,
                    "ls_dist":-1
                }]
            )

        if not np.isinf(dist):  # test for no path
            if hs_dist < 0:
                hs_dist = dist
                hs_connected = True
            if dist < hs_dist:
                hs_dist = dist

        # remove temporary vertices from graph
        self.hs_graph.remove_vertex([o_vertex,d_vertex])

        # next test ls connection (but only if hs_connected)
        if hs_connected:
            o_vertex = self.ls_graph.add_vertex()
            d_vertex = self.ls_graph.add_vertex()
            for i in row["graph_vfrom"]:
                self.ls_graph.add_edge(o_vertex,self.ls_graph.vertex(i))
            for i in row["graph_vto"]:
                self.ls_graph.add_edge(self.ls_graph.vertex(i),d_vertex)

            dist = shortest_distance(
                    self.ls_graph,
                    source=o_vertex,
                    target=d_vertex,
                    weights=self.ls_graph.ep.cost,
                    max_dist=self.config["max_distance"]
            )
            if self.debug:
                self.block_distances.append(
                    [{
                        "blockidfrom":from_block,
                        "blockidto":to_block,
                        "hs_dist":-1,
                        "ls_dist":dist
                    }]
                )

            if not np.isinf(dist):  # no path
                if ls_dist <= (hs_dist * self.config["max_detour"]):
                    ls_connected = True

            # remove temporary vertices from graph
            self.ls_graph.remove_vertex([o_vertex,d_vertex])

        return hs_connected | (ls_connected << 1)


    def _get_block_nodes(self):
        # set up substitutions
        subs = {
            "blocks_schema": sql.Identifier(self.bna.blocks_schema),
            "blocks": sql.Identifier(self.bna.config["bna"]["blocks"]["table"]),
            "block_id": sql.Identifier(self.bna.config["bna"]["blocks"]["id_column"]),
            "block_geom": sql.Identifier(self.bna.config["bna"]["blocks"]["geom"]),
            "roads_schema": sql.Identifier(self.bna._get_schema(self.config["roads"]["table"])),
            "roads": sql.Identifier(self.config["roads"]["table"]),
            "road_id": sql.Identifier(self.config["roads"]["uid"]),
            "road_geom": sql.Identifier(self.config["roads"]["geom"]),
            "nodes": sql.Identifier(self.config["nodes"]["table"]),
            "node_id": sql.Identifier(self.config["nodes"]["id_column"]),
            "distance": sql.Literal(self.bna.config["bna"]["blocks"]["roads_tolerance"]),
            "min_length": sql.Literal(self.bna.config["bna"]["blocks"]["min_road_length"])
        }

        # read in the raw query language
        f = open(os.path.join(self.module_dir,"sql","block_nodes.sql"))
        raw = f.read()
        f.close()

        self.bna._reestablish_conn()
        q = sql.SQL(raw).format(**subs).as_string(self.bna.conn)

        if self.debug:
            print(q)

        return pd.read_sql_query(
            q,
            self.bna.conn
        )


    def _get_graph_nodes(self,nodes):
        gnodes = list()
        for n in nodes:
            try:
                gnodes.append(int(find_vertex(self.hs_graph,self.hs_graph.vp.pkid,n)[0]))
            except IndexError:
                pass    # no graph nodes in network (orphaned segment problem)
        return gnodes


    def _set_debug(self,d):
        self.debug = d
        if self.debug:
            self.block_distances = pd.DataFrame(columns=["blockidfrom","blockidto","hs_dist","ls_dist"])


    def _reestablishConn(self):
        db_connection_string = self.conn.dsn
        try:
            cur = self.conn.cursor()
            cur.execute('select 1')
            cur.fetchone()
            cur.close()
        except:
            self.conn = psycopg2.connect(db_connection_string)


    def build_db_network(self,dry=False):
        """
        Builds the network in the DB using details from the BNA config file.

        args:
        dry -- dry run only, don't execute the query in the DB (for debugging)
        """
        if self.verbose:
            print("Building network in database")

        # set up substitutions
        net_subs = {
            "srid": sql.Literal(self.bna.srid),
            "schema": sql.Identifier(self.bna._get_schema(self.config["roads"]["table"])),
            "roads": sql.Identifier(self.config["roads"]["table"]),
            "road_id": sql.Identifier(self.config["roads"]["uid"]),
            "roads_geom": sql.Identifier(self.config["roads"]["geom"]),
            "road_source": sql.Identifier(self.config["roads"]["source_column"]),
            "road_target": sql.Identifier(self.config["roads"]["target_column"]),
            "one_way": sql.Identifier(self.config["roads"]["oneway"]["name"]),
            "forward": sql.Literal(self.config["roads"]["oneway"]["forward"]),
            "backward": sql.Literal(self.config["roads"]["oneway"]["backward"]),
            "intersections": sql.Identifier(self.config["intersections"]["table"]),
            "int_id": sql.Identifier(self.config["intersections"]["uid"]),
            "nodes": sql.Identifier(self.config["nodes"]["table"]),
            "node_id": sql.Identifier(self.config["nodes"]["id_column"]),
            "edges": sql.Identifier(self.config["edges"]["table"]),
            "edge_id": sql.Identifier(self.config["edges"]["id_column"]),
            "ft_seg_stress": sql.Identifier(self.config["roads"]["stress"]["segment"]["forward"]),
            "tf_seg_stress": sql.Identifier(self.config["roads"]["stress"]["segment"]["backward"]),
            "ft_int_stress": sql.Identifier(self.config["roads"]["stress"]["crossing"]["forward"]),
            "tf_int_stress": sql.Identifier(self.config["roads"]["stress"]["crossing"]["backward"])
        }

        # read in the raw query language
        f = open(os.path.join(self.module_dir,"sql","build_network.sql"))
        raw = f.read()
        f.close()

        cur = self.conn.cursor()
        statements = [s for s in raw.split(";") if len(s.strip()) > 1]
        for statement in tqdm(statements):
            # compose the query
            q = sql.SQL(statement).format(**net_subs)

            if dry:
                print(q.as_string(self.conn))
            else:
                cur.execute(q)
        self.conn.commit()
        del cur


    def _check_db_network(self):
        """
        Checks for the db network tables identified in the config file.

        returns True if they exist, False if they don't
        """
        for table in [self.config["edges"]["table"],self.config["nodes"]["table"]]:
            if self.verbose:
                print("Checking for %s in database" % table)

            try:
                cur = self.conn.cursor()
                cur.execute(
                    sql.SQL(
                        "select * from {} limit 1"
                    ).format(
                        sql.Identifier(table)
                    )
                )
                cur.fetchone()
            except psycopg2.ProgrammingError:
                return False

        # no errors = tables found
        return True


    def create_db_connectivity_table(self,db_table,overwrite=False):
        cur = self.conn.cursor()
        if overwrite:
            cur.execute(sql.SQL('drop table if exists {}').format(sql.Identifier(db_table)))
        cur.execute(sql.SQL(
            'create table {}.{} ( \
                id serial primary key, \
                source_blockid10 varchar(15), \
                target_blockid10 varchar(15), \
                high_stress BOOLEAN, \
                low_stress BOOLEAN \
            )'
        ).format(sql.Identifier(self.bna.blocks_schema),sql.Identifier(db_table)))
        cur.close()
        self.conn.commit()
