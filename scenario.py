###################################################################
# The Scenario class stores a BNA scenario for use in pyBNA.
# A scenario includes two graphs: one for high stress, one for low stress
###################################################################
import sys, StringIO
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

    def __init__(self, bna, config):
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

        self.name = self.config["name"]
        bna._reestablish_conn()
        self.conn = bna.conn
        self.config = _set_config_from_defaults(config,bna["defaults"]["scenario"])

        if "notes" in self.config:
            self.notes = self.config["notes"]
        else:
            notes = "Scenario %s" % self.name

        self.verbose = bna.verbose
        self.debug = bna.debug

        # build graphs
        self.hs_graph = graphutils.build_network(
            self.conn,
            self.config["edges"]["table"],
            self.config["nodes"]["table"],
            self.config["edges"]["id_column"],
            self.config["nodes"]["id_column"],
            self.config["edges"]["source_column"],
            self.config["edges"]["target_column"],
            self.config["edges"]["cost_column"],
            self.config["edges"]["stress_column"],
            self.verbose
        )
        self.ls_graph = graphutils.build_restricted_network(
            self.hs_graph,
            self.config["max_stress"]
        )

        # get block nodes
        self.blocks = bna.blocks.merge(
            self._get_block_nodes(censusTable,blockIdCol,self.config["roads"]["table"],self.config["roads"]["uid"],self.config["nodes"]["table"],self.config["nodes"]["id_column"]),
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
            self._setDebug(True)


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
                _set_config_from_defaults(config[k], defaults[k])
            elif k in config:
                pass
            else:
                config[k] = defaults[k]


    def _get_connectivity(self,tiles=None,db_table=None):
        """Create a connectivity matrix using the this class' networkx graphs and
        census blocks. The matrix relies on bitwise math so the following
        correspondence of values to connectivity combinations is possible:
        0 = neither hs nor ls connected       (binary 00)
        1 = hs connected but not ls connected (binary 01)
        2 = ls connected but not hs connected (binary 10) (not possible under current methodology)
        3 = both hs and ls connected          (binary 11)

        kwargs:
        tiles -- a geopandas dataframe holding polygons for breaking the analysis into chunks
        db_table -- if given, (over)writes the results to this table in the db

        return: pandas sparse dataframe
        """
        if self.verbose:
            print("Building connectivity matrix")

        # drop db table if given
        if db_table:
            cur = self.conn.cursor()
            cur.execute(sql.SQL('drop table if exists {}').format(sql.Identifier(db_table)))
            cur.execute(sql.SQL(
                'create table {}.{} ( \
                    id serial primary key, \
                    source_blockid10 varchar(15), \
                    target_blockid10 varchar(15), \
                    high_stress BOOLEAN, \
                    low_stress BOOLEAN \
                )'
            ).format(sql.Identifier(bna.blocks_schema),sql.Identifier(db_table)))
            cur.close()
            self.conn.commit()

        # create single tile if no tiles given
        if tiles is None:
            tiles = gpd.GeoDataFrame.from_features(gpd.GeoSeries(
                _get_graph_nodes.unary_union.envelope,
                name="geom"
            ))

        cdf = pd.DataFrame(columns=["blockidfrom","nodesfrom","graph_vfrom",
            "blockidto","nodesto","graph_vto","hsls"])
        c = 1
        ctotal = len(tiles)
        for i in tiles.index:
            if self.verbose:
                print("Processing tile %i out of %i" % (c,ctotal))
                c += 1

            # select blocks that intersect the tile
            _get_graph_nodes["tempkey"] = 1
            df = gpd.sjoin(
                _get_graph_nodes,
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

            # cartesian join of subselected blocks (origins) with all census blocks (destinations)
            df = df.merge(
                _get_graph_nodes[["blockid","tempkey","nodes","graph_v","geom"]],
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

            _get_graph_nodes = _get_graph_nodes.drop(columns=["tempkey"])

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
                sql.Identifier(bna.blocks_schema),
                sql.Identifier(db_table),
                sql.Identifier("idx_"+db_table+"_blockpairs_ls"),
                sql.Identifier(bna.blocks_schema),
                sql.Identifier(db_table),
                sql.Identifier(bna.blocks_schema),
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

        fromBlock = row["blockidfrom"]
        toBlock = row["blockidto"]
        if fromBlock == toBlock:    # if same block assume connected
            return 3
        hsDist = -1
        lsDist = -1

        # first test hs connection
        for i in row["graph_vfrom"]:
            dist = np.min(
                shortest_distance(
                    self.hs_graph,
                    source=self.hs_graph.vertex(i),
                    target=row["graph_vto"],
                    weights=self.hs_graph.ep.cost,
                    max_dist=self.config["max_distance"]
                )
            )
            if self.debug:
                self.blockDistances.append(
                    [{
                        "blockidfrom":fromBlock,
                        "blockidto":toBlock,
                        "hsDist":dist,
                        "lsDist":-1
                    }]
                )

            if not np.isinf(dist):  # test for no path
                if hsDist < 0:
                    hsDist = dist
                    hs_connected = True
                if dist < hsDist:
                    hsDist = dist

        # next test ls connection (but only if hs_connected)
        if hs_connected:
            for i in row["graph_vfrom"]:
                dist = np.min(
                    shortest_distance(
                        self.ls_graph,
                        source=self.ls_graph.vertex(i),
                        target=row["graph_vto"],
                        weights=self.ls_graph.ep.cost,
                        max_dist=self.config["max_distance"]
                    )
                )
                if self.debug:
                    self.blockDistances.append(
                        [{
                            "blockidfrom":fromBlock,
                            "blockidto":toBlock,
                            "hsDist":-1,
                            "lsDist":dist
                        }]
                    )

                if not np.isinf(dist):  # no path
                    ls_connected = True
                    continue

        return hs_connected | (ls_connected << 1)


    # def _get_block_nodes(self,censusTable,blockIdCol,self.config["roads"]["table"],self.config["roads"]["uid"],self.config["nodes"]["table"],nodeIdCol):
    def _get_block_nodes(self):
        bna._reestablish_conn()
        cur = bna.conn.cursor()

        # make temporary nodes table and add blocks
        q = sql.SQL(' \
            drop table if exists tmp_blocknodes; \
            create temp table tmp_blocknodes ( \
                id serial primary key, \
                geom geometry(multipolygon, %s), \
                blockid text, \
                nodes int[] \
            ); \
            \
            insert into tmp_blocknodes (blockid, geom) \
            select {}, st_multi(st_buffer(geom,15)) from {}; \
            \
            create index tsidx_blocknodesgeom on tmp_blocknodes using gist (geom); \
            analyze tmp_blocknodes; \
        ').format(
            sql.Identifier(bna.config["bna"]["blocks"]["id_column"]),
            sql.Identifier(bna.config["bna"]["blocks"]["table"])
        ).as_string(cur)
        cur.execute(q % bna.srid)

        # add nodes
        q = sql.SQL(' \
            update tmp_blocknodes \
            set     nodes = array(( \
                        select  v.{} \
                        from    {} r, {} v \
                        where   r.{} = v.{} \
                        and     st_intersects(tmp_blocknodes.geom,r.geom) \
                        and     ( \
                                    st_contains(tmp_blocknodes.geom,r.geom) \
                                or  st_length( \
                                        st_intersection(tmp_blocknodes.geom,r.geom) \
                                    ) > 30 \
                                ) \
            )); \
        ').format(
            sql.Identifier(self.config["nodes"]["id_column"]),
            sql.Identifier(self.config["roads"]["table"]),
            sql.Identifier(self.config["nodes"]["table"]),
            sql.Identifier(self.config["roads"]["uid"]),
            sql.Identifier(self.config["roads"]["uid"])
        )
        cur.execute(q)

        # pull down to pandas df
        df = pd.read_sql_query(
            "select blockid, nodes from tmp_blocknodes",
            self.conn
        )

        # clean up
        cur.execute('drop table if exists tmp_blocknodes;')
        cur.close()
        return df


    def _get_graph_nodes(self,nodes):
        gnodes = list()
        for n in nodes:
            try:
                gnodes.append(int(find_vertex(self.hs_graph,self.hs_graph.vp.pkid,n)[0]))
            except IndexError:
                pass    # no graph nodes in network (orphaned segment problem)
        return gnodes


    def _setDebug(self,d):
        self.debug = d
        if self.debug:
            self.blockDistances = pd.DataFrame(columns=["blockidfrom","blockidto","hsdist","lsdist"])


    def _reestablishConn(self):
        db_connection_string = self.conn.dsn
        try:
            cur = self.conn.cursor()
            cur.execute('select 1')
            cur.fetchone()
            cur.close()
        except:
            self.conn = psycopg2.connect(db_connection_string)
