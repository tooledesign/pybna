###################################################################
# The Scenario class stores a BNA scenario for use in pyBNA.
# A scenario includes two graphs: one for high stress, one for low stress
###################################################################
import sys, StringIO
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

    def __init__(self, name, notes, conn, blocks, srid,
                    maxDist, maxStress, maxDetour,
                    censusSchema, censusTable, blockIdCol,
                    roadTable, roadIdCol,
                    nodeTable, nodeIdCol,
                    edgeTable, edgeIdCol, fromNodeCol, toNodeCol, stressCol, edgeCostCol,
                    verbose=False, debug=False):
        """Get already built network from PostGIS connection.

        args:
        name -- this scenario's name as stored in the parent pyBNA object
        notes -- any notes to provide further information about this scenario
        conn -- a psycopg2 database connection
        blocks -- geopandas dataframe of census blocks
        srid -- the srid of the database
        maxDist -- the travel shed size, or maximum allowable trip distance (in units of the underlying coordinate system)
        maxStress -- the highest stress rating to allow for the low stress graph
        maxDetour -- maximum allowable detour for determining relative connectivity (given as a percentage, i.e. 25 = 25%)
        censusSchema -- DB schema the census table is in
        censusTable -- name of table of census blocks (default: neighborhood_census_blocks, the BNA default)
        blockIdCol -- name of the column with census block ids in the block table (default: blockid10, the BNA default)
        roadTable -- the table with road data
        roadIdCol -- column name that uniquely identifies roads. if None uses the primary key defined on the table.
        nodeTable -- name of the table of network nodes
        nodeIdCol -- column name for the node IDs
        edgeTable -- name of the table of network edges
        edgeIdCol -- column name for edge IDs
        fromNodeCol -- column name for the from node in edge table
        toNodeCol -- column name for the to node in edge table
        stressCol -- column name for the stress of the edge
        edgeCostCol -- column name for the cost of the edge
        verbose -- output useful messages
        debug -- produce additional output for testing and debugging

        return: None
        """
        # register pandas apply with tqdm for progress bar
        tqdm.pandas(desc="Evaluating connectivity")

        self.name = name
        self.notes = notes
        self.conn = conn
        self.censusSchema = censusSchema
        self.blocks = blocks
        self.srid = srid
        self.maxStress = maxStress
        self.maxDist = maxDist
        if maxDetour:
            self.maxDetour = 1 + float(maxDetour) / 100
        else:
            self.maxDetour = 1.25
        self.verbose = verbose
        self.debug = debug

        # build graphs
        self.hsG = graphutils.buildNetwork(conn,edgeTable,nodeTable,edgeIdCol,nodeIdCol,
            fromNodeCol,toNodeCol,edgeCostCol,stressCol,self.verbose
        )
        self.lsG = graphutils.buildRestrictedNetwork(self.hsG,self.maxStress)

        # get block nodes
        self.blocks = self.blocks.merge(
            self._getBlockNodes(censusTable,blockIdCol,roadTable,roadIdCol,nodeTable,nodeIdCol),
            on="blockid"
        )

        # get block graph nodes
        # self.blocks["graph_v"] = self.blocks["nodes"].apply(
        #     lambda x: [int(find_vertex(self.hsG,self.hsG.vp.pkid,i)[0]) for i in x]
        # )
        self.blocks["graph_v"] = self.blocks["nodes"].apply(self._getGraphNodes)

        # create connectivity matrix
        self.connectivity = None

        if debug:
            self._setDebug(True)


    def __unicode__(self):
        return u"Scenario %s  :  Max stress %i  :  Notes: %s" % (self.name, self.maxStress, self.notes)


    def __repr__(self):
        return r"Scenario %s  :  Max stress %i  :  Notes: %s" % (self.name, self.maxStress, self.notes)


    def getConnectivity(self,tiles=None,dbTable=None):
        """Create a connectivity matrix using the this class' networkx graphs and
        census blocks. The matrix relies on bitwise math so the following
        correspondence of values to connectivity combinations is possible:
        0 = neither hs nor ls connected       (binary 00)
        1 = hs connected but not ls connected (binary 01)
        2 = ls connected but not hs connected (binary 10) (not possible under current methodology)
        3 = both hs and ls connected          (binary 11)

        kwargs:
        tiles -- a geopandas dataframe holding polygons for breaking the analysis into chunks
        dbTable -- if given, (over)writes the results to this table in the db

        return: pandas sparse dataframe
        """
        if self.verbose:
            print("Building connectivity matrix")

        # drop db table if given
        if dbTable:
            cur = self.conn.cursor()
            cur.execute(sql.SQL('drop table if exists {}').format(sql.Identifier(dbTable)))
            cur.execute(sql.SQL(
                'create table {}.{} ( \
                    id serial primary key, \
                    source_blockid10 varchar(15), \
                    target_blockid10 varchar(15), \
                    high_stress BOOLEAN, \
                    low_stress BOOLEAN \
                )'
            ).format(sql.Identifier(self.censusSchema),sql.Identifier(dbTable)))
            cur.close()
            self.conn.commit()

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
            if self.verbose:
                print("Processing tile %i out of %i" % (c,ctotal))
                c += 1

            # select blocks that intersect the tile
            self.blocks["tempkey"] = 1
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

            # cartesian join of subselected blocks (origins) with all census blocks (destinations)
            df = df.merge(
                self.blocks[["blockid","tempkey","nodes","graph_v","geom"]],
                on="tempkey",
                suffixes=("from","to")
            ).drop(columns=["tempkey"])

            # filter out based on distances between blocks
            if self.verbose:
                print("Filtering blocks based on crow-flies distance")
            df["keep"] = df.apply(
                lambda x: x["geomfrom"].distance(x["geomto"]) < self.maxDist,
                axis=1
            )
            df = df[df.keep].drop(columns=["geomfrom","geomto","keep"]) #.to_sparse()

            self.blocks = self.blocks.drop(columns=["tempkey"])

            # add stress connectivity column
            df["hsls"] = pd.SparseSeries([False] * len(df),dtype="int8",fill_value=0)

            # run connectivity for all rows
            df["hsls"] = df.progress_apply(self._isConnected,axis=1)

            if dbTable:
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
                cur.copy_from(f,dbTable,columns=("source_blockid10","target_blockid10","high_stress","low_stress"),sep=",")
                cur.close()
                self.conn.commit()
            cdf = cdf.append(df).drop_duplicates(subset=["blockidfrom","blockidto"])

        if dbTable:
            self._reestablishConn()
            cur = self.conn.cursor()
            cur.execute(sql.SQL(' \
                create index {} on {}.{} (source_blockid10,target_blockid10) where high_stress is true; \
                create index {} on {}.{} (source_blockid10,target_blockid10) where low_stress is true; \
                analyze {}.{}; \
            ').format(
                sql.Identifier("idx_"+dbTable+"_blockpairs_hs"),
                sql.Identifier(self.censusSchema),
                sql.Identifier(dbTable),
                sql.Identifier("idx_"+dbTable+"_blockpairs_ls"),
                sql.Identifier(self.censusSchema),
                sql.Identifier(dbTable),
                sql.Identifier(self.censusSchema),
                sql.Identifier(dbTable)
            ))
            self.conn.commit()
        return cdf


    def _isConnected(self,row):
        hsConnected = False
        lsConnected = False

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
                    self.hsG,
                    source=self.hsG.vertex(i),
                    target=row["graph_vto"],
                    weights=self.hsG.ep.cost,
                    max_dist=self.maxDist
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
                    hsConnected = True
                if dist < hsDist:
                    hsDist = dist

        # next test ls connection (but only if hsConnected)
        if hsConnected:
            for i in row["graph_vfrom"]:
                dist = np.min(
                    shortest_distance(
                        self.lsG,
                        source=self.lsG.vertex(i),
                        target=row["graph_vto"],
                        weights=self.lsG.ep.cost,
                        max_dist=self.maxDist
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
                    lsConnected = True
                    continue

        return hsConnected | (lsConnected << 1)


    def _getBlockNodes(self,censusTable,blockIdCol,roadTable,roadIdCol,nodeTable,nodeIdCol):
        cur = self.conn.cursor()

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
            sql.Identifier(blockIdCol),
            sql.Identifier(censusTable)
        ).as_string(cur)
        cur.execute(q % self.srid)

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
            sql.Identifier(nodeIdCol),
            sql.Identifier(roadTable),
            sql.Identifier(nodeTable),
            sql.Identifier(roadIdCol),
            sql.Identifier(roadIdCol)
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


    def _getGraphNodes(self,nodes):
        gnodes = list()
        for n in nodes:
            try:
                gnodes.append(int(find_vertex(self.hsG,self.hsG.vp.pkid,n)[0]))
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
