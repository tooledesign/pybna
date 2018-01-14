###################################################################
# The Scenario class stores a BNA scenario for use in pyBNA.
# A scenario includes two graphs: one for high stress, one for low stress
###################################################################
import sys
import networkx as nx
from nxutils import *
import psycopg2
import numpy as np
from scipy.sparse import coo_matrix
import pandas as pd
import geopandas as gpd


class Scenario:
    """A scenario to analyze in the BNA."""

    def __init__(self, name, notes, conn, blocks, srid,
                    maxDist, maxStress, maxDetour,
                    censusTable, blockIdCol,
                    roadTable, roadIdCol,
                    nodeTable, nodeIdCol,
                    edgeTable, edgeIdCol, fromNodeCol, toNodeCol, stressCol, edgeCostCol,
                    verbose=False):
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

        return: None
        """
        self.progress = 0
        self.progressTotal = -1
        self.name = name
        self.notes = notes
        self.conn = conn
        self.blocks = blocks
        self.srid = srid
        self.maxStress = maxStress
        self.maxDist = maxDist
        if maxDetour:
            self.maxDetour = 1 + float(maxDetour) / 100
        else:
            self.maxDetour = 1.25
        self.verbose = verbose

        # build graphs
        self.hsG = buildNetwork(conn,edgeTable,nodeTable,edgeIdCol,nodeIdCol,
            fromNodeCol,toNodeCol,edgeCostCol,stressCol,self.verbose
        )
        self.lsG = buildRestrictedNetwork(self.hsG,self.maxStress)

        # get block nodes
        self.blocks = self.blocks.merge(
            self._getBlockNodes(censusTable,blockIdCol,roadTable,roadIdCol,nodeTable,nodeIdCol),
            on="blockid"
        )

        # create connectivity matrix
        self.connectivity = None


    def __unicode__(self):
        return u"Scenario %s  :  Max stress %i  :  Notes: %s" % (self.name, self.maxStress, self.notes)


    def __repr__(self):
        return r"Scenario %s  :  Max stress %i  :  Notes: %s" % (self.name, self.maxStress, self.notes)


    def getConnectivity(self,tiles=None):
        """Create a connectivity matrix using the this class' networkx graphs and
        census blocks. The matrix relies on bitwise math so the following
        correspondence of values to connectivity combinations is possible:
        0 = neither hs nor ls connected       (binary 00)
        1 = hs connected but not ls connected (binary 01)
        2 = ls connected but not hs connected (binary 10) (not possible under current methodology)
        3 = both hs and ls connected          (binary 11)

        kwargs:
        tiles -- a geopandas dataframe holding polygons for breaking the analysis into chunks

        return: pandas sparse dataframe
        """
        if self.verbose:
            print("Building connectivity matrix")

        # create single tile if no tiles given
        if tiles is None:
            tiles = gpd.GeoDataFrame.from_features(gpd.GeoSeries(
                self.blocks.unary_union.envelope,
                name="geom"
            ))

        df = None
        c = 1
        ctotal = len(tiles)
        for i in tiles.index:
            self.progress = 0
            if self.verbose:
                print("Processing tile %i out of %i" % (c,ctotal))

            # select blocks that intersect the tile
            self.blocks["tempkey"] = 1
            df = gpd.sjoin(
                self.blocks,
                tiles[tiles.index==i]
            )[["blockid","geom","nodes","tempkey"]]

            # convert to centroids for more accurate intersection
            df.geom = df.centroid

            # select blocks whose centroids intersect the tile
            df = gpd.sjoin(
                df,
                tiles[tiles.index==i]
            ).drop(columns=["index_right"])

            # cartesian join of subselected blocks (origins) with all census blocks (destinations)
            df = df.merge(
                self.blocks[["blockid","tempkey","nodes","geom"]],
                on="tempkey",
                suffixes=("from","to")
            ).drop(columns=["tempkey"])

            df = df[df["blockidfrom"] != df["blockidto"]]

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
            if self.verbose:
                print("Testing connectivity")
            self.progressTotal = len(df)
            df["hsls"] = df.apply(self._isConnected,axis=1)

        return df

        # get nx routes using:
        # http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.applymap.html#pandas.DataFrame.applymap
        # https://stackoverflow.com/questions/43654727/pandas-retrieve-row-and-column-name-for-each-element-during-applymap

    def _isConnected(self,row):
        if self.verbose and self.progress % 500 == 0:
            self._progbar(self.progress,self.progressTotal,50)
            sys.stdout.flush()
            # bar = "+" * int(10*float(self.progress)/self.progressTotal)
            # print("\r  %i/%i %s" % (self.progress,self.progressTotal,bar)),
        self.progress += 1

        hsConnected = False
        lsConnected = False

        fromBlock = row["blockidfrom"]
        toBlock = row["blockidto"]
        hsDist = -1
        lsDist = -1

        # first test hs connection
        for i in row["nodesfrom"]:
            for j in row["nodesto"]:
                """This part risks having a distance that is not the minimum, but
                it's necessary for speeding up the calculations"""
                if hsDist > 0 and hsDist < self.maxDist:
                    continue

                try:
                    l = nx.dijkstra_path_length(self.hsG,i,j,weight="weight")
                    if l < self.maxDist:
                        hsDist = l
                        hsConnected = True
                except nx.NetworkXNoPath:
                    pass

        # next test ls connection (but only if hsConnected)
        # if hsConnected:
        #     pass

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

        return df


    def _progbar(self, curr, total, full_progbar):
        frac = curr/total
        filled_progbar = int(round(frac*full_progbar))
        print('\r' + '#'*filled_progbar + '-'*(full_progbar-filled_progbar) + '  %0.1f%  (%i/%i)' % (frac,curr,total)),
