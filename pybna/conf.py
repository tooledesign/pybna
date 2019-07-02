###################################################################
# The Config class houses methods for dealing with the config file
###################################################################
import collections
from psycopg2 import sql
from munch import Munch
from dbutils import DBUtils


class Conf(DBUtils):
    """pyBNA Connectivity class"""

    def __init__(self):
        DBUtils.__init__(self,"")


    def parse_config(self,config):
        """
        Reads through the giant dictionary loaded from YAML and converts into
        munches that can be accessed with dot-notation

        args:
        config -- a dictionary of configuration options

        returns:
        Munch
        """
        if isinstance(config, collections.Mapping):
            for key, value in config.iteritems():
                config[key] = self.parse_config(value)
            return Munch(config)
        return config


    def make_sql_substitutions(self, config):
        """
        Constructs universal SQL substitutions from all of the config
        parameters.

        returns:
        dictionary of SQL substitutions
        """
        default_schema = self.get_default_schema()
        boundary = config.bna.boundary
        blocks = config.bna.blocks
        network = config.bna.network
        connectivity = config.bna.connectivity

        # boundary
        boundary_schema, boundary.table = self.parse_table_name(boundary.table)
        if boundary_schema is None:
            try:
                boundary_schema = self.get_schema(boundary.table)
            except:
                boundary_schema = default_schema
        if "geom" in boundary:
            boundary_geom = boundary.geom
        else:
            boundary_geom = "geom"

        # blocks
        blocks_schema, blocks.table = self.parse_table_name(blocks.table)
        if blocks_schema is None:
            try:
                blocks_schema = self.get_schema(blocks.table)
            except:
                blocks_schema = default_schema
        if "uid" in blocks:
            blocks_id_col = blocks.uid
        else:
            blocks_id_col = self.get_pkid_col(blocks.table,blocks_schema)
        if "geom" in blocks:
            blocks_geom_col = blocks.geom
        else:
            blocks_geom_col = "geom"

        # roads
        roads_schema, network.roads.table = self.parse_table_name(network.roads.table)
        if roads_schema is None:
            try:
                roads_schema = self.get_schema(network.roads.table)
            except:
                roads_schema = default_schema
        if "uid" in network.roads:
            roads_id_col = network.roads.uid
        else:
            roads_id_col = self.get_pkid_col(network.roads.table,roads_schema)
        if "geom" in network.roads:
            roads_geom_col = network.roads.geom
        else:
            roads_geom_col = "geom"

        # intersections
        ints_schema, network.intersections.table = self.parse_table_name(network.intersections.table)
        if ints_schema is None:
            try:
                ints_schema = self.get_schema(network.intersections.table)
            except:
                ints_schema = default_schema
        if "uid" in network.intersections:
            ints_id_col = network.intersections.uid
        else:
            ints_id_col = self.get_pkid_col(network.intersections.table,ints_schema)
        if "geom" in network.intersections:
            ints_geom_col = network.intersections.geom
        else:
            ints_geom_col = "geom"

        # edges
        edges_schema, network.edges.table = self.parse_table_name(network.edges.table)
        if edges_schema is None:
            if self.table_exists(network.edges.table):
                edges_schema = self.get_schema(network.edges.table)
            else:
                edges_schema = roads_schema
        if "uid" in network.edges:
            edges_id_col = network.edges.uid
        elif self.table_exists(network.edges.table,edges_schema):
            edges_id_col = self.get_pkid_col(network.edges.table,edges_schema)
        else:
            edges_id_col = "edge_id"
        if "geom" in network.edges:
            edges_geom_col = network.edges.geom
        elif self.table_exists(network.edges.table,edges_schema):
            edges_geom_col = "geom"
        else:
            edges_geom_col = "geom"

        # nodes
        nodes_schema, network.nodes.table = self.parse_table_name(network.nodes.table)
        if nodes_schema is None:
            if self.table_exists(network.nodes.table):
                nodes_schema = self.get_schema(network.nodes.table)
            else:
                nodes_schema = roads_schema
        if "uid" in network.nodes:
            nodes_id_col = network.nodes.uid
        elif self.table_exists(network.nodes.table,nodes_schema):
            nodes_id_col = self.get_pkid_col(network.nodes.table,nodes_schema)
        else:
            nodes_id_col = "node_id"
        if "geom" in network.nodes:
            nodes_geom_col = network.nodes.geom
        elif self.table_exists(network.nodes.table,nodes_schema):
            nodes_geom_col = "geom"
        else:
            nodes_geom_col = "geom"

        # connectivity
        connectivity_schema, connectivity.table = self.parse_table_name(connectivity.table)
        if connectivity_schema is None:
            connectivity_schema = blocks_schema

        # srid
        if "srid" in config:
            srid = config.srid
        else:
            srid = self.get_srid(blocks.table,schema=blocks_schema)

        subs = {
            "srid": sql.Literal(srid),
            "boundary_table": sql.Identifier(boundary.table),
            "boundary_schema": sql.Identifier(boundary_schema),
            "boundary_geom_col": sql.Identifier(boundary_geom),
            "blocks_table": sql.Identifier(blocks.table),
            "blocks_schema": sql.Identifier(blocks_schema),
            "blocks_id_col": sql.Identifier(blocks_id_col),
            "blocks_geom_col": sql.Identifier(blocks_geom_col),
            "blocks_population_col": sql.Identifier(blocks.population),
            "blocks_roads_tolerance": sql.Literal(blocks.roads_tolerance),
            "blocks_min_road_length": sql.Literal(blocks.min_road_length),
            "roads_table": sql.Identifier(network.roads.table),
            "roads_schema": sql.Identifier(roads_schema),
            "roads_id_col": sql.Identifier(roads_id_col),
            "roads_geom_col": sql.Identifier(roads_geom_col),
            "roads_source_col": sql.Identifier(network.roads.source_column),
            "roads_target_col": sql.Identifier(network.roads.target_column),
            "roads_oneway_col": sql.Identifier(network.roads.oneway.name),
            "roads_oneway_fwd": sql.Literal(network.roads.oneway.forward),
            "roads_oneway_bwd": sql.Literal(network.roads.oneway.backward),
            "roads_stress_seg_fwd": sql.Identifier(network.roads.stress.segment.forward),
            "roads_stress_seg_bwd": sql.Identifier(network.roads.stress.segment.backward),
            "roads_stress_cross_fwd": sql.Identifier(network.roads.stress.crossing.forward),
            "roads_stress_cross_bwd": sql.Identifier(network.roads.stress.crossing.backward),
            "ints_table": sql.Identifier(network.intersections.table),
            "ints_schema": sql.Identifier(ints_schema),
            "ints_id_col": sql.Identifier(ints_id_col),
            "ints_geom_col": sql.Identifier(ints_geom_col),
            "ints_cluster_distance": sql.Literal(network.intersections.cluster_distance),
            "edges_table": sql.Identifier(network.edges.table),
            "edges_schema": sql.Identifier(edges_schema),
            "edges_id_col": sql.Identifier(edges_id_col),
            "edges_geom_col": sql.Identifier(edges_geom_col),
            "edges_source_col": sql.Identifier(network.edges.source_column),
            "edges_target_col": sql.Identifier(network.edges.target_column),
            "edges_stress_col": sql.Identifier(network.edges.stress_column),
            "edges_cost_col": sql.Identifier(network.edges.cost_column),
            "nodes_table": sql.Identifier(network.nodes.table),
            "nodes_schema": sql.Identifier(nodes_schema),
            "nodes_id_col": sql.Identifier(nodes_id_col),
            "nodes_geom_col": sql.Identifier(nodes_geom_col),
            "connectivity_table": sql.Identifier(connectivity.table),
            "connectivity_schema": sql.Identifier(connectivity_schema),
            "connectivity_source_col": sql.Identifier(connectivity.source_column),
            "connectivity_target_col": sql.Identifier(connectivity.target_column),
            "connectivity_max_distance": sql.Literal(connectivity.max_distance),
            "connectivity_max_detour": sql.Literal(connectivity.max_detour),
            "connectivity_detour_agnostic_threshold": sql.Literal(connectivity.detour_agnostic_threshold),
            "connectivity_max_stress": sql.Literal(connectivity.max_stress)
        }

        return subs
