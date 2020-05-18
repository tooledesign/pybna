###################################################################
# The Config class houses methods for dealing with the config file
###################################################################
import os
import collections
from psycopg2 import sql
from munch import Munch
from .dbutils import DBUtils
from .core import FORWARD_DIRECTION
from .core import BACKWARD_DIRECTION


class Conf(DBUtils):
    """pyBNA configuration class"""

    def __init__(self):
        DBUtils.__init__(self,"")
        self.segment_subs = None


    def parse_config(self,config):
        """
        Reads through the giant dictionary loaded from YAML and converts into
        munches that can be accessed with dot-notation

        Parameters
        ----------
        config : dict
            a dictionary of configuration options

        returns:
        Munch
        """
        if isinstance(config, collections.Mapping):
            for key, value in config.items():
                config[key] = self.parse_config(value)
            return Munch(config)
        return config


    def make_bna_substitutions(self, config):
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
        boundary_schema, boundary_table = self.parse_table_name(boundary.table)
        if boundary_schema is None:
            try:
                boundary_schema = self.get_schema(boundary_table)
            except:
                boundary_schema = default_schema
        if "geom" in boundary:
            boundary_geom = boundary.geom
        else:
            boundary_geom = "geom"

        # blocks
        blocks_schema, blocks_table = self.parse_table_name(blocks.table)
        if blocks_schema is None:
            try:
                blocks_schema = self.get_schema(blocks_table)
            except:
                blocks_schema = default_schema
        if "uid" in blocks:
            blocks_id_col = blocks.uid
        else:
            blocks_id_col = self.get_pkid_col(blocks_table,blocks_schema)
        if self.table_exists(blocks_table,blocks_schema):
            blocks_id_type = self.get_column_type(blocks_table,blocks_id_col,schema=blocks_schema)
        else:
            blocks_id_type = "text"
        if "geom" in blocks:
            blocks_geom_col = blocks.geom
        else:
            blocks_geom_col = "geom"

        # roads
        roads_schema, roads_table = self.parse_table_name(network.roads.table)
        if roads_schema is None:
            try:
                roads_schema = self.get_schema(roads_table)
            except:
                roads_schema = default_schema
        if "uid" in network.roads:
            roads_id_col = network.roads.uid
        else:
            roads_id_col = self.get_pkid_col(roads_table,roads_schema)
        if "geom" in network.roads:
            roads_geom_col = network.roads.geom
        else:
            roads_geom_col = "geom"

        # intersections
        ints_schema, ints_table = self.parse_table_name(network.intersections.table)
        if ints_schema is None:
            try:
                ints_schema = self.get_schema(ints_table)
            except:
                ints_schema = default_schema
        if "uid" in network.intersections:
            ints_id_col = network.intersections.uid
        else:
            ints_id_col = self.get_pkid_col(ints_table,ints_schema)
        if "geom" in network.intersections:
            ints_geom_col = network.intersections.geom
        else:
            ints_geom_col = "geom"

        # edges
        edges_schema, edges_table = self.parse_table_name(network.edges.table)
        if edges_schema is None:
            if self.table_exists(edges_table):
                edges_schema = self.get_schema(edges_table)
            else:
                edges_schema = roads_schema
        if "uid" in network.edges:
            edges_id_col = network.edges.uid
        elif self.table_exists(edges_table,edges_schema):
            edges_id_col = self.get_pkid_col(edges_table,edges_schema)
        else:
            edges_id_col = "edge_id"
        if "geom" in network.edges:
            edges_geom_col = network.edges.geom
        elif self.table_exists(edges_table,edges_schema):
            edges_geom_col = "geom"
        else:
            edges_geom_col = "geom"

        # nodes
        nodes_schema, nodes_table = self.parse_table_name(network.nodes.table)
        if nodes_schema is None:
            if self.table_exists(nodes_table):
                nodes_schema = self.get_schema(nodes_table)
            else:
                nodes_schema = roads_schema
        if "uid" in network.nodes:
            nodes_id_col = network.nodes.uid
        elif self.table_exists(nodes_table,nodes_schema):
            nodes_id_col = self.get_pkid_col(nodes_table,nodes_schema)
        else:
            nodes_id_col = "node_id"
        if "geom" in network.nodes:
            nodes_geom_col = network.nodes.geom
        elif self.table_exists(nodes_table,nodes_schema):
            nodes_geom_col = "geom"
        else:
            nodes_geom_col = "geom"

        # connectivity
        connectivity_schema, connectivity_table = self.parse_table_name(connectivity.table)
        if connectivity_schema is None:
            connectivity_schema = blocks_schema

        # srid
        if "srid" in config:
            srid = config.srid
        else:
            srid = self.get_srid(blocks_table,schema=blocks_schema)

        subs = {
            "srid": sql.Literal(srid),
            "boundary_table": sql.Identifier(boundary_table),
            "boundary_schema": sql.Identifier(boundary_schema),
            "boundary_geom_col": sql.Identifier(boundary_geom),
            "blocks_table": sql.Identifier(blocks_table),
            "blocks_schema": sql.Identifier(blocks_schema),
            "blocks_id_col": sql.Identifier(blocks_id_col),
            "blocks_id_type": sql.SQL(blocks_id_type),
            "blocks_geom_col": sql.Identifier(blocks_geom_col),
            "blocks_population_col": sql.Identifier(blocks.population),
            "blocks_roads_tolerance": sql.Literal(blocks.roads_tolerance),
            "blocks_min_road_length": sql.Literal(blocks.min_road_length),
            "roads_table": sql.Identifier(roads_table),
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
            "ints_table": sql.Identifier(ints_table),
            "ints_schema": sql.Identifier(ints_schema),
            "ints_id_col": sql.Identifier(ints_id_col),
            "ints_geom_col": sql.Identifier(ints_geom_col),
            "ints_cluster_distance": sql.Literal(network.intersections.cluster_distance),
            "edges_table": sql.Identifier(edges_table),
            "edges_schema": sql.Identifier(edges_schema),
            "edges_id_col": sql.Identifier(edges_id_col),
            "edges_geom_col": sql.Identifier(edges_geom_col),
            "edges_source_col": sql.Identifier(network.edges.source_column),
            "edges_target_col": sql.Identifier(network.edges.target_column),
            "edges_stress_col": sql.Identifier(network.edges.stress_column),
            "edges_cost_col": sql.Identifier(network.edges.cost_column),
            "nodes_table": sql.Identifier(nodes_table),
            "nodes_schema": sql.Identifier(nodes_schema),
            "nodes_id_col": sql.Identifier(nodes_id_col),
            "nodes_geom_col": sql.Identifier(nodes_geom_col),
            "connectivity_table": sql.Identifier(connectivity_table),
            "connectivity_schema": sql.Identifier(connectivity_schema),
            "connectivity_source_col": sql.Identifier(connectivity.source_column),
            "connectivity_target_col": sql.Identifier(connectivity.target_column),
            "connectivity_max_distance": sql.Literal(connectivity.max_distance),
            "connectivity_max_detour": sql.Literal(connectivity.max_detour),
            "connectivity_detour_agnostic_threshold": sql.Literal(connectivity.detour_agnostic_threshold),
            "connectivity_max_stress": sql.Literal(connectivity.max_stress)
        }

        return subs


    def _build_segment_sql_substitutions(self,direction):
        """
        Builds commonly-shared segment-oriented SQL substitutions from the
        entries in the config file

        Parameters
        ----------
        direction : str
            the direction to generate substitutions for

        returns:
        a dictionary holding SQL objects
        """
        assumptions = self.config.stress.assumptions.segment
        settings = self.config.stress.segment[direction]

        # check required inputs
        if "lanes" not in settings and "lanes" not in assumptions:
            raise ValueError("Lane data is required as either an attribute or an assumption")
        if "speed" not in settings and "speed" not in assumptions:
            raise ValueError("Speed data is required as either an attribute or an assumption")
        if "aadt" not in settings and "aadt" not in assumptions:
            raise ValueError("AADT data is required as either an attribute or an assumption")

        # lanes
        if "lanes" in settings:
            lanes = sql.Identifier(settings["lanes"])
        else:
            lanes = sql.SQL("NULL")
        if "lanes" in assumptions:
            assumed_lanes = self._build_case(assumptions["lanes"])
        else:
            assumed_lanes = sql.SQL("NULL")

        # centerline
        if "centerline" in settings:
            centerline_column = sql.Identifier(settings["centerline"]["name"])
            centerline_value = sql.Literal(settings["centerline"]["val"])
        else:
            centerline_column = sql.SQL("NULL")
            centerline_value = sql.SQL("NULL")
        centerline = sql.SQL("({}={})").format(centerline_column,centerline_value)
        if "centerline" in assumptions:
            assumed_centerline = self._build_case(assumptions["centerline"])
        else:
            assumed_centerline = sql.SQL("FALSE")

        # low_parking
        if "low_parking" in settings:
            low_parking_column = sql.Identifier(settings["low_parking"]["name"])
            low_parking_value = sql.Literal(settings["low_parking"]["val"])
        else:
            low_parking_column = sql.SQL("NULL")
            low_parking_value = sql.SQL("NULL")
        low_parking = sql.SQL("({}={})").format(low_parking_column,low_parking_value)
        if "low_parking" in assumptions:
            assumed_low_parking = self._build_case(assumptions["low_parking"])
        else:
            assumed_low_parking = sql.SQL("FALSE")

        # speed
        if "speed" in settings:
            speed = sql.Identifier(settings["speed"])
        else:
            speed = sql.SQL("NULL")
        if "speed" in assumptions:
            assumed_speed = self._build_case(assumptions["speed"])
        else:
            assumed_speed = sql.SQL("NULL")

        # width
        if "width" in settings:
            width = sql.Identifier(settings["width"])
        else:
            width = sql.SQL("NULL")
        if "width" in assumptions:
            assumed_width = self._build_case(assumptions["width"])
        else:
            assumed_width = sql.SQL("NULL")

        # oneway
        if "oneway" in settings:
            oneway_column = sql.Identifier(settings["oneway"]["name"])
            oneway_value = sql.Literal(settings["oneway"]["val"])
            all_oneways = list()
            for d in [FORWARD_DIRECTION,BACKWARD_DIRECTION]:
                s = self.config.stress.segment[d]
                all_oneways.append(sql.Literal(s["oneway"]["val"]))
            all_oneway_values = sql.SQL(",").join(all_oneways)
        else:
            oneway_column = sql.Literal(0)
            oneway_value = sql.Literal(1)
            all_oneway_values = sql.Literal(1)
        twoway = sql.SQL("({} IS NULL OR {} NOT IN ({}))").format(
            oneway_column,
            oneway_column,
            all_oneway_values
        )
        oneway = sql.SQL("({}={})").format(oneway_column,oneway_value)

        # aadt
        if "aadt" in settings:
            aadt = sql.Identifier(settings["aadt"])
        else:
            aadt = sql.SQL("NULL")
        if "aadt" in assumptions:
            assumed_aadt = self._build_case(assumptions["aadt"])
        else:
            assumed_aadt = sql.SQL("NULL")

        # parking
        if "parking" in settings:
            parking_column = sql.Identifier(settings["parking"]["name"])
            parking_value = sql.Literal(settings["parking"]["val"])
        else:
            parking_column = sql.Literal(0)
            parking_value = sql.Literal(1)
        parking = sql.SQL("({}={})").format(parking_column,parking_value)
        if "parking" in assumptions:
            assumed_parking = self._build_case(assumptions["parking"])
        else:
            assumed_parking = sql.SQL("NULL")

        # parking_width
        if "parking_width" in settings:
            parking_width = sql.Identifier(settings["parking_width"])
        else:
            parking_width = sql.SQL("NULL")
        if "parking_width" in assumptions:
            assumed_parking_width = self._build_case(assumptions["parking_width"])
        else:
            assumed_parking_width = sql.SQL("NULL")

        # bike_lane_width
        if "bike_lane_width" in settings:
            bike_lane_width = sql.Identifier(settings["bike_lane_width"])
        else:
            bike_lane_width = sql.SQL("NULL")
        if "bike_lane_width" in assumptions:
            assumed_bike_lane_width = self._build_case(assumptions["bike_lane_width"])
        else:
            assumed_bike_lane_width = sql.SQL("NULL")

        # shared
        shared = sql.SQL("{c} IS NULL OR {c} NOT IN ({l},{bl},{t},{p})").format(
            c=sql.Identifier(settings["bike_infra"]["name"]),
            l=sql.Literal(settings["bike_infra"]["lane"]),
            bl=sql.Literal(settings["bike_infra"]["buffered_lane"]),
            t=sql.Literal(settings["bike_infra"]["track"]),
            p=sql.Literal(settings["bike_infra"]["path"])
        )

        # bike_lane
        bike_lane = sql.SQL("({} IN ({},{}))").format(
            sql.Identifier(settings["bike_infra"]["name"]),
            sql.Literal(settings["bike_infra"]["lane"]),
            sql.Literal(settings["bike_infra"]["buffered_lane"])
        )

        # track
        track = sql.SQL("({}={})").format(
            sql.Identifier(settings["bike_infra"]["name"]),
            sql.Literal(settings["bike_infra"]["track"])
        )

        # path
        path = sql.SQL("({}={})").format(
            sql.Identifier(settings["bike_infra"]["name"]),
            sql.Literal(settings["bike_infra"]["path"])
        )

        # other vals
        schema, table = self.parse_table_name(self.config.bna.network.roads.table)
        if schema is None:
            schema = self.get_schema(table)
        if "uid" in self.config.bna.network.roads:
            id_column = self.config.bna.network.roads.uid
        else:
            id_column = self.get_pkid_col(table,schema)
        if "geom" in self.config.bna.network.roads:
            geom = self.config.bna.network.roads.geom
        else:
            geom = self._get_geom_column(table,schema)
        shared_lts_schema, shared_lts_table = self.parse_table_name(self.config.stress.lookup_tables.shared)
        if shared_lts_schema is None:
            shared_lts_schema = self.get_schema(shared_lts_table)
        bike_lane_lts_schema, bike_lane_lts_table = self.parse_table_name(self.config.stress.lookup_tables.bike_lane)
        if bike_lane_lts_schema is None:
            bike_lane_lts_schema = self.get_schema(bike_lane_lts_table)

        # set up substitutions
        subs = {
            "id_column": sql.Identifier(id_column),
            "segment_stress_forward": sql.Identifier(self.config.bna.network.roads.stress.segment.forward),
            "segment_stress_backward": sql.Identifier(self.config.bna.network.roads.stress.segment.backward),
            "lanes": lanes,
            "assumed_lanes": assumed_lanes,
            "centerline": centerline,
            "assumed_centerline": assumed_centerline,
            "low_parking": low_parking,
            "assumed_low_parking": assumed_low_parking,
            "speed": speed,
            "assumed_speed": assumed_speed,
            "width": width,
            "assumed_width": assumed_width,
            "aadt": aadt,
            "assumed_aadt": assumed_aadt,
            "parking": parking,
            "assumed_parking": assumed_parking,
            "parking_width": parking_width,
            "assumed_parking_width": assumed_parking_width,
            "bike_lane_width": bike_lane_width,
            "assumed_bike_lane_width": assumed_bike_lane_width,
            "in_schema": sql.Identifier(schema),
            "in_table": sql.Identifier(table),
            "geom": sql.Identifier(geom),
            "oneway": oneway,
            "twoway": twoway,
            "shared": shared,
            "bike_lane": bike_lane,
            "track": track,
            "path": path,
            "shared_lts_schema": sql.Identifier(shared_lts_schema),
            "shared_lts_table": sql.Identifier(shared_lts_table),
            "bike_lane_lts_schema": sql.Identifier(bike_lane_lts_schema),
            "bike_lane_lts_table": sql.Identifier(bike_lane_lts_table)
        }

        return subs


    def _build_crossing_sql_substitutions(self,direction):
        """
        Builds crossing SQL substitutions from the entries in the config
        file

        Parameters
        ----------
        direction : str
            the direction to generate substitutions for

        returns:
        a dictionary holding SQL objects
        """
        assumptions = self.config.stress.assumptions.crossing
        if self.config.stress.crossing[direction] is None:
            settings = dict()
        else:
            settings = self.config.stress.crossing[direction]

        # check required inputs
        if "intersection_tolerance" not in self.config.stress.crossing:
            raise ValueError("Intersection tolerance not specified in config")
        if "control" not in self.config.stress.crossing:
            raise ValueError("Control data not specified in config")

        intersection_tolerance = self.config.stress.crossing.intersection_tolerance

        # stress table
        schema, table = self.parse_table_name(self.config.bna.network.roads.table)
        if schema is None:
            schema = self.get_schema(table)
        if "uid" in self.config.bna.network.roads:
            id_column = self.config.bna.network.roads.uid
        else:
            id_column = self.get_pkid_col(table,schema)
        if "geom" in self.config.bna.network.roads:
            geom = self.config.bna.network.roads.geom
        else:
            geom = self._get_geom_column(table,schema)

        # control
        control_schema, control_table = self.parse_table_name(self.config.stress.crossing.control.table)
        if control_schema is None:
            control_schema = self.get_schema(control_table)
        if "geom" in self.config.stress.crossing.control:
            control_geom = self.config.stress.crossing.control.geom
        else:
            control_geom = self._get_geom_column(control_table,control_schema)
        control_column = self.config.stress.crossing.control.column.name
        four_way_stop = self.config.stress.crossing.control.column.four_way_stop
        signal = self.config.stress.crossing.control.column.signal
        rrfb = self.config.stress.crossing.control.column.rrfb
        hawk = self.config.stress.crossing.control.column.hawk

        # island
        island_schema, island_table = self.parse_table_name(self.config.stress.crossing.island.table)
        if island_schema is None:
            island_schema = self.get_schema(island_table)
        if "geom" in self.config.stress.crossing.island:
            island_geom = self.config.stress.crossing.island.geom
        else:
            island_geom = self._get_geom_column(island_table,island_schema)
        island_column = self.config.stress.crossing.island.column.name

        # directional_attribute_aggregation
        data_insert = self.read_sql_from_file(
            os.path.join(
                self.module_dir,
                "sql",
                "stress",
                "crossing",
                "directional_attributes.sql"
            )
        )
        directional_attributes = self.read_sql_from_file(
            os.path.join(
                self.module_dir,
                "sql",
                "stress",
                "crossing",
                "directional_attributes_table.sql"
            )
        )
        data_insert_query = sql.SQL("")
        for d in [FORWARD_DIRECTION,BACKWARD_DIRECTION]:
            data_insert_query += sql.SQL(data_insert).format(**self.segment_subs[d])
        data_insert_subs = self.segment_subs["forward"].copy()
        data_insert_subs["data_insert"] = data_insert_query
        directional_attribute_aggregation = sql.SQL(directional_attributes).format(**data_insert_subs)

        #
        # grab direct settings (if any are specified)
        #

        # lanes
        if "lanes" in settings:
            cross_lanes = sql.SQL("actual.") + sql.Identifier(settings["lanes"])
        else:
            cross_lanes = sql.SQL("NULL")

        # speed
        if "speed" in settings:
            cross_speed = sql.SQL("actual.") + sql.Identifier(settings["speed"])
        else:
            cross_speed = sql.SQL("NULL")

        # control
        if "control" in settings:
            cross_control = sql.SQL("actual.") + sql.Identifier(settings["control"])
        else:
            cross_control = sql.SQL("NULL")

        # island
        if "island" in settings:
            cross_island = sql.SQL("actual.") + sql.Identifier(settings["island"])
        else:
            cross_island = sql.SQL("NULL")

        # misc
        cross_lts_schema, cross_lts_table = self.parse_table_name(self.config.stress.lookup_tables.crossing)
        if cross_lts_schema is None:
            cross_lts_schema = self.get_schema(cross_lts_table)

        subs = {
            "directional_attribute_aggregation": directional_attribute_aggregation,
            "intersection_tolerance": sql.Literal(intersection_tolerance),
            "point": sql.Identifier("_".join([direction,"pt"])),
            "in_schema": sql.Identifier(schema),
            "in_table": sql.Identifier(table),
            "geom": sql.Identifier(geom),
            "id_column": sql.Identifier(id_column),
            "cross_stress_forward": sql.Identifier(self.config.bna.network.roads.stress.crossing.forward),
            "cross_stress_backward": sql.Identifier(self.config.bna.network.roads.stress.crossing.backward),
            "control_schema": sql.Identifier(control_schema),
            "control_table": sql.Identifier(control_table),
            "control_geom": sql.Identifier(control_geom),
            "control_column": sql.Identifier(control_column),
            "four_way_stop": sql.Literal(four_way_stop),
            "signal": sql.Literal(signal),
            "rrfb": sql.Literal(rrfb),
            "hawk": sql.Literal(hawk),
            "island_schema": sql.Identifier(island_schema),
            "island_table": sql.Identifier(island_table),
            "island_geom": sql.Identifier(island_geom),
            "island_column": sql.Identifier(island_column),
            "cross_lanes": cross_lanes,
            "cross_speed": cross_speed,
            "cross_control": cross_control,
            "cross_island": cross_island,
            "cross_lts_schema": sql.Identifier(cross_lts_schema),
            "cross_lts_table": sql.Identifier(cross_lts_table)
        }

        # control_assignment
        raw = self.read_sql_from_file(
            os.path.join(
                self.module_dir,
                "sql",
                "stress",
                "crossing",
                "control_assignment.sql"
            )
        )
        control_assignment = sql.SQL(raw).format(**subs)
        subs["control_assignment"] = control_assignment

        # island_assignment
        raw = self.read_sql_from_file(
            os.path.join(
                self.module_dir,
                "sql",
                "stress",
                "crossing",
                "island_assignment.sql"
            )
        )
        island_assignment = sql.SQL(raw).format(**subs)
        subs["island_assignment"] = island_assignment

        # priority_assignment
        raw = self.read_sql_from_file(
            os.path.join(
                self.module_dir,
                "sql",
                "stress",
                "crossing",
                "priority_assignment.sql"
            )
        )
        priority_assignment = sql.SQL("")
        if "priority" in assumptions:
            for i, w in enumerate(assumptions["priority"]):
                s = subs.copy()
                this_priority_table = "tmp_this_priority_" + str(i)
                this_where_test = w["where"]
                if this_where_test == "*":
                    this_where_test = "TRUE"
                that_priority_table = "tmp_that_priority_" + str(i)
                that_where_test = w["meets"]
                if that_where_test == "*":
                    that_where_test = "TRUE"
                priority_table = "tmp_priority_" + str(i)

                s["this_priority_table"] = sql.Identifier(this_priority_table)
                s["this_where_test"] = sql.SQL(this_where_test)
                s["that_priority_table"] = sql.Identifier(that_priority_table)
                s["that_where_test"] = sql.SQL(that_where_test)
                s["priority_table"] = sql.Identifier(priority_table)

                priority_assignment += sql.SQL(raw).format(**s)

        subs["priority_assignment"] = priority_assignment

        return subs


    def _build_case(self,vals,prefix=None):
        if prefix is None:
            prefix = sql.SQL("")
        else:
            prefix = sql.SQL(prefix + ".")
        case = sql.SQL(" CASE ")
        for val in vals:
            if "else" in val:
                pass
            elif "where" in val:
                if val["where"] == "*":
                    case += sql.SQL(" WHEN TRUE THEN ") + sql.Literal(val["val"])
                else:
                    case += sql.SQL(" WHEN ") + prefix + sql.SQL(val["where"]) + sql.SQL(" THEN ") + sql.Literal(val["val"])
            else:
                raise
        if "else" in vals[-1]:
            case += sql.SQL(" ELSE ") + sql.Literal(vals[-1]["else"])
        case += sql.SQL(" END ")
        return case


    def get_destination_tags(self):
        """
        Compiles a list of dictionaries that describe the tables and OSM tags
        for destinations in the config file.

        returns:
        a list of dictionaries
        """
        tags = []
        for destination in self.config.bna.destinations:
            tags.extend(self._get_destination_tags(destination))
        return tags


    def _get_destination_tags(self,node):
        """
        Helper method to be used in recursing destinations for OSM tags.
        Returns a list of dictionaries with the table and OSM tags for the given
        node in the destinations. If the node has subcats, appends these to the
        result.

        Parameters
        ----------
        node : dict
            A destination type in the config file

        returns:
        a list of dictionaries
        """
        tags = []
        if "subcats" in node:
            for destination in node["subcats"]:
                tags.extend(self._get_destination_tags(destination))

        if "table" in node and "osm_tags_query" in node:
            tags.append({"table":node["table"],"tags_query":node["osm_tags_query"]})

        return tags
