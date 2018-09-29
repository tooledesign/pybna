# pyBNA Configuration

Most of the flexibility in pyBNA is managed using a configuration file. This
file is passed to the pyBNA object as an argument when it is created and tells
pyBNA important things about the input data and assumptions used in the process. An example configuration file using standard BNA defaults can be found [here](pybna/config.yaml).

The first section tells pyBNA how to connect to the database
```
db:
    user: "gis"
    password: "gis"
    host: "localhost"
    dbname: "bna"
```

The second section, under the **_bna_** root, contains all the references to data and assumptions. Entries that aren't required can often be inferred by pyBNA, however, ambiguities in your data may result in an error or unusual results.

### boundary

```
    boundary:
        table: "neighborhood_boundary"
        geom: "geom"
```

The boundary tells pyBNA the extents of the area of analysis.

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
schema | Name of the schema |
geom | Name of the geometry column |

### blocks

```
    blocks:
        table: "neighborhood_census_blocks"
        schema: "generated"
        id_column: "blockid10"
        population: "pop10"
        geom: "geom"
        roads_tolerance: 15
        min_road_length: 30  
```

This section tells pyBNA about the table of population areas used as the unit of analysis for the BNA. pyBNA was originally written to use US Census data so "block" refers to the US Census block geography, but any area can be used as long as it contains information about population. Be careful using large areas, though. Large areas are likely produce unexpected results because pyBNA assumes any destinations _within_ a block are accessible to all of its people. Larger areas may not conform with this assumption.

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
schema | Name of the schema |
id_column | Name of the table's primary key |
population | Name of the population attribute | X
geom | Name of the geometry column |
roads_tolerance | Tolerance used when searching for roads that are associated with a block | X
min_road_length | Length a roadway must share with a block area in order to be considered associated with that block | X

### tiles

```
    tiles:
        table: "grid_bna"
        geom: "geom"
```

Tiles are used to break the analysis up into manageable chunks. It's not necessary to provide tiles to pyBNA, but it allows you to track progress more accurately and may prevent failures due to high memory usage for larger analyses.

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
schema | Name of the schema |
geom | Name of the geometry column |

### network

```
    network:
        roads:
            table: "neighborhood_ways"
            geom: "geom"
            uid: "road_id"
            source_column: "intersection_from"
            target_column: "intersection_to"
            oneway: {name: "one_way", forward: "ft", backward: "tf"}
            stress:
                segment:
                    forward: ft_seg_stress
                    backward: tf_seg_stress
                crossing:
                    forward: ft_int_stress
                    backward: tf_int_stress
        intersections:
            table: "neighborhood_ways_intersections"
            geom: "geom"
            uid: "int_id"
        edges:
            table: "neighborhood_ways_net_link"
            source_column: "source_vert"
            target_column: "target_vert"
            stress_column: "link_stress"
            cost_column: "link_cost"
            id_column: link_id
        nodes:
            table: "neighborhood_ways_net_vert"
            id_column: vert_id
```


The network settings tell pyBNA what your road dataset looks like and designate table names to use for building a routable network.

#### roads

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
schema | Name of the schema |
geom | Name of the geometry column |
uid | Primary key |
source_column | ID of the intersection at the start of the line | X
target_column | ID of the intersection at the end of the line | X

oneway

Entry | Description | Required
:--- | :--- | :---:
name | Name of the attribute holding one way designation | X
forward | Value indicating one way in the forward direction | X
backward | Value indicating one way in the backward direction | X

segment stress

Entry | Description | Required
:--- | :--- | :---:
forward | Segment LTS rating in the forward direction | X
backward | Segment LTS rating in the backward direction | X

crossing stress

Entry | Description | Required
:--- | :--- | :---:
forward | Crossing LTS rating in the forward direction | X
backward | Crossing LTS rating in the backward direction | X

#### intersections

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
schema | Name of the schema |
geom | Name of the geometry column |
uid | Primary key |

#### edges

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
schema | Name of the schema |
source_column | Name of the attribute indicating the source node | X
target_column | Name of the attribute indicating the target node | X
stress_column | Name of the attribute indicating the LTS on the edge | X
cost_column | Name of the attribute indicating the cost of the edge | X
id_column | Primary key | X

#### nodes

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
schema | Name of the schema |
id_column | Primary key | X

### connectivity
