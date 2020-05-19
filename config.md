# pyBNA Configuration

Most of the flexibility in pyBNA is managed using a configuration file. This
file is passed to the pyBNA object as an argument when it is created and tells
pyBNA important things about the input data and assumptions used in the process. An example configuration file using standard BNA defaults can be found [here](pybna/config.yaml).

`srid` identifies the map projection to use for the data

`units` identifies whether speed limits and widths in the road data are given in
imperial (`mi`) or metric (`km`) units. Note that spatial numbers given
elsewhere in the configuration file are in the units associated with the `srid`.
This means, for example, you could be using imperial units (mph and feet) in your
roadway data, but the distance for `max_distance` could be given in meters if
your projection uses meters.

The first grouped section tells pyBNA how to connect to the database
```
db:
    user: "gis"
    password: "gis"
    host: "localhost"
    dbname: "bna"
```

The second section, under the `bna` root, contains all the references to data and assumptions. Entries that aren't required can often be inferred by pyBNA, however, ambiguities in your data may result in an error or unusual results. Schemas can be qualified in the table name.

### boundary

```
    boundary:
        table: "received.neighborhood_boundary"
        geom: "geom"
```

The boundary tells pyBNA the extents of the area of analysis.

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
geom | Name of the geometry column |

### blocks

```
    blocks:
        table: "generated.neighborhood_census_blocks"
        uid: "blockid10"
        population: "pop10"
        geom: "geom"
        roads_tolerance: 15
        min_road_length: 30  
```

This section tells pyBNA about the table of population areas used as the unit of analysis for the BNA. pyBNA was originally written to use US Census data so "block" refers to the US Census block geography, but any area can be used as long as it contains information about population. Be careful using large areas, though. Large areas are likely produce unexpected results because pyBNA assumes any destinations _within_ a block are accessible to all of its people. Larger areas may not conform with this assumption.

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
uid | Name of the table's primary key |
population | Name of the population attribute | X
geom | Name of the geometry column |
roads_tolerance | Tolerance used when searching for roads that are associated with a block | X
min_road_length | Length a roadway must share with a block area in order to be considered associated with that block | X

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
            cluster_distance: 1
        edges:
            table: "neighborhood_ways_net_link"
            source_column: "source_vert"
            target_column: "target_vert"
            stress_column: "link_stress"
            cost_column: "link_cost"
            uid: link_id
        nodes:
            table: "neighborhood_ways_net_vert"
            uid: vert_id
```


The network settings tell pyBNA what your road dataset looks like and designate table names to use for building a routable network.

#### roads

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
geom | Name of the geometry column |
uid | Primary key |
source_column | ID of the intersection at the start of the line | X
target_column | ID of the intersection at the end of the line | X

`oneway`

Entry | Description | Required
:--- | :--- | :---:
name | Name of the attribute holding one way designation | X
forward | Value indicating one way in the forward direction | X
backward | Value indicating one way in the backward direction | X

N.B. If there are values in the one-way column that don't match either the
forward or backward value the road is considered to be two-way.

`segment stress`

Entry | Description | Required
:--- | :--- | :---:
forward | Segment LTS rating in the forward direction | X
backward | Segment LTS rating in the backward direction | X

`crossing stress`

Entry | Description | Required
:--- | :--- | :---:
forward | Crossing LTS rating in the forward direction | X
backward | Crossing LTS rating in the backward direction | X

#### intersections

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
geom | Name of the geometry column |
uid | Primary key |
cluster_distance | Tolerance for grouping nearby vertices | X

#### edges

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
source_column | Name of the attribute indicating the source node | X
target_column | Name of the attribute indicating the target node | X
stress_column | Name of the attribute indicating the LTS on the edge | X
cost_column | Name of the attribute indicating the cost of the edge | X
uid | Primary key | X

#### nodes

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
uid | Primary key | X

### connectivity

The connectivity settings set assumptions for testing connectivity and determine
the location for saving the connectivity matrix, which stores block-to-block
connectivity information.

Entry | Description | Required
:--- | :--- | :---:
table | Name of the table | X
source_column | Name of the attribute holding the ID of the source block | X
target_column | Name of the attribute holding the ID of the target block | X
max_distance | The maximum distance to search for possible block connections | X
max_detour | The maximum percentage to exceed high-stress distance and still be considered connected on the low-stress network (given as a whole number out of 100) | X
detour_agnostic_threshold | Distance under which the % detour is ignored. As long as a low-stress connection is under this threshold it is counted even if it is significantly longer than the high-stress alternative | X
max_stress | The maximum LTS score to consider for low-stress connectivity | X

### destinations

Destinations are given as a list (denoted by a dash at the beginning of each
entry). Destinations can include subcategories, which allows for nesting
destinations. For example, the _Opportunity_ score has subcategories for
_Employment_ and _Schools_. There's no built-in limit to the nesting levels for
destinations, although it probably doesn't make sense to nest beyond one level.

Categories that have subcategories use the `subcats` entry

```
      - name: opportunity
          weight: 20
---->     subcats:
            - name: employment
              weight: 35
              table: neighborhood_census_block_jobs
              ...
              ...
```

Entry | Description | Required
:--- | :--- | :---:
name | Category name | X
weight | Category weight | X
table | Name of the table | X
method | Either `count` or `percentage` | X
datafield | Attribute used to calculate percentage<br>(only used for `percentage` method) | depends
maxpoints | Total possible points for this category | X
breaks | List of breaks at which points are awarded | X
uid | Primary key<br>(for `percentage` method this must match the primary key of the census blocks table) |
geom | Name of the geometry column |
osm_tags_query | See below for additional documentation |

The category weight is relative to its peers. In other words, the weights of
subcategories are unrelated to the weight of their parent categories.

Breaks are defined values at which the specified number of points are awarded.
Any results that sit between the given breaks are pro-rated based on the
surrounding break values.

The `percentage` method assigns points cumulatively based on the ratio of the
given attribute within low-stress access divided by the high-stress access. For
example, if a block has high-stress access to 100 people and low-stress access
to only 30 people, its percentage is 0.3 or 30%.

`osm_tags_query` allows you to define an OSM query for extracting these
destinations directly from OSM. The query follows the [Overpass
API](https://wiki.openstreetmap.org/wiki/Overpass_API) format. Queries can be
given as a list to combine multiple queries. For example, the query for doctor
offices would use the following format:

```
osm_tags_query:
    - "['amenity'='doctors']"
    - "['amenity'='doctor']"
    - "['amenity'='clinic']"
```

The queries are applied as part of the `import_osm_destinations` method in
pybna's [Importer](import.md#Destinations) class.
