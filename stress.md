# pyBNA Traffic Stress

pyBNA includes a module with methods for calculating traffic stress on the road network. For more information on the concept of Traffic Stress see [What is BNA?](what_is_bna.md#level-of-traffic-stress)

# Getting Started

Traffic Stress is done using the Stress class. This can be imported with
```
from pybna import Stress
```

From there, a Stress object must be instantiated. The Stress module operates
with the same configuration file setup as the rest of pyBNA.

```
stress = Stress(config="/path/to/config/file")
```

# Segment Stress

Traffic stress on segments can be calculated with

```
stress.segment_stress()
```

This calculates the segment-based traffic stress and saves the results in the
forward/backward segment stress column identified in the configuration file.

# Crossing Stress

Traffic stress for crossings can be calculated with

```
stress.crossing_stress()
```

This calculates the crossing stress and saves the results in the
forward/backward segment stress column identified in the configuration file.

# Configuration

Information about the data used in the traffic stress calculations, as well as
the assumptions applied in the absence of data, is given in the configuration
file. As with the other parts other configuration file, the goal is to provide
flexibility in the data used as inputs to Traffic Stress process.

### lookup_tables

```
    lookup_tables:
        shared: "generated.stress_shared"
        bike_lane: "generated.stress_bike_lane"
        crossing: "generated.stress_crossing"
```

These are the tables that contain thresholds for the Traffic Stress scores. It's
a computer-readable version of the stress lookup tables available
[here](https://s3.amazonaws.com/pfb-public-documents/Charts.for.LTS.Definitions.2017-0713.xlsx).

These tables are generally created by pyBNA when you instantiate the Stress
object. You could adjust thresholds as desired by modifying entries in the
database table.

Entry | Description | Required
:--- | :--- | :---:
shared | Name of the table for shared roadways | X
bike_lane | Name of the table for bike lanes | X
crossing | Name of the table for crossings | X

### segment

```
    segment:
        forward:
            lanes: ft_lanes
            oneway: {name: "one_way", val: "ft"}
            aadt: aadt
            centerline: {name: "centerline", val: 1}
            speed: speed_limit
            low_parking: {name: "low_parking", val: TRUE}
            parking: {name: "ft_park", val: TRUE}
            width: width
            parking_width: ft_park_width
            bike_infra: {name: "ft_bike_infra", lane: "lane", buffered_lane: "buffered_lane", track: "track", path: "path"}
            bike_lane_width: ft_bike_infra_width
        backward:
            lanes: tf_lanes
            oneway: {name: "one_way", val: "tf"}
            aadt: aadt
            centerline: {name: "centerline", val: 1}
            speed: speed_limit
            low_parking: {name: "low_parking", val: TRUE}
            parking: {name: "tf_park", val: TRUE}
            width: width
            parking_width: tf_park_width
            bike_infra: {name: "tf_bike_infra", lane: "lane", buffered_lane: "buffered_lane", track: "track", path: "path"}
            bike_lane_width: tf_bike_infra_width
```

These are attributes in the table representing inputs to the segment stress.
There are entries for both directions of travel (forward/backward). There's no
problem with pointing to the same column for both directions as long as that's
consistent with the conditions on the ground.

Entry | Description | Required
:--- | :--- | :---:
lanes | Number of lanes in this direction of travel<br>(N.B. if your data has _total_ number of lanes you'll need to divide into directional lanes) | X
oneway | Indicator of one-way travel | X
aadt | Average traffic count |
centerline | Whether this segment has a striped centerline |
speed | Operating speed |
low_parking | Indicator of parking usage |
parking | Is parking allowed? |
width | Roadway width |
parking_width | Width of the parking lane |
bike_infra | Type of bike infrastructure | X
bike_lane_width | Width of bike lane |

Note that many of these attributes are not necessary to complete the Traffic
Stress calculation. In the absence of data in the roadway layer, you can define
[assumptions](stress.md#assumptions) that will be applied.

`oneway`

Entry | Description | Required
:--- | :--- | :---:
name | Name of the attribute holding one way designation | X
val | Value indicating one way in this direction | X

N.B. If there are values in the one-way column that don't match either the
forward or backward value the road is considered to be two-way.

`centerline`

Entry | Description | Required
:--- | :--- | :---:
name | Name of the attribute holding centerline information | X
val | Value indicating a centerline is present | X

`low_parking`

Some of the Traffic Stress thresholds are dependent on how heavily parking is
used on the segment. A road with low parking usage leaves additional operating
space for riding and doesn't have the same level of complexity as a road with
lots of parked cars. This attribute is a flag for low-parking segments that
allows them to be treated accordingly in the calculations.

Entry | Description | Required
:--- | :--- | :---:
name | Name of the attribute holding the low_parking data | X
val | Value indicating this segment is low-parking | X

`bike_infra`

The BNA considers four types of bike facility:

1. Bike lane
2. Buffered lane
3. Cycle track
4. Off-street trail

Entry | Description | Required
:--- | :--- | :---:
name | Name of the attribute holding bikeway data | X
lane | Value for bike lane | X
buffered_lane | Value for buffered lane | X
track | Value for cycle track | X
path | Value for off-street trail | X

### crossing

```
    crossing:
        intersection_tolerance: 5
        control:
            # control is required
            table: neighborhood_ways_intersections
            geom: geom
            column:
                name: "control"
                four_way_stop: "stop"
                signal: "signal"
                rrfb: "rrfb"
                hawk: "hawk"
        island:
            table: neighborhood_ways_intersections
            geom: geom
            column:
                name: "island"
                value: True
        forward:
            # lanes: ft_cross_lanes
            # speed: ft_speed_limit
            # control: ft_control
            # island: ft_island
        backward:
            # lanes: tf_cross_lanes
            # speed: tf_speed_limit
            # control: tf_control
            # island: tf_island
```

These are attributes and tables representing inputs to the crossing stress.

#### intersection_tolerance

Tolerance distance for finding a crossing

#### control

Information about intersection control.

Entry | Description | Required
:--- | :--- | :---:
table | Table holding intersection controls | X
geom | Geometry column |
column | The column that holds control information | X

`column`

Entry | Description | Required
:--- | :--- | :---:
name | Name of the column | X
four_way_stop | Value indicating a four-way stop | X
signal | Value indicating a full signal | X
rrfb | Value indicating an RRFB | X
hawk | Value indicating a HAWK signal | X

#### island

Information for crossing islands

Entry | Description | Required
:--- | :--- | :---:
table | Table holding island locations | X
geom | Geometry column |
column | The column that holds island information | X

`column`

Entry | Description | Required
:--- | :--- | :---:
name | Name of the column | X
value | Value indicating an island | X

#### forward and backward

If data are available to indicate crossing features on the segments themselves
they can be provided here. These are not necessary as pyBNA will infer that
information by comparing each segment to the segments that intersect it.

Entry | Description | Required
:--- | :--- | :---:
lanes | Number of lanes to be crossed |
speed | Operating speed of the crossing road |
control | Traffic control at the intersection |
island | Presence of a crossing island at the intersection |

### assumptions

Assumptions are applied any time data are missing. In some cases, this could be
an entire class of information (e.g. if you don't have any ADT data at all and
thus left it out of the `segment` section completely) or it could be applied
only to features where data is not present (e.g. you have ADT for some locations
but not all). Assumptions are divided into segment and crossing sections.

#### segment

Segment assumptions are defined by where/value statements as in the following:

```
lanes:
    - where: "functional_class IN ('primary','secondary')"
      val: 2
    - where: "functional_class IN ('tertiary')"
      val: 1
    - else: 0
```

pyBNA would interpret this by assigning 2 lanes to any segments that match the
first `where` clause, 1 lane to any segments that match the second `where`, and
0 lanes (no centerline) to anything else. Statements are evaluated in order so a
match in an earlier `where` would be honored over any other potential matches. A
`*` can be used to match everything. This is helpful for defining a universal
assumption.

Assumptions can be defined for any attributes described in the
[segment](stress.md#segment) section above except for `oneway` and `bike_infra`
attributes.

#### crossing

Crossing assumptions identify low-stress crossings from meetings of roads as in
the following:

```
    - where: "functional_class = 'primary'"
      meets: "*"
    - where: "functional_class = 'secondary'"
      meets: "*"
    - where: "functional_class = 'tertiary'"
      meets: "functional_class IN ('residential','unclassified')"
```

pyBNA would interpret this by applying a low-stress crossing score on any
primary roads that cross any other road (i.e. any primary road is assumed to
have a low-stress crossing). The same assumption would be applied to any
secondary road (presumably a secondary road crossing a primary road would have a
traffic signal). Lastly, any tertiary road that crosses a residential or
unclassified road would be assumed to be a low-stress crossing.
