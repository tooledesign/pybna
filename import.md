# pyBNA Data Import

pyBNA includes a module with methods for importing data from publicly-available
sources. These sources are:

Source        | Usage
--------------|------------------
US Census     | Population; Employment
OpenStreetMap | Streets and trails; Destinations

# Getting Started

Data imports are done using the Importer class. This can be imported with
```
from pybna import Importer
```

From there, an Importer object must be instantiated. There are two ways to tell
Importer what it needs to connect to your database. The first way is to provide
a configuration file (see [config instructions here](config.md)). The second way
is to provide connection details explicitly.

_Config option:_
```
i = Importer(config="/path/to/config/file")
```

_Explicit details option:_
```
i = Importer(host="myhost" db_name="mydb" user="myuser" password="mypassword")
```

Any of the connection parameters in the configuration file can be overwritten
with explicit arguments. E.g.
```
i = Importer(config="/path/to/config/file",user="myuser")
```

# Defining a Study Area Boundary

The BNA uses a study area boundary to limit the geography under consideration.
If you provided a config file at startup, this boundary should have been defined
in the config file. You can import the boundary into your database with:
```
i.import_boundary(fpath="/path/to/my/boundary/file")
```

If you want to override the config or if you started Importer without a config
file, you can specify a table name with the `table` option.

The boundary file is used in all other import methods for filtering data before
it is uploaded to your database. If you started Importer without a config file,
you will need to provide a boundary with the `boundary_file` option for each of
the other import methods.

# Census Blocks

Census blocks are the standard unit of analysis for the BNA. By default, the BNA
uses Census Blocks as defined in the 2010 US Census. When blocks are imported
they are automatically clipped to the study area. Blocks can be obtained in one
of three ways: path to a file, URL, or US state FIPS code.

It is entirely possible to use blocks that are totally unrelated to the US
Census. This is necessary for e.g. running BNA outside of the United States. The
only items necessary to the BNA are:

* Geographic boundaries
* A unique identifier
* Population data

Blocks will be uploaded to your database at the location specified in your
config file, unless you provide an alternate table name. If you started the
Importer without a config file and don't explicitly specify a table, they will
be uploaded to the default location (`generated.neighborhood_census_blocks`).

The import also requires a study area boundary. If this is provided in your
config file it will be used. If not, you'll need to specify one with the
`boundary_file` option.

_File path option:_

If you have already downloaded a file to use for blocks, you can point to this
with:

```
i.import_census_blocks(fpath="/path/to/my/blocks/file")
```

We have found that the default import from the Census can take some time as it
is necessary to download the entire state's dataset, load it in memory, and then
filter to a smaller area. If you prefer not to wait, it is usually faster to
download the state dataset yourself, delete census blocks outside of the
vicinity of your study area, and then point the Importer to the saved shapefile
with this `fpath` option.

_URL option:_

You can point the BNA to a URL to download a file with:

```
i.import_census_blocks(url="https://valid/url/to/blocks/file")
```

_FIPS option:_

If you supply the FIPS code for the state you're working in pyBNA will
download the blocks automatically for you:

```
i.import_census_blocks(fips=16)  # e.g. for working in Idaho (FIPS 16)
```

# Census Jobs

By default, jobs data come from the Longitudinal Employer-Household Dynamics
(LEHD) dataset produced by the US Census. As with Census Blocks, the jobs
dataset does not have to be related to US Census data. Supplying your own jobs
data is easy. pyBNA has the following requirements for user-supplied jobs data:

* A unique identifier that corresponds to the ID associated with a block
* A number of jobs specific to that block

Jobs importing is easily done by specifying the table to create and the US state
to pull jobs data for:

```
i.import_census_jobs("myschema.myjobstable",state="ID")
```

The state argument should be a two-letter abbreviation of the state as used by
the US Census. If you have already downloaded the LEHD datasets you can point to
the "main" and "aux" files with the `fpath_main` and `fpath_aux` arguments.

The table created by this statement should correspond to the jobs table
identified in your config file as a destination type. By default, this is the
"employment" category in the config file and the table is named
`neighborhood_census_block_jobs`.

If you're supplying your own file for jobs data, you should load it into the
database manually (ensuring to update your config file accordingly).

# OSM Road Network

OpenStreetMap is the default source for road network and off-street trail data
for the BNA. The OSM network is already topologically correct and ready for
computer routing, including for off-street trails. This is a huge advantage
over many municipal road network datasets that often don't include trails and
sometimes don't enforce basic network topology.

The import is as easy as:
```
i.import_osm_network()
```

As with other import methods, the import will use your config file (if you
provided one) or BNA defaults (if you didn't), unless you explicitly provide a
table name. A boundary file is also required for this import unless one is
given in your config file.

If you already downloaded an OSM extract locally you can refer to it instead of
pulling data over the network with the `osm_file` option.

# Destinations

Destination data also comes from OpenStreetMap by default. The Importer uses the
default destination categories and definitions for the BNA, but you can provide
your own instructions for extracting OSM destinations using the
`destination_tags` option. To do this, you'll need to create a dictionary of
table names and OSM tags that mimics the default baked into the code.

Importing destinations can be done with:
```
i.import_osm_destinations()
```

If you downloaded an OSM extract you can point to this with the `osm_file`
option. Please note pyBNA will look for destination matches on the entire OSM
file without filtering to your study area boundary. This won't affect your
analysis, but it could take an extremely long time if you downloaded an OSM
extract that is significantly larger than your study area. We suggest clipping
your extract before using it. There are many excellent tools available to do
this.
