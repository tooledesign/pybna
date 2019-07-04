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

_URL option:_

You can point the BNA to a URL to download a file with:

```
i.import_census_blocks(url="https://valid/url/to/blocks/file")
```

_FIPS option:_

If you supply the FIPS code for the state you're working in the BNA will
download the blocks automatically for you:

```
i.import_census_blocks(fips=16)  # for working in Idaho (FIPS 16)
```

# Census Jobs

This is under construction.

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
your own instructions for extracting OSM destinations using the `tags` option.
To do this, you'll need to create a dictionary of table names and OSM tags that
mimics the default baked into the code.

Importing destinations can be done with:
```
i.import_osm_destinations()
```

At present, OSM's Overpass API is used for obtaining destinations. In other
words, you cannot use a downloaded OSM extract.
