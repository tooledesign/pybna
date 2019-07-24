# pyBNA

## Introduction

Python module to implement BNA logic on a PostGIS database. Uses a configuration
file for customizability. The current implementation requires a user to run the
old-style BNA on the command line to import datasets. This library can build the
routeable network, process the connectivity routing, and calculate scores.

## Requirements

pyBNA is tested with Python 2.7. The following libraries are required:
- psycopg2
- tqdm
- pyyaml
- geopandas
- munch
- overpass

You can install these via pip:
```
pip install psycopg2 tqdm pyyaml geopandas munch overpass
```

The imposm library requires some additional packages to be installed. Consult the imposm documentation for details, but as of this writing you can handle the dependencies by running the following on a recent Ubuntu install
```
sudo apt install build-essential python-devel protobuf-compiler libprotobuf-dev
```

## tl;dr (Simple run)

The most simple BNA run, using stock datasets and no customization, can be
completed in a few easy steps. The following assumes you already have a database
running named "bna" on the local machine.

```
import pybna

# imports
i = pybna.Importer()
i.import_boundary('/path/to/your/boundary/file')
i.import_census_blocks(fips=16)
i.import_census_jobs("received.neighborhood_census_block_jobs",state="ID")
i.import_osm_network()
i.import_osm_destinations()

# stress
s = pybna.Stress()
s.segment_stress()
s.crossing_stress()

# connectivity
b = pybna.pyBNA()
b.calculate_connectivity()
b.score_destinations()
```

## Importing data

pyBNA includes a workflow to import data from publicly available sources (for
the United States, at least). Automatic import of demographic data relies on US
Census 2010 data. Street network and bicycle facility data is imported from
OpenStreetMap.

For more guidance on the import process, see our [import instructions](import.md).

## Getting started

First, import pybna and create a pyBNA object by pointing it to the config file.
```
import pybna
bna = pybna.pyBNA(config="/home/spencer/dev/napa/bna/bna_vine_config.yaml")
```

Next, you can calculate the connectivity with
```
bna.calculate_connectivity()
```

Lastly, you can generate block-level scores with
```
b.score_destinations("my_results_table")
```

## Configuration file

Most options in pyBNA are managed using a configuration file. This file is
passed as an argument when creating the pyBNA object and tells pyBNA important
things about your data and the assumptions you want to make in the analysis. The configuration file is written using [YAML](http://yaml.org/start.html).

There's more information about the configuration file [here](config.md)

## Travel sheds

Once you've completed the connectivity analysis, you can develop a low/high stress travel shed for any census block with
```
bna.travel_sheds([list, of, block, ids, here], my_travel_shed_table)
```
