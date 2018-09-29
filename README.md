# pyBNA

## Introduction

Python module to implement BNA logic on a PostGIS database. Uses a configuration
file for customizability. The current implementation requires a user to run the
old-style BNA on the command line to import datasets. This library can build the
routeable network, process the connectivity routing, and calculate scores.

## Getting started

First, import pybna and create a pyBNA object by pointing it to the config file.
```
import pybna
bna = pybna.pyBNA(config="/home/spencer/dev/napa/bna/bna_vine_config.yaml")
```

If you don't already have tiles in your database you can create them with
```
bna.make_tiles()
```
It isn't necessary to create tiles, but it can be helpful for monitoring progress
on a larger area. In addition, larger areas can sometimes overwhelm the
computer, resulting in an out-of-memory error. Tiling reduces the amount of
memory needed to process the connectivity calculations and can alleviate that
problem.

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
