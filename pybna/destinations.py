###################################################################
# This is the class that manages destinations for the pyBNA object
###################################################################
import os
import yaml
import psycopg2
from psycopg2 import sql
import pandas as pd
import geopandas as gpd
import pickle
from tqdm import tqdm

from destinationcategory import DestinationCategory


class Destinations():
    """pyBNA Destinations class"""
    config = None
    verbose = None
    debug = None
    db = None              # reference to DBUtils class
    srid = None
    blocks = None






    def score_destinations(self,output_table,schema=None,destinations=None,overwrite=False):
        """
        Creates a new db table of scores for each block

        args:
        output_table -- table to create
        schema -- schema for the table. default is the schema where the census block table is stored.
        destinations -- list of destinations to calculate scores for. if None use all destinatinos
        overwrite -- overwrite a pre-existing table
        """
        if destinations is None:
            destinations = [i["category"] for i in self.destinations]
        if schema is None:
            schema = self.blocks.schema

        conn = self.db.get_db_connection()
        cur = conn.cursor()

        if overwrite:
            pass
            # drop table here

        for destination in self.destinations:
            if destination in destinations:
                tbl = ''.join(random.choice(string.ascii_lowercase) for _ in range(7))
                try:
                    cur.execute(destination._select_query(table,schema))
                except:
                    conn.rollback()

        conn.commit()
