###################################################################
# The Destination class stores a BNA destination for use in pyBNA.
###################################################################
import psycopg2
from psycopg2 import sql
import pandas as pd


class Destinations:
    def __init__(self,category,conn,table,id_col,blocks_col=None,verbose=False):
        """Sets up a new category of BNA destinations and retrieves data from
        the given db table

        category -- destination category type
        table -- the db table where data is stored
        blocks_col -- the column name where census block ids are stored. if None
            uses blockid10, the BNA default.

        return: None
        """
        self.verbose = verbose
        self.category = category
        self.ls_population = None
        self.hs_population = None
        self.destinations = list()

        if blocks_col is None:
            blocks_col = 'blockid10'

        self.destination_blocks = set(self._retrieve(conn,table,id_col,blocks_col))


    def __unicode__(self):
        n = len(self.destinations)
        return u'%s: %i destinations' % (self.category, n)


    def __repr__(self):
        n = len(self.destinations)
        return r'%s: %i destinations' % (self.category, n)


    def _retrieve(self,conn,table,id_col,blocks_col):
        """Retrieve destinations from the database and store them in
        this class' list of destinations

        return: list of all census block ids that contain a destination in this category
        """
        if self.verbose:
            print('Getting destinations for %s from %s' % (self.category,table))
        cur = conn.cursor()

        cur.execute(
            sql.SQL('select {}, {} from {};')
                .format(
                    sql.Identifier(id_col),
                    sql.Identifier(blocks_col),
                    sql.Identifier(table)
                )
        )

        if self.verbose:
            print(cur.query)

        all_blocks = set()

        for row in cur:
            if isinstance(row[1], list):
                blocks = set(row[1])
            else:
                blocks = set([row[1]])
            self.destinations.append({
                'id': row[0],
                'blocks': blocks
            })

            all_blocks.update(blocks)

        return all_blocks

    def set_population(self,blocks,connected_blocks):
        pass
        # self.ls_population =
        # self.hs_population =
