###################################################################
# The Destination class stores a BNA destination for use in pyBNA.
###################################################################
import psycopg2
from psycopg2 import sql


class Destinations:
    def __init__(self,category,conn,table,idCol,nameCol,blocksCol=None,verbose=False):
        """Sets up a new category of BNA destinations and retrieves data from
        the given db table

        category -- destination category type
        table -- the db table where data is stored
        nameCol -- the column name where destination names are stored
        blocksCol -- the column name where census block ids are stored. if None
            uses blockid10, the BNA default.

        return: None
        """
        self.verbose = verbose
        self.category = category
        self.lsPopulation = None
        self.hsPopulation = None
        self.destinations = list()

        if blocksCol is None:
            blocksCol = 'blockid10'

        self.destination_blocks = set(self._retrieve(conn,table,idCol,nameCol,blocksCol))


    def __unicode__(self):
        n = len(self.destinations)
        return u'%s: %i destinations' % (self.category, n)


    def __repr__(self):
        n = len(self.destinations)
        return r'%s: %i destinations' % (self.category, n)


    def _retrieve(self,conn,table,idCol,nameCol,blocksCol):
        """Retrieve destinations from the database and store them in
        this class' list of destinations

        return: list of all census block ids that contain a destination in this category
        """
        if self.verbose:
            print('Getting destinations for %s' % table)
        cur = conn.cursor()

        cur.execute(
            sql.SQL('select {}, {}::text, {} from {};')
                .format(
                    sql.Identifier(idCol),
                    sql.Identifier(nameCol),
                    sql.Identifier(blocksCol),
                    sql.Identifier(table)
                )
                .as_string(cur)
        )

        if self.verbose:
            print(cur.query)

        allBlocks = set()

        for row in cur:
            if type(row[2]) is list:
                blocks = set(row[2])
            else:
                blocks = set([row[2]])
            self.destinations.append({
                'id': row[0],
                'name': row[1],
                'blocks': blocks
            })

            allBlocks.update(blocks)

        return allBlocks
