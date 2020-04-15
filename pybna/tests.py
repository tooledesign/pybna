#
# Methods for testing various parts of the pyBNA library
#
import os, random, string
from .stress import Stress
import pandas as pd

def test_segment_stress(config=None,host=None,db_name=None,user=None,
                        password=None,shared_lookup=None,bike_lane_lookup=None):
    """
    Uses the segments spreadsheet, lookup tables, and pyBNA logic to test
    various roadway characteristics for the LTS score. Saves results as a
    spreadsheet.

    Parameters
    ----------
    config : str, optional
        path to the config file, if not given use the default config.yaml
    host : str, optional
        host to connect to
    db_name : str, optional
        database name
    user : str, optional
        database user
    password : str, optional
        database password
    shared_lookup : str, optional
        stress lookup table for shared roadways
    bike_lane_lookup : str, optional
        stress lookup table for bike lanes

    Returns
    -------
    pandas DataFrame
    """
    module_dir = os.path.dirname(os.path.abspath(__file__))
    segments = pd.read_excel(os.path.join(module_dir,"tests","segments.xlsx"))
    # need to read config file in and tweak it before sending over to stress
    s = Stress(config=config,host=host,db_name=db_name,user=user,password=password)

    table = "".join(random.choice(string.ascii_lowercase) for i in range(7))
    conn = s.get_db_connection()
    s.gdf_to_postgis(segments,table,no_geom=True,conn=conn)

    #s.segment_stress()
    #save back to pandas object
    #close connection
