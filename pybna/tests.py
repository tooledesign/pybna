#
# Methods for testing various parts of the pyBNA library
#
import os, random, string
import tempfile
import yaml
from psycopg2 import sql
from .stress import Stress
from .dbutils import DBUtils
import pandas as pd

def test_segment_stress(out_file=None,config=None,host=None,db_name=None,user=None,
                        password=None,shared_lookup=None,bike_lane_lookup=None,
                        km=False):
    """
    Uses the segments spreadsheet, lookup tables, and pyBNA logic to test
    various roadway characteristics for the LTS score. Saves results as a
    spreadsheet.

    Parameters
    ----------
    out_file : str, optional
        path to save an .xlsx output of the results
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
    km : boolean, optional
        run the tests in metric units

    Returns
    -------
    pandas DataFrame unless Excel output is indicated
    """
    if config is None:
        module_dir = os.path.dirname(os.path.abspath(__file__))
        config = os.path.join(module_dir,"config.yaml")
    conf = yaml.safe_load(open(config))

    if host is None:
        host = conf["db"]["host"]
    if db_name is None:
        db_name = conf["db"]["dbname"]
    if user is None:
        user = conf["db"]["user"]
    if password is None:
        password = conf["db"]["password"]

    # connect to db to get pertinent info
    db_connection_string = " ".join([
        "dbname=" + db_name,
        "user=" + user,
        "host=" + host,
        "password=" + password
    ])
    db = DBUtils(db_connection_string)
    schema = db.get_default_schema()
    table = "".join(random.choice(string.ascii_lowercase) for i in range(7))

    # make a temporary config file with some strategic changes
    conf["bna"]["network"]["roads"]["table"] = schema + "." + table
    conf["bna"]["network"]["roads"]["uid"] = "id"
    if km:
        conf["units"] = "km"
    conf["stress"]["crossing"]["control"]["table"] = "xxxx.xxxx"
    conf["stress"]["crossing"]["island"]["table"] = "xxxx.xxxx"
    conf["stress"]["segment"]["forward"]["lanes"] = "lanes"
    conf["stress"]["segment"]["forward"]["aadt"] = "aadt"
    conf["stress"]["segment"]["forward"]["centerline"] = {"name":"centerline","val":True}
    conf["stress"]["segment"]["forward"]["speed"] = "speed"
    conf["stress"]["segment"]["forward"]["width"] = "width"
    conf["stress"]["segment"]["forward"]["parking"] = {"name":"parking","val":True}
    conf["stress"]["segment"]["forward"]["low_parking"] = {"name":"low_parking","val":True}
    conf["stress"]["segment"]["forward"]["park_width"] = "park_width"
    conf["stress"]["segment"]["forward"]["bike_infra"] = {"name":"bike","lane":"lane","buffered_lane":"buffered_lane","track":"track","path":"path"}
    conf["stress"]["segment"]["forward"]["bike_lane_width"] = "bike_width"
    conf["stress"]["segment"]["backward"] = dict(conf["stress"]["segment"]["forward"])
    conf["bna"]["network"]["roads"]["stress"]["segment"]["forward"] = "calculated_stress"
    if shared_lookup:
        conf["stress"]["lookup_tables"]["shared"] = shared_lookup
    if bike_lane_lookup:
        conf["stress"]["lookup_tables"]["bike_lane"] = bike_lane_lookup

    tempf, temppath = tempfile.mkstemp()
    with open(tempf,'w') as f:
        yaml.dump(conf,f)

    # create the stress object
    s = Stress(config=temppath,host=host,db_name=db_name,user=user,password=password)

    # read the spreadsheet and convert data types in db
    if km:
        segments = pd.read_excel(os.path.join(module_dir,"tests","segments_km.xlsx"))
    else:
        segments = pd.read_excel(os.path.join(module_dir,"tests","segments.xlsx"))
    conn = s.get_db_connection()
    s.gdf_to_postgis(segments,table,schema=schema,no_geom=True,conn=conn)
    subs = {"schema": sql.Identifier(schema), "table": sql.Identifier(table)}
    s._run_sql(
        """
            update {schema}.{table} set bike=null where bike='NaN';
            update {schema}.{table} set centerline=null where centerline='NaN';
            update {schema}.{table} set aadt=null where aadt='NaN';
            update {schema}.{table} set speed=null where speed='NaN';
            update {schema}.{table} set width=null where width='NaN';
            update {schema}.{table} set bike_width=null where bike_width='NaN';
            update {schema}.{table} set lanes=null where lanes='NaN';
            update {schema}.{table} set parking=null where parking='NaN';
            update {schema}.{table} set park_width=null where park_width='NaN';
            update {schema}.{table} set low_parking=null where low_parking='NaN';
            alter table {schema}.{table} alter column centerline type boolean using centerline::boolean;
            alter table {schema}.{table} alter column aadt type integer using aadt::integer;
            alter table {schema}.{table} alter column parking type boolean using parking::boolean;
            alter table {schema}.{table} alter column speed type integer using speed::integer;
            alter table {schema}.{table} alter column lanes type integer using lanes::integer;
            alter table {schema}.{table} alter column low_parking type boolean using low_parking::boolean;
            alter table {schema}.{table} add column one_way text;
            alter table {schema}.{table} add column functional_class text;
            alter table {schema}.{table} add column calculated_stress integer;
            alter table {schema}.{table} add column tf_seg_stress integer;
            alter table {schema}.{table} add column geom geometry(linestring,4326);
        """,
        subs,
        conn=conn
    )
    if km:
        s._run_sql(
            """
                alter table {schema}.{table} alter column width type float using width::float;
                alter table {schema}.{table} alter column bike_width type float using bike_width::float;
                alter table {schema}.{table} alter column park_width type float using park_width::float;
            """,
            subs,
            conn=conn
        )
    else:
        s._run_sql(
            """
                alter table {schema}.{table} alter column width type integer using width::integer;
                alter table {schema}.{table} alter column bike_width type integer using bike_width::integer;
                alter table {schema}.{table} alter column park_width type integer using park_width::integer;
            """,
            subs,
            conn=conn
        )

    conn.commit()

    # run stress calc and save back to pandas
    s.segment_stress(table=table)
    q = sql.SQL(
        """
            select
                bike, speed, centerline, aadt, bike_width, lanes, width,
                parking, park_width, low_parking, calculated_stress
            from {schema}.{table}
            order by id
        """
    ).format(**subs)
    result = pd.read_sql_query(
        q.as_string(conn),
        con=conn
    )

    # clean up
    subs["table_fw"] = sql.Identifier(table+"_forward")
    subs["table_bw"] = sql.Identifier(table+"_backward")
    s._run_sql(
        """
            drop table if exists {schema}.{table};
            drop table if exists {schema}.{table_fw};
            drop table if exists {schema}.{table_bw};
        """,
        subs,
        conn=conn
    )
    conn.commit()
    conn.close()

    if out_file:
        result.to_excel(out_file)
    else:
        return result


def test_crossing_stress(out_file=None,config=None,host=None,db_name=None,user=None,
                         password=None,crossing_lookup=None,km=False):
    """
    Uses the crossing spreadsheet, lookup tables, and pyBNA logic to test
    various roadway characteristics for the crossing LTS score. Saves results as
    a spreadsheet.

    Parameters
    ----------
    out_file : str, optional
        path to save an .xlsx output of the results
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
    crossing_lookup : str, optional
        stress lookup table for crossing
    km : boolean, optional
        run the tests in metric units

    Returns
    -------
    pandas DataFrame unless Excel output is indicated
    """
    if config is None:
        module_dir = os.path.dirname(os.path.abspath(__file__))
        config = os.path.join(module_dir,"config.yaml")
    conf = yaml.safe_load(open(config))

    if host is None:
        host = conf["db"]["host"]
    if db_name is None:
        db_name = conf["db"]["dbname"]
    if user is None:
        user = conf["db"]["user"]
    if password is None:
        password = conf["db"]["password"]

    # connect to db to get pertinent info
    db_connection_string = " ".join([
        "dbname=" + db_name,
        "user=" + user,
        "host=" + host,
        "password=" + password
    ])
    db = DBUtils(db_connection_string)
    schema = db.get_default_schema()
    in_table = "in" + "".join(random.choice(string.ascii_lowercase) for i in range(7))
    out_table = "out" + "".join(random.choice(string.ascii_lowercase) for i in range(7))

    # make a temporary config file with some strategic changes
    if crossing_lookup:
        conf["stress"]["lookup_tables"]["crossing"] = crossing_lookup

    cross_lts_schema, cross_lts_table = db.parse_table_name(conf["stress"]["lookup_tables"]["crossing"])
    if cross_lts_schema is None:
        cross_lts_schema = db.get_schema(cross_lts_table)

    subs = {
        "cross_lts_schema": sql.Identifier(cross_lts_schema),
        "cross_lts_table": sql.Identifier(cross_lts_table),
        "in_schema": sql.Identifier(schema),
        "in_table": sql.Identifier(in_table),
        "out_schema": sql.Identifier(schema),
        "out_table": sql.Identifier(out_table),
        "id_column": sql.Identifier("id"),
        "geom": sql.Identifier("geom")
    }

    # read the spreadsheet and convert data types in db
    if km:
        crossings = pd.read_excel(os.path.join(module_dir,"tests","crossings_km.xlsx"))
    else:
        crossings = pd.read_excel(os.path.join(module_dir,"tests","crossings.xlsx"))
    conn = db.get_db_connection()
    db.gdf_to_postgis(crossings,in_table,schema=schema,no_geom=True,conn=conn)
    db._run_sql(
        """
            update {in_schema}.{in_table} set control=null where control='NaN';
            --update {in_schema}.{in_table} set lanes=null where lanes='NaN';
            --update {in_schema}.{in_table} set speed=null where speed='NaN';
            update {in_schema}.{in_table} set island=null where island='NaN';
            alter table {in_schema}.{in_table} alter column lanes type integer using lanes::integer;
            alter table {in_schema}.{in_table} alter column speed type integer using speed::integer;
            alter table {in_schema}.{in_table} alter column island type boolean using island::boolean;

            --alter table {in_schema}.{in_table} add column one_way text;
            --alter table {in_schema}.{in_table} add column functional_class text;
            alter table {in_schema}.{in_table} add column calculated_stress integer;
            --alter table {in_schema}.{in_table} add column tf_int_stress integer;
            alter table {in_schema}.{in_table} add column geom geometry(linestring,4326);
            select * into pg_temp.tmp_cross_streets from {in_schema}.{in_table};
            create table pg_temp.tmp_intcount (id integer, legs integer);
            create table {out_schema}.{out_table} (
                id integer,
                geom geometry(linestring,4326),
                legs integer,
                control text,
                lanes integer,
                speed integer,
                island boolean,
                stress integer
            );
        """,
        subs,
        conn=conn
    )
    # conn.commit()

    # run stress calc and save back to pandas
    db._run_sql_script("crossing.sql",subs,dirs=["sql","stress","crossing"],conn=conn)
    db._run_sql(
        """
            update {in_schema}.{in_table} i
            set calculated_stress = o.stress
            from {out_schema}.{out_table} o
            where i.id = o.id
        """,
        subs,
        conn=conn
    )
    q = sql.SQL(
        """
            select control, lanes, speed, island, calculated_stress
            from {in_schema}.{in_table}
            order by id
        """
    ).format(**subs)
    result = pd.read_sql_query(
        q.as_string(conn),
        con=conn
    )

    # clean up
    db._run_sql(
        """
            drop table if exists {in_schema}.{in_table};
            drop table if exists {out_schema}.{out_table};
        """,
        subs,
        conn=conn
    )
    conn.commit()
    conn.close()

    if out_file:
        result.to_excel(out_file)
    else:
        return result
