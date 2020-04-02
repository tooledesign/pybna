from pybna import Importer
i = Importer(host="192.168.60.220",db_name="bna")
i.import_boundary('/home/spencer/gis/bna/City_Limit.shp',srid=26916,overwrite=True)
i.import_osm_destinations(osm_file='/home/spencer/gis/bna/madison.osm',srid=26916)
