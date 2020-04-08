psql -h 192.168.60.220 -U gis -d bna -f ./08_build_intersections.sql
psql -h 192.168.60.220 -U gis -d bna -f ./09_build_review_table.sql
python 10_pybna_LTS_on_website_ways.py
