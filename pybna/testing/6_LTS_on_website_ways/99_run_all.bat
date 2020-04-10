
psql -h 192.168.60.220 -U gis -d bna -c "DROP TABLE IF EXISTS automated.website_ways_through_pybna_for_lts_segment_backward"
psql -h 192.168.60.220 -U gis -d bna -c "DROP TABLE IF EXISTS automated.website_ways_through_pybna_for_lts_segment_forward"
psql -h 192.168.60.220 -U gis -d bna -c "DROP TABLE IF EXISTS automated.website_ways_through_pybna_for_lts_segment_break_backward"
psql -h 192.168.60.220 -U gis -d bna -c "DROP TABLE IF EXISTS automated.website_ways_through_pybna_for_lts_segment_break_forward"
psql -h 192.168.60.220 -U gis -d bna -f ./08_build_intersections.sql
psql -h 192.168.60.220 -U gis -d bna -f ./09_build_review_table.sql
python 10_pybna_LTS_on_website_ways.py
psql -h 192.168.60.220 -U gis -d bna -f ./11_build_break_table.sql
python 12_broken_lts.py
