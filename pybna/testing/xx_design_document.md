
## PYBNA Comparison (old vs. new)

We need to compare pybna results against a comparable BNA run using the old BNA method. I've downloaded results from the website for Madison, WI. The following tables are in the received schema of the bna DB on the Portland server:

1. website_neighborhood_census_blocks
2. website_neighborhood_ways
3. website_connected_census_blocks

Our process should proceed as follows:

- [x]  Import destinations from OSM using pybna
- [x]  Manually calculate hs/ls destination counts for each block using website_connected_census_blocks (this represents the website condition, but using updated destination data since we don't have the destination sets used in the website BNA run)
- [x]  Reconstruct the intersection layer using the intersection_from/intersection_to columns in website_neighborhood_ways.
- [x]  Run pybna on website_neighborhood_ways to create a connected blocks table for pybna.
- [ ]  Calculate hs/ls destination counts for each block and compare with counts generated in Step 2.
- [x]  Import new OSM network using pybna and run LTS
- [ ]  Run pybna LTS off website_neighborhood_ways
- [ ]  Compare LTS in Step 6 with LTS from website in website_neighborhood_ways, as well as the LTS values generated in Step 7


We can document any issues or discrepancies we come across here. Ultimately, we need to be able to catalog any differences as either a regression (needs fixing), a neutral change (difference that results in no substantial effect), or an enhancement (fixes a problem with the original)
