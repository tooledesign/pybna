----------------------------------------
-- Returns block IDs and a list of road
-- IDs associated with each block
----------------------------------------
SELECT  blocks.{block_id} AS blockid,
        array_agg(nodes.{node_id}) AS nodes
FROM    {blocks_schema}.{blocks} blocks,
        {roads_schema}.{roads} roads,
        {roads_schema}.{nodes} nodes
WHERE   ST_DWithin(blocks.{block_geom},roads.{road_geom},{distance})
AND     roads.{road_id} = nodes.{node_id}
AND     (
            ST_Contains(ST_Buffer(blocks.{block_geom},{distance}),roads.{road_geom})
        OR  ST_Length(
                ST_Intersection(ST_Buffer(blocks.{block_geom},{distance}),roads.{road_geom})
            ) > {min_length}
        )
GROUP BY blockid;
