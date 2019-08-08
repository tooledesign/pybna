SELECT array_agg(tmp_blocks_nodes.node_id) AS node_ids
FROM tmp_blocks_nodes
WHERE tmp_blocks_nodes.id = {block_id}::{blocks_id_type}
