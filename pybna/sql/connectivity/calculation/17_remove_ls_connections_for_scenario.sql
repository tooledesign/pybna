--
-- removes any blocks that already have a low stress connection
-- under existing conditions since these can't be positively affected
-- by a project
--

-- need to test which of these is faster

-- DROP TABLE IF EXISTS tmp_remove;
-- CREATE TEMP TABLE tmp_remove AS (
--     SELECT tmp_blocks.id
--     FROM tmp_blocks, tmp_this_block, {connectivity_schema}.{connectivity_table} c
--     WHERE
--         c.scenario IS NULL
--         AND tmp_this_block.id = c.{connectivity_source_col}
--         AND tmp_blocks.id = c.{connectivity_target_col}
--         AND low_stress
-- );
--
-- DELETE FROM tmp_blocks
-- WHERE id IN (SELECT id FROM tmp_remove)
-- ;
--
-- DROP TABLE tmp_remove;

DELETE FROM tmp_blocks
WHERE EXISTS (
    SELECT 1
    FROM {connectivity_schema}.{connectivity_table} c, tmp_this_block
    WHERE
        c.scenario IS NULL
        AND tmp_this_block.id != tmp_blocks.id
        AND tmp_this_block.id = c.{connectivity_source_col}
        AND tmp_blocks.id = c.{connectivity_target_col}
        AND low_stress
);
