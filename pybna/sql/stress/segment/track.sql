-----------------------------------------------
-- cycle tracks
-- (assumes LTS = 1)
-----------------------------------------------
INSERT INTO {out_schema}.{out_table} (
    {id_column},
    {geom},
    stress
)
SELECT
    {id_column},
    {in_table}.geom,
    1
FROM {in_schema}.{in_table}
WHERE {track}
;
