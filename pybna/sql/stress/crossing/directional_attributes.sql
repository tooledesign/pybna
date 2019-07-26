-- two way
INSERT INTO pg_temp.tmp_alldirs
SELECT
    {id_column}::INTEGER AS id,
    COALESCE({lanes},{assumed_lanes})::INTEGER,
    COALESCE({speed},{assumed_speed})::INTEGER
FROM
    {in_schema}.{in_table}
WHERE
    {twoway}
;
-- one way
INSERT INTO pg_temp.tmp_alldirs
SELECT
    {id_column}::INTEGER,
    COALESCE({lanes},{assumed_lanes})::INTEGER,
    COALESCE({speed},{assumed_speed})::INTEGER
FROM
    {in_schema}.{in_table}
WHERE
    {oneway}
;
