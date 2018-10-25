SELECT
    array_agg({blocks_id_col}) AS block_ids,
    ST_Union({blocks_geom_col})::geometry(multipolygon,{srid}) AS {zones_geom_col}
INTO {zones_schema}.{zones_table}
FROM {blocks_schema}.{blocks_table}
WHERE FALSE
;

DELETE FROM {zones_schema}.{zones_table};
