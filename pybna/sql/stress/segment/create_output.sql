CREATE TABLE {out_schema}.{out_table} AS (
    SELECT
        {id_column},
        {geom},
        NULL::INTEGER AS lanes,
        NULL::BOOLEAN AS marked_centerline,
        NULL::INTEGER AS speed,
        NULL::INTEGER AS effective_aadt,
        NULL::BOOLEAN AS parking,
        NULL::BOOLEAN AS low_parking,
        NULL::INTEGER AS parking_width,
        NULL::INTEGER AS bike_lane_width,
        NULL::INTEGER AS stress
    FROM {in_schema}.{in_table}
    WHERE FALSE
);
