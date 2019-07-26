DROP TABLE IF EXISTS pg_temp.tmp_alldirs;
CREATE TEMP TABLE pg_temp.tmp_alldirs (
    id INTEGER,
    lanes INTEGER,
    speed INTEGER
);

{data_insert}

DROP TABLE IF EXISTS pg_temp.tmp_combineddirs;
SELECT DISTINCT ON (tmp_alldirs.id)
    tmp_alldirs.id,
    o.{geom} AS geom,
    bna_MultiEndPoint(o.{geom}) AS forward_pt,
    ST_MakeLine(ST_PointN(o.{geom},-2),ST_EndPoint(o.{geom})) AS forward_ln,
    bna_MultiStartPoint(o.{geom}) AS backward_pt,
    ST_MakeLine(ST_PointN(o.{geom},2),ST_StartPoint(o.{geom})) AS backward_ln,
    SUM(tmp_alldirs.lanes) AS lanes,
    MAX(tmp_alldirs.speed) AS speed
INTO TEMP TABLE tmp_combineddirs
FROM
    pg_temp.tmp_alldirs,
    {in_schema}.{in_table} o
WHERE
    tmp_alldirs.id = o.{id_column}
GROUP BY
    tmp_alldirs.id,
    o.{geom}
ORDER BY
    tmp_alldirs.id
;

CREATE INDEX tidx_tmp_combineddirs_id ON pg_temp.tmp_alldirs (id);
CREATE INDEX sidx_tmp_combineddirs ON pg_temp.tmp_combineddirs USING GIST (geom);
CREATE INDEX sidx_tmp_combineddirs_fwd ON pg_temp.tmp_combineddirs USING GIST (forward_pt);
CREATE INDEX sidx_tmp_combineddirs_bwd ON pg_temp.tmp_combineddirs USING GIST (backward_pt);
ANALYZE pg_temp.tmp_combineddirs;
