--
-- Finds the "start" point of a multilinestring by identifying the point
-- furthest from the centroid
--
CREATE OR REPLACE FUNCTION public.bna_MultiEndPoint (
    _geom geometry
)
RETURNS geometry IMMUTABLE AS $func$

DECLARE
    _centroid geometry;
    _start geometry;
    _result geometry;

BEGIN
    _centroid := ST_Centroid(_geom);
    _start := bna_MultiStartPoint(_geom);

    IF geometrytype(_geom) = 'LINESTRING' THEN
        _result := ST_EndPoint(_geom);
    ELSIF geometrytype(_geom) IN ('MULTILINESTRING','GEOMETRYCOLLECTION') THEN
        _result := dump.geom
            FROM ST_DumpPoints(_geom) dump
            ORDER BY
                ST_Distance(dump.geom,_start)
                    + ST_Distance(dump.geom,_centroid) DESC
            LIMIT 1
        ;
    ELSE
        RETURN NULL;
    END IF;

    RETURN _result;

END $func$ LANGUAGE plpgsql;
