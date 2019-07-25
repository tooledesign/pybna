CREATE OR REPLACE FUNCTION public.bna_CompareAzimuths(
    _base_geom geometry,
    _comp_geom geometry,
    _line_start FLOAT DEFAULT 0.33,
    _line_end FLOAT DEFAULT 0.67
)
RETURNS FLOAT IMMUTABLE AS $func$

DECLARE
    _angle_diff FLOAT;

BEGIN
    _angle_diff = degrees(
        acos(
            abs(
                cos(
                    ST_Azimuth(
                        ST_LineInterpolatePoint(
                            _comp_geom,
                            _line_start
                        ),
                        ST_LineInterpolatePoint(
                            _comp_geom,
                            _line_end
                        )
                    ) -
                    ST_Azimuth(
                        ST_LineInterpolatePoint(
                            _base_geom,
                            _line_start
                        ),
                        ST_LineInterpolatePoint(
                            _base_geom,
                            _line_end
                        )
                    )
                )
            )
        )
    );

    RETURN _angle_diff;

END $func$ LANGUAGE plpgsql;
