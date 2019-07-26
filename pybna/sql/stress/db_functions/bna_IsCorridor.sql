CREATE OR REPLACE FUNCTION public.bna_IsCorridor (
    geom1_ geometry,
    geom2_ geometry,
    angle_ FLOAT,       -- given in degrees
    buffer_dist_ FLOAT DEFAULT 1.0
)
RETURNS BOOLEAN IMMUTABLE AS $func$

DECLARE
    geom1 geometry;
    geom2 geometry;
    int_geom geometry;

BEGIN
    IF  ST_GeometryType(geom1_) != 'ST_LineString'
        OR ST_GeometryType(geom2_) != 'ST_LineString' THEN
            RAISE EXCEPTION 'Road geometry must be of type Linestring';
    END IF;

    IF  NOT ST_DWithin(geom1_,geom2_,buffer_dist_) THEN
        RETURN FALSE;
    END IF;

    int_geom := ST_ClosestPoint(geom1_,geom2_);

    geom1 := ST_Intersection(
        geom1_,
        ST_Buffer(int_geom,buffer_dist_)
    );

    geom2 := ST_Intersection(
        geom2_,
        ST_Buffer(int_geom,buffer_dist_)
    );

    RETURN  degrees(
                acos(
                    abs(
                        cos(
                            ST_Azimuth(
                                ST_StartPoint(geom1),
                                ST_EndPoint(geom1)
                            ) -
                            ST_Azimuth(
                                ST_StartPoint(geom2),
                                ST_EndPoint(geom2)
                            )
                        )
                    )
                )
            ) <= angle_;

END $func$ LANGUAGE plpgsql;
