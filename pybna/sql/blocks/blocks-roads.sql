drop table if exists tmp_blocks;
select gid, st_buffer(geom,5) as geom
into temp table tmp_blocks
from neighborhood_census_blocks;

create index tsidx_b on tmp_blocks using gist (geom);
analyze tmp_blocks;

drop table if exists pg_temp.a;
select
    blocks.gid,
    blocks.geom as bgeom,
    roads.road_id,
    roads.geom as rgeom
into temp table a
from
    tmp_blocks blocks,
    neighborhood_ways roads
where
    st_intersects(blocks.geom,roads.geom)
;

drop table if exists pg_temp.b;
select gid, road_id
into temp table b
from a
where
            ST_Contains(bgeom,rgeom)
        OR  ST_Length(
                ST_Intersection(bgeom,rgeom)
            ) > 35
       
