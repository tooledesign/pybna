select
    ' ' as priority,
    max(case {priority_a} end) as priority_a,
    ' ' as priority_cross,
    {control} as control,
    ' ' as control_a,
    -- first_value({control_a})
    --     over (order by array_position(
    --         array['signal','hawk','four way stop','rrfb','none'],
    --         {control_a}
    --     )) as control_a,
    ' ' as control_cross,
    {lanes} as lanes,
    ' ' as lanes_a,
    max({lanes_cross}) as lanes_cross,
    {speed} as speed,
    ' ' as speed_a,
    max({speed_cross}) as speed_cross
from {in_table} this_way, {seg} crossing_way
where this_way.{id_column} = {fid}
and this_way.{way_int} IN (crossing_way.{net_source}, crossing_way.{net_target})
and not bna_iscorridor(this_way.{geom},crossing_way.{geom},{angle})
group by priority,priority_cross,control,control_cross,lanes,lanes_a,speed,speed_a;
