SELECT {roads_id_col}
FROM {roads_schema}.{roads_table}
WHERE
    {roads_scenario_col} IS NOT NULL
    AND {roads_scenario_col} != {scenario_id}
