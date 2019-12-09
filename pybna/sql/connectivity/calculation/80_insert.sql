INSERT INTO {connectivity_schema}.{connectivity_table}
SELECT * FROM tmp_connectivity
;

DROP TABLE tmp_connectivity;
