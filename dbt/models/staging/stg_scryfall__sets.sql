SELECT
    json ->> 'id'                              AS set_id,
    json                                       AS payload,
    CAST('{{ var("snapshot_date") }}' AS DATE) AS snapshot_date
FROM read_ndjson_objects({{ bronze('sets') }})
