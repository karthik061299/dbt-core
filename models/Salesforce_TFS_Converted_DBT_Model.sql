{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 1 as id
)

SELECT *
FROM source_data
