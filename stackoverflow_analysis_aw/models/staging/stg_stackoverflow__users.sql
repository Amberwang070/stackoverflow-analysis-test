-- staging user data
{{
    config(
        materialized='incremental',
        unique_key='user_id',
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_created_at ON {{ this }} (created_at)",
            "ANALYZE {{ this }}"
        ]
    )
}}

WITH source_data AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY CreationDate) as batch_id
    FROM {{ source('stackoverflow', 'users') }}
    -- For incremental loads, only process new records
    {% if is_incremental() %}
    WHERE CreationDate > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)
SELECT
    CAST(Id AS BIGINT) as user_id,
    DisplayName as display_name,
    CreationDate as created_at
FROM source_data