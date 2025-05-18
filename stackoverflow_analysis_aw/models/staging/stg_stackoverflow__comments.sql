-- staging comments data
{{ 
    config(
        materialized='incremental',
        unique_key='comment_id',
        partition_by=['created_at_month'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_created_at ON {{ this }} (created_at)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_post_id ON {{ this }} (post_id)",
            "ANALYZE {{ this }}"
        ]
    )
}}

WITH source_data AS (
    SELECT 
        Id as comment_id,
        PostId as post_id,
        UserId as user_id,
        Text as comment_text,
        CreationDate as created_at,
        DATE_TRUNC('month', CreationDate) as created_at_month
    FROM {{ source('stackoverflow', 'comments') }}
    WHERE Id IS NOT NULL
    -- For incremental loads, only process new records
    {% if is_incremental() %}
        AND CreationDate > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

validated_data AS (
    SELECT 
        CAST(comment_id AS BIGINT) as comment_id,
        CAST(post_id AS BIGINT) as post_id,
        CAST(user_id AS BIGINT) as user_id,
        comment_text,
        created_at,
        created_at_month
    FROM source_data
    WHERE comment_id > 0
    AND post_id > 0
)

SELECT 
    comment_id,
    post_id,
    user_id,
    comment_text,
    created_at,
    created_at_month
FROM validated_data