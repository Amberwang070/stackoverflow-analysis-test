-- staging posts data
-- using partition and incremental for handling large size data loading and updating
{{
    config(
        materialized='incremental',
        unique_key='post_id',
        partition_by=['created_at_month'],
        post_hook=["CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_created_at ON {{ this }} (created_at)",
                  "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_post_type ON {{ this }} (post_type_id)",
                  "ANALYZE {{ this }}"]
    )
}}

WITH source_data AS (
    SELECT 
        Id,
        OwnerUserId,
        PostTypeId,
        ViewCount,
        CreationDate,
        AcceptedAnswerId,
        Score
    FROM {{ source('stackoverflow', 'posts') }}
    WHERE Id IS NOT NULL
    -- For incremental loads, only process new records
    {% if is_incremental() %}
        AND CreationDate > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

validated_data AS (
    SELECT 
        CAST(Id AS BIGINT) as post_id,
        CAST(OwnerUserId AS BIGINT) as user_id,
        CAST(PostTypeId AS INT) as post_type_id,
        CAST(ViewCount AS INT) as view_count,
        CreationDate as created_at,
        DATE_TRUNC('month', CreationDate) as created_at_month,
        CAST(AcceptedAnswerId AS BIGINT) as accepted_answer_id,
        CAST(Score AS INT) as score
    FROM source_data
    WHERE Id > 0 
    AND PostTypeId IN (1, 2)  -- 1 for questions, 2 for answers
)

SELECT 
    post_id,
    user_id,
    post_type_id,
    view_count,
    created_at,
    created_at_month,
    accepted_answer_id,
    score
FROM validated_data