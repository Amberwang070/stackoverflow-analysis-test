-- Q3: mart_questions_with_author_replies.sql
{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH total_questions AS (
    SELECT COUNT(*) as total_count
    FROM {{ ref('stg_stackoverflow__posts') }}
    WHERE post_type_id = 1
),

reply_metrics AS (
    SELECT 
        COUNT(DISTINCT post_id) as questions_with_replies,
        COUNT(*) as total_replies
    FROM {{ ref('int_stackoverflow__at_replies') }}
)

SELECT 
    questions_with_replies,
    total_replies,
    ROUND(questions_with_replies * 100.0 / total_count, 2) as percentage_questions_with_replies  -- this is not required from the test, adding for providing additional info
FROM reply_metrics
CROSS JOIN total_questions