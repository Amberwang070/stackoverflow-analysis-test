-- intermediate for getting the data in the require format
{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

SELECT
    p.user_id,
    u.display_name,  -- Added for readability
    COUNT(*) as total_questions,
    SUM(p.view_count) as total_views,
    ROUND(AVG(p.view_count), 2) as avg_views_per_question
FROM {{ ref('stg_stackoverflow__posts') }} p
LEFT JOIN {{ ref('stg_stackoverflow__users') }} u
    ON p.user_id = u.user_id
WHERE p.post_type_id = 1  -- Questions only
  AND p.user_id IS NOT NULL
GROUP BY 
    p.user_id,
    u.display_name