-- Q1: top 10 users with their metrics
{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH ranked_users AS (
    -- Calculate user rankings based on total views
    SELECT 
        user_id,
        display_name as user_name,
        total_questions,
        total_views,
        avg_views_per_question,
        -- Generate rank based on total views in descending order
        ROW_NUMBER() OVER (ORDER BY total_views DESC) as rank
    FROM {{ ref('int_stackoverflow__question_views') }}
)

SELECT 
    rank,
    user_id,
    user_name,
    total_questions,
    total_views,
    avg_views_per_question
FROM ranked_users
WHERE rank <= 10
ORDER BY total_views DESC