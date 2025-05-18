-- intermediate for getting the data in the require format
{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

WITH answer_timing AS (
    SELECT 
        q.post_id as question_id,
        q.created_at as question_date, -- When question was asked
        a.created_at as answer_date,  -- When answer was posted
        -- Join questions with their accepted answers to calculate response time
        DATEDIFF('minute', q.created_at, a.created_at) as minutes_to_answer
    FROM {{ ref('stg_stackoverflow__posts') }} q
    LEFT JOIN {{ ref('stg_stackoverflow__posts') }} a
        ON q.accepted_answer_id = a.post_id
    WHERE q.post_type_id = 1  -- Questions only
        AND a.created_at IS NOT NULL
        AND DATEDIFF('minute', q.created_at, a.created_at) >= 0  -- Filter out negative times
)

SELECT 
    at.question_id,          
    at.question_date,        
    at.answer_date,          
    at.minutes_to_answer,    
    -- add time band based on the time_band_stats 
    CASE 
        WHEN at.minutes_to_answer < 15 THEN 'Very Fast (less than 15 mins)'
        WHEN at.minutes_to_answer < 60 THEN 'Fast (15 to 60 mins)'
        WHEN at.minutes_to_answer < 480 THEN 'Same Day (1 to 8 hours)'
        WHEN at.minutes_to_answer < 1440 THEN 'Next Day (less than 24 hours)'
        WHEN at.minutes_to_answer < 7200 THEN 'Within 5 Days'
        ELSE 'More than 5 Days'
    END as response_time_band
FROM answer_timing at