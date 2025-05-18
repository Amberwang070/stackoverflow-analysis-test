-- there are extra metrics in this result, could be useful
{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT 
    STRFTIME(DATE_TRUNC('month', question_date), '%Y-%m') as month,  -- Changed to STRFTIME
    response_time_band as time_band,
    COUNT(*) as question_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE_TRUNC('month', question_date)), 2) as percentage_in_month
FROM {{ ref('int_stackoverflow__answer_timing') }}
GROUP BY 
    DATE_TRUNC('month', question_date),
    response_time_band
ORDER BY 
    DATE_TRUNC('month', question_date) DESC,
    CASE response_time_band
        WHEN 'Very Fast (less than 15 mins)' THEN 1
        WHEN 'Fast (15 to 60 mins)' THEN 2
        WHEN 'Same Day (1 to 8 hours)' THEN 3
        WHEN 'Next Day (less than 24 hours)' THEN 4
        WHEN 'Within 5 Days' THEN 5
        ELSE 6
    END